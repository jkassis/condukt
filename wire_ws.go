package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

// WSWire manages WebSocket connections for sending and receiving messages.
type WSWire struct {
	mu          sync.Mutex
	connections map[string]*websocket.Conn // Channel -> WebSocket connection
	upgrader    websocket.Upgrader
	recvCh      map[string]chan Msg // Channel -> Message queue
}

// WSWireMake initializes a WebSocketSender.
func WSWireMake() *WSWire {
	return &WSWire{
		connections: make(map[string]*websocket.Conn),
		recvCh:      make(map[string]chan Msg),
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true }, // Allow all origins
		},
	}
}

// SendMessage sends a message via WebSocket.
func (s *WSWire) SendMessage(msg Msg) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	conn, exists := s.connections[msg.Strand]
	if !exists {
		logger.Warn("No WebSocket connection for channel", zap.String("channel", msg.Strand))
		return errors.New("no active WebSocket connection for channel")
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
		logger.Error("Failed to send WebSocket message", zap.Error(err))
		return err
	}

	messagesSent.WithLabelValues(msg.Strand).Inc()
	logger.Info("Message sent via WebSocket",
		zap.String("channel", msg.Strand),
		zap.String("payload", msg.Payload),
	)
	return nil
}

// ReceiveMessage retrieves a message from the WebSocket receive queue.
func (s *WSWire) ReceiveMessage(channel string) (*Msg, error) {
	s.mu.Lock()
	ch, exists := s.recvCh[channel]
	s.mu.Unlock()

	if !exists {
		logger.Warn("No WebSocket receive channel available", zap.String("channel", channel))
		return nil, errors.New("no WebSocket receive channel available")
	}

	msg := <-ch
	messagesReceived.WithLabelValues(channel).Inc()

	logger.Info("Message received via WebSocket",
		zap.String("channel", msg.Strand),
		zap.String("payload", msg.Payload),
	)

	return &msg, nil
}

// HandleWebSocketConnection upgrades an HTTP connection to a WebSocket and handles message reception.
func (s *WSWire) HandleWebSocketConnection(w http.ResponseWriter, r *http.Request, channel string) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		logger.Error("WebSocket upgrade failed", zap.Error(err))
		return
	}

	s.mu.Lock()
	s.connections[channel] = conn
	if _, exists := s.recvCh[channel]; !exists {
		s.recvCh[channel] = make(chan Msg, 100) // Buffered channel for received messages
	}
	s.mu.Unlock()

	logger.Info("WebSocket connection established", zap.String("channel", channel))

	// Handle incoming messages
	go func() {
		defer conn.Close()
		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				logger.Warn("WebSocket read error", zap.Error(err))
				break
			}

			var msg Msg
			if err := json.Unmarshal(message, &msg); err != nil {
				logger.Warn("Failed to unmarshal WebSocket message", zap.Error(err))
				continue
			}

			s.mu.Lock()
			if ch, exists := s.recvCh[msg.Strand]; exists {
				ch <- msg
			}
			s.mu.Unlock()
		}

		// Remove the connection when closed
		s.mu.Lock()
		delete(s.connections, channel)
		delete(s.recvCh, channel)
		s.mu.Unlock()
	}()
}
