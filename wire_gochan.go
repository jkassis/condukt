package main

import (
	"errors"
	"sync"

	"go.uber.org/zap"
)

// GoChanWire is a transport that uses Go channels for messaging.
type GoChanWire struct {
	mu       sync.Mutex
	channels map[string]chan Msg
}

// GoChanWireMake initializes a new GoChanWire.
func GoChanWireMake() *GoChanWire {
	return &GoChanWire{
		channels: make(map[string]chan Msg),
	}
}

// SendMessage sends a message via a Go channel.
func (s *GoChanWire) SendMessage(msg Msg) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.channels[msg.Strand]; !exists {
		s.channels[msg.Strand] = make(chan Msg, 100000) // Buffered channel
	}

	select {
	case s.channels[msg.Strand] <- msg:
		messagesSent.WithLabelValues(msg.Strand).Inc()
		logger.Debug("Message sent via GoChanWire",
			zap.String("channel", msg.Strand),
			zap.String("payload", msg.Payload),
		)
		return nil
	default:
		logger.Warn("Channel buffer full", zap.String("channel", msg.Strand))
		return errors.New("channel buffer full")
	}
}

// ReceiveMessage retrieves the next message from a Go channel.
func (s *GoChanWire) ReceiveMessage(channel string) (*Msg, error) {
	s.mu.Lock()
	ch, exists := s.channels[channel]
	s.mu.Unlock()

	if !exists {
		logger.Warn("Channel does not exist", zap.String("channel", channel))
		return nil, errors.New("channel does not exist")
	}

	msg, ok := <-ch
	if !ok {
		logger.Warn("Channel closed", zap.String("channel", channel))
		return nil, errors.New("channel closed")
	}

	messagesReceived.WithLabelValues(channel).Inc()
	logger.Debug("Message received via GoChanWire",
		zap.String("channel", channel),
		zap.String("payload", msg.Payload),
	)

	return &msg, nil
}

// Reset clears all channels, simulating a failure.
func (s *GoChanWire) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Close all existing channels
	for strand, ch := range s.channels {
		close(ch)
		delete(s.channels, strand)
	}

	// Reinitialize channels map
	s.channels = make(map[string]chan Msg)

	logger.Debug("GoChanWire reset: all channels cleared")
}
