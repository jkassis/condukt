package main

import (
	"encoding/json"
	"errors"
	"net"

	"go.uber.org/zap"
)

// UDPWire handles UDP message transport (sending & receiving).
type UDPWire struct {
	conn *net.UDPConn
	addr *net.UDPAddr
}

// UDPWireMake initializes a new UDP connection.
func UDPWireMake(address string) (*UDPWire, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, err
	}
	return &UDPWire{conn: conn, addr: udpAddr}, nil
}

// SendMessage sends a message via UDP.
func (s *UDPWire) SendMessage(msg Msg) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	_, err = s.conn.WriteToUDP(data, s.addr)
	if err != nil {
		logger.Error("UDP send failed", zap.Error(err))
	}
	return err
}

// ReceiveMessage listens for incoming messages via UDP.
func (s *UDPWire) ReceiveMessage(channel string) (*Msg, error) {
	buffer := make([]byte, 4096)
	n, addr, err := s.conn.ReadFromUDP(buffer)
	if err != nil {
		logger.Warn("UDP receive error", zap.Error(err))
		return nil, err
	}

	var msg Msg
	if err := json.Unmarshal(buffer[:n], &msg); err != nil {
		logger.Warn("Failed to unmarshal UDP message", zap.Error(err))
		return nil, errors.New("invalid UDP message format")
	}

	logger.Info("Message received via UDP",
		zap.String("channel", msg.Channel),
		zap.String("payload", msg.Payload),
		zap.String("from", addr.String()),
	)

	return &msg, nil
}
