package main

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

// Conduktor manages sending and receiving messages through the appropriate store.
type Conduktor struct {
	mu       sync.Mutex
	wire     Wire
	volatile Store // Non-durable strands
	durable  Store // Durable strands
}

// ConduktorMake initializes a new Conduktor with separate volatile and durable stores.
func ConduktorMake(volatile Store, durable Store, wire Wire) *Conduktor {
	return &Conduktor{
		wire:     wire,
		volatile: volatile,
		durable:  durable,
	}
}

// StrandAdd registers a new strand and determines whether to store it in volatile or durable storage.
func (c *Conduktor) StrandAdd(strandID string, config StrandConf) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	store := c.selectStore(config.Durable)
	if err := store.CreateStrand(strandID, config); err != nil {
		logger.Error("Failed to create strand", zap.String("strand", strandID), zap.Error(err))
		return err
	}

	logger.Debug("Strand added",
		zap.String("strand", strandID),
		zap.Bool("durable", config.Durable),
	)
	return nil
}

// Send places a message in the appropriate store and sends it via the configured transport.
func (c *Conduktor) Send(strandID string, payload string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	store, err := c.getStore(strandID)
	if err != nil {
		return err
	}

	msg := Msg{
		ID:        fmt.Sprintf("%d", time.Now().UnixNano()),
		Strand:    strandID,
		Payload:   payload,
		Acked:     false,
		Timestamp: time.Now().Unix(),
	}

	// Always save the message, regardless of durability
	if err := store.Save(msg); err != nil {
		return err
	}

	// Send via transport
	if err := c.wire.SendMessage(msg); err != nil {
		logger.Error("Message send failed", zap.String("strand", strandID), zap.Error(err))
		return err
	}

	messagesSent.WithLabelValues(strandID).Inc()
	logger.Debug("Message sent", zap.String("strand", strandID), zap.String("payload", msg.Payload))
	return nil
}

// Receive retrieves the next message from the queue via transport.
func (c *Conduktor) Receive(strandID string) (*Msg, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Attempt to receive from the transport
	msg, err := c.wire.ReceiveMessage(strandID)
	if err != nil {
		logger.Warn("No messages available", zap.String("strand", strandID), zap.Error(err))
		return nil, err
	}

	messagesReceived.WithLabelValues(strandID).Inc()
	logger.Debug("Message received", zap.String("strand", strandID), zap.String("payload", msg.Payload))
	return msg, nil
}

// Acknowledge marks a message as processed and removes it from storage.
func (c *Conduktor) Acknowledge(strandID, msgID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	store, err := c.getStore(strandID)
	if err != nil {
		return err
	}

	if err := store.Acknowledge(strandID, msgID); err != nil {
		logger.Error("Acknowledgment failed", zap.String("strand", strandID), zap.String("msgID", msgID), zap.Error(err))
		return err
	}

	logger.Debug("Message acknowledged", zap.String("strand", strandID), zap.String("msgID", msgID))
	return nil
}

// StrandRemove deletes a strand and all of its messages.
func (c *Conduktor) StrandRemove(strandID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	store, err := c.getStore(strandID)
	if err != nil {
		return err
	}

	if err := store.DeleteStrand(strandID); err != nil {
		logger.Error("Failed to delete strand", zap.String("strand", strandID), zap.Error(err))
		return err
	}

	logger.Info("Strand deleted", zap.String("strand", strandID))
	return nil
}

// selectStore determines which store to use based on strand durability.
func (c *Conduktor) selectStore(durable bool) Store {
	if durable {
		return c.durable
	}
	return c.volatile
}

// getStore retrieves the store and configuration for a given strand.
func (c *Conduktor) getStore(strandID string) (Store, error) {
	// Check both stores for the strand configuration
	for _, store := range []Store{c.durable, c.volatile} {
		if store.HasStrand(strandID) {
			return store, nil
		}
	}

	logger.Warn("Strand not found", zap.String("strand", strandID))
	return nil, errors.New("strand not found")
}

// RecoverUnackedMessages iterates through unacknowledged messages and resends them.
func (c *Conduktor) RecoverUnackedMessages() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Retrieve an iterator for unacked messages
	iterator, err := c.durable.UnackedIterator()
	if err != nil {
		logger.Error("Failed to get UnackedIterator", zap.Error(err))
		return err
	}
	defer iterator.Close()

	logger.Info("Starting recovery of unacked messages")

	// Process messages one by one
	for {
		msg, hasNext := iterator.Next()
		if !hasNext {
			break
		}

		// Attempt to resend the message
		if err := c.wire.SendMessage(*msg); err != nil {
			logger.Error("Failed to resend unacked message",
				zap.String("msgID", msg.ID),
				zap.Error(err),
			)
			continue
		}

		logger.Info("Successfully recovered message",
			zap.String("msgID", msg.ID),
		)
	}

	logger.Info("Completed recovery for strand")
	return nil
}
