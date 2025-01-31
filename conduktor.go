package main

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

// Conduktor manages sending and receiving messages
type Conduktor struct {
	mu          sync.Mutex
	wire        Wire
	store       Store
	strandConfs map[string]StrandConf
	strandMsgs  map[string][]Msg
}

// ConduktorMake initializes a new message queue
func ConduktorMake(store Store, wire Wire) *Conduktor {
	return &Conduktor{
		store:       store,
		wire:        wire,
		strandConfs: make(map[string]StrandConf),
		strandMsgs:  make(map[string][]Msg),
	}
}

// ConfStrand allows configuration of individual channels
func (mq *Conduktor) ConfStrand(channel string, config StrandConf) {
	mq.mu.Lock()
	defer mq.mu.Unlock()
	mq.strandConfs[channel] = config
}

// Send places a message in the queue and sends it via the configured transport
func (mq *Conduktor) Send(channel string, payload string) error {
	mq.mu.Lock()
	defer mq.mu.Unlock()

	config, exists := mq.strandConfs[channel]
	if !exists {
		return errors.New("channel not configured")
	}

	msg := Msg{
		ID:        fmt.Sprintf("%d", time.Now().UnixNano()),
		Channel:   channel,
		Payload:   payload,
		Acked:     false,
		Timestamp: time.Now().Unix(),
	}

	// Store the message if durability is enabled
	if config.Durable {
		if err := mq.store.Save(msg); err != nil {
			return err
		}
	}

	// Send via transport
	if err := mq.wire.SendMessage(msg); err != nil {
		logger.Error("Message send failed", zap.String("channel", channel), zap.Error(err))
		return err
	}

	// Queue the message for tracking
	mq.strandMsgs[channel] = append(mq.strandMsgs[channel], msg)
	messagesSent.WithLabelValues(channel).Inc()
	queueSize.WithLabelValues(channel).Set(float64(len(mq.strandMsgs[channel])))

	return nil
}

// Receive retrieves the next message from the queue via transport
func (mq *Conduktor) Receive(channel string) *Msg {
	mq.mu.Lock()
	defer mq.mu.Unlock()

	// Check if the channel is configured
	if _, exists := mq.strandConfs[channel]; !exists {
		logger.Warn("Channel not configured", zap.String("channel", channel))
		return nil
	}

	// Attempt to receive from the transport
	msg, err := mq.wire.ReceiveMessage(channel)
	if err != nil {
		logger.Warn("No messages available", zap.String("channel", channel), zap.Error(err))
		return nil
	}

	// Track received messages
	messagesReceived.WithLabelValues(channel).Inc()
	logger.Debug("Message received",
		zap.String("channel", channel),
		zap.String("payload", msg.Payload),
	)

	return msg
}

// Acknowledge marks a message as processed
func (mq *Conduktor) Acknowledge(channel, msgID string) error {
	mq.mu.Lock()
	defer mq.mu.Unlock()

	config, exists := mq.strandConfs[channel]
	if !exists {
		return errors.New("channel not configured")
	}

	// Mark message as acknowledged in durable storage
	if err := mq.store.Acknowledge(channel, msgID); err != nil && config.Durable {
		logger.Error("Acknowledgment failed", zap.String("channel", channel), zap.Error(err))
		return err
	}

	// Remove acknowledged message from queue
	for i, msg := range mq.strandMsgs[channel] {
		if msg.ID == msgID {
			mq.strandMsgs[channel] = append(mq.strandMsgs[channel][:i], mq.strandMsgs[channel][i+1:]...)
			break
		}
	}

	queueSize.WithLabelValues(channel).Set(float64(len(mq.strandMsgs[channel])))

	logger.Warn("Message acknowledged",
		zap.String("channel", channel),
		zap.String("msgID", msgID),
	)

	return nil
}

// RecoverUnackedMessages reloads unacknowledged messages from storage and retries sending them.
func (mq *Conduktor) RecoverUnackedMessages(channel string) error {
	mq.mu.Lock()
	defer mq.mu.Unlock()

	config, exists := mq.strandConfs[channel]
	if !exists {
		logger.Warn("Channel not configured for recovery", zap.String("channel", channel))
		return errors.New("channel not configured")
	}

	// Only recover messages if durability is enabled
	if !config.Durable {
		logger.Warn("Skipping recovery for non-durable channel", zap.String("channel", channel))
		return nil
	}

	// Load unacknowledged messages from the store
	unackedMessages, err := mq.store.LoadUnacked(channel)
	if err != nil {
		logger.Error("Failed to load unacknowledged messages", zap.String("channel", channel), zap.Error(err))
		return err
	}

	logger.Warn("Recovering unacknowledged messages",
		zap.String("channel", channel),
		zap.Int("count", len(unackedMessages)),
	)

	// Attempt to resend each unacknowledged message
	for _, msg := range unackedMessages {
		if err := mq.wire.SendMessage(msg); err != nil {
			logger.Error("Failed to resend unacked message",
				zap.String("channel", channel),
				zap.String("msgID", msg.ID),
				zap.Error(err),
			)
			continue // Skip this message but continue with others
		}
		mq.strandMsgs[channel] = append(mq.strandMsgs[channel], msg)
	}

	return nil
}
