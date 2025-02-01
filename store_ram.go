package main

import (
	"errors"
	"sync"

	"go.uber.org/zap"
)

// RamStore (fast but volatile)
type RamStore struct {
	mu      sync.Mutex
	store   map[string][]Msg
	configs map[string]StrandConf
}

// RamStoreMake initializes an in-memory store.
func RamStoreMake() *RamStore {
	return &RamStore{
		store:   make(map[string][]Msg),
		configs: make(map[string]StrandConf),
	}
}

// CreateStrand registers a new strand with a given configuration.
func (s *RamStore) CreateStrand(strandID string, config StrandConf) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.configs[strandID]; exists {
		return errors.New("strand already exists")
	}

	s.configs[strandID] = config
	s.store[strandID] = []Msg{} // Initialize empty message slice
	return nil
}

// DeleteStrand removes a strand and all associated messages.
func (s *RamStore) DeleteStrand(strandID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.configs[strandID]; !exists {
		return errors.New("strand does not exist")
	}

	delete(s.store, strandID)
	delete(s.configs, strandID)
	return nil
}

// HasStrand checks if a strand exists in the store.
func (s *RamStore) HasStrand(strandID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, exists := s.configs[strandID]
	return exists
}

// RecoverStrands restores all strands on startup.
func (s *RamStore) RecoverStrands() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for strandID := range s.configs {
		logger.Debug("Recovered strand from RamStore", zap.String("strand", strandID))
	}
	return nil
}

// Save persists a message in memory.
func (s *RamStore) Save(msg Msg) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.configs[msg.Strand]; !exists {
		return errors.New("strand not configured")
	}

	s.store[msg.Strand] = append(s.store[msg.Strand], msg)
	return nil
}

// Acknowledge marks a message as processed by removing it from the queue.
func (s *RamStore) Acknowledge(strandID, msgID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.configs[strandID]; !exists {
		return errors.New("strand does not exist")
	}

	// Remove the acknowledged message from the store
	messages := s.store[strandID]
	for i, msg := range messages {
		if msg.ID == msgID {
			s.store[strandID] = append(messages[:i], messages[i+1:]...)
			return nil
		}
	}

	return errors.New("message not found")
}

// UnackedIterator returns an iterator over unacknowledged messages.
func (s *RamStore) UnackedIterator() (UnackedMessageIterator, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return &RamUnackedIterator{
		messages: []Msg{},
		index:    0,
	}, nil
}

// RamUnackedIterator implements UnackedMessageIterator for RamStore.
type RamUnackedIterator struct {
	messages []Msg
	index    int
}

// Next retrieves the next unacknowledged message.
func (it *RamUnackedIterator) Next() (*Msg, bool) {
	if it.index < len(it.messages) {
		msg := it.messages[it.index]
		it.index++
		return &msg, true
	}
	return nil, false
}

// Close cleans up iterator resources (no-op for RamStore).
func (it *RamUnackedIterator) Close() error {
	return nil
}

// Close is a no-op for an in-memory store.
func (s *RamStore) Close() error {
	return nil
}

// Reset clears all data in the RamStore.
func (s *RamStore) Reset() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Reset the internal storage
	s.store = make(map[string][]Msg)
	s.configs = make(map[string]StrandConf)

	logger.Debug("RamStore reset completed")
	return nil
}

func (s *RamStore) Reload() error {
	return s.Reset()
}
