package main

import (
	"sync"
)

// RamStore (fast but volatile)
type RamStore struct {
	mu    sync.Mutex
	store map[string][]Msg
}

func RamStoreMake() *RamStore {
	return &RamStore{store: make(map[string][]Msg)}
}

func (s *RamStore) Save(msg Msg) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.store[msg.Channel] = append(s.store[msg.Channel], msg)
	return nil
}

func (s *RamStore) LoadUnacked(channel string) ([]Msg, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.store[channel], nil
}

func (s *RamStore) Acknowledge(channel, msgID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.store[channel] = nil
	return nil
}

func (s *RamStore) Clear(channel string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.store, channel)
	return nil
}

func (s *RamStore) Close() error { return nil }
