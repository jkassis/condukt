package main

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"go.uber.org/zap"
)

// BadgerStore implements a durable message store using BadgerDB.
type BadgerStore struct {
	db *badger.DB
	mu sync.Mutex
}

// BadgerStoreMake initializes and opens a BadgerDB-backed message store.
func BadgerStoreMake(path string) (*BadgerStore, error) {
	opts := badger.DefaultOptions(path).WithSyncWrites(true).WithLoggingLevel(badger.ERROR)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &BadgerStore{db: db}, nil
}

// Save persists a message to BadgerDB.
func (s *BadgerStore) Save(msg Msg) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("%s:%s", msg.Channel, msg.ID)

	err = s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), data)
	})

	if err == nil {
		logger.Debug("Message saved to BadgerDB",
			zap.String("channel", msg.Channel),
			zap.String("msgID", msg.ID),
		)
	}
	return err
}

// LoadUnacked retrieves all unacknowledged messages from a channel.
func (s *BadgerStore) LoadUnacked(channel string) ([]Msg, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var messages []Msg

	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		prefix := []byte(channel + ":")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			err := item.Value(func(val []byte) error {
				var msg Msg
				if err := json.Unmarshal(val, &msg); err != nil {
					return err
				}
				messages = append(messages, msg)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err == nil {
		logger.Warn("Loaded unacked messages from BadgerDB",
			zap.String("channel", channel),
			zap.Int("count", len(messages)),
		)
	}
	return messages, err
}

// Acknowledge marks a message as processed and removes it from BadgerDB.
func (s *BadgerStore) Acknowledge(channel, msgID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := fmt.Sprintf("%s:%s", channel, msgID)

	err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})

	if err == nil {
		logger.Debug("Message acknowledged and deleted from BadgerDB",
			zap.String("channel", channel),
			zap.String("msgID", msgID),
		)
	}
	return err
}

// Clear removes all messages for a specific channel.
func (s *BadgerStore) Clear(channel string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.db.Update(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		prefix := []byte(channel + ":")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.KeyCopy(nil)
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		return nil
	})

	if err == nil {
		logger.Warn("All messages cleared from BadgerDB",
			zap.String("channel", channel),
		)
	}
	return err
}

// Close closes the BadgerDB connection.
func (s *BadgerStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	logger.Warn("Closing BadgerDB")
	return s.db.Close()
}
