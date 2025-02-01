package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"go.uber.org/zap"
)

// BadgerStore implements a durable message store using BadgerDB.
type BadgerStore struct {
	db   *badger.DB
	mu   sync.Mutex
	path string // Store the original path
}

// BadgerStoreMake initializes and opens a BadgerDB-backed message store with sync writes enabled.
func BadgerStoreMake(path string) (*BadgerStore, error) {
	opts := badger.DefaultOptions(path).
		WithSyncWrites(true).          // Ensures writes are flushed to disk immediately
		WithLoggingLevel(badger.ERROR) // Reduce log noise

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	s := &BadgerStore{db: db, path: path}
	s.RecoverStrands() // Recover strands on startup
	return s, nil
}

// CreateStrand registers a new strand with a given configuration.
func (s *BadgerStore) CreateStrand(strandID string, config StrandConf) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	configData, err := json.Marshal(config)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("strand-config:%s", strandID)

	err = s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), configData)
	})

	if err == nil {
		logger.Debug("Strand created in BadgerDB", zap.String("strand", strandID))
	}
	return err
}

// DeleteStrand removes a strand and all associated messages.
func (s *BadgerStore) DeleteStrand(strandID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Delete all messages associated with the strand
	err := s.db.Update(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		prefix := []byte(strandID + ":")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.KeyCopy(nil)
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		return err
	}

	// Delete the strand configuration
	key := fmt.Sprintf("strand-config:%s", strandID)
	err = s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})

	if err == nil {
		logger.Info("Strand deleted from BadgerDB", zap.String("strand", strandID))
	}
	return err
}

// HasStrand checks if a strand exists in the store.
func (s *BadgerStore) HasStrand(strandID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := fmt.Sprintf("strand-config:%s", strandID)
	err := s.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(key))
		return err
	})

	return err == nil
}

// RecoverStrands restores all strands on startup.
func (s *BadgerStore) RecoverStrands() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		prefix := []byte("strand-config:")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			err := item.Value(func(val []byte) error {
				var config StrandConf
				if err := json.Unmarshal(val, &config); err != nil {
					return err
				}
				strandID := string(item.Key()[len(prefix):])
				logger.Debug("Recovered strand from BadgerDB", zap.String("strand", strandID))
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	return err
}

// Close closes the BadgerDB connection.
func (s *BadgerStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	logger.Debug("Closing BadgerDB")
	return s.db.Close()
}

// Reset clears all data in the BadgerStore by closing and reopening the database.
func (s *BadgerStore) Reset() error {
	// Close the database
	if err := s.db.Close(); err != nil {
		logger.Error("Failed to close BadgerDB during reset", zap.String("path", s.path), zap.Error(err))
		return err
	}

	// Wait for OS to release lock
	time.Sleep(500 * time.Millisecond)

	// Ensure the directory is empty before reopening
	os.RemoveAll(s.path)

	// Reopen the database using the same path
	opts := badger.DefaultOptions(s.path).WithSyncWrites(true).WithLoggingLevel(badger.ERROR)
	db, err := badger.Open(opts)
	if err != nil {
		logger.Error("Failed to reopen BadgerDB during reset", zap.String("path", s.path), zap.Error(err))
		return err
	}

	s.db = db
	logger.Debug("BadgerStore reset completed", zap.String("path", s.path))
	return nil
}

// Reload closes and reopens the BadgerDB store without deleting data.
func (s *BadgerStore) Reload() error {
	// Close the database
	if err := s.db.Close(); err != nil {
		logger.Error("Failed to close BadgerDB during reload", zap.String("path", s.path), zap.Error(err))
		return err
	}

	// Wait for OS to release lock
	time.Sleep(500 * time.Millisecond)

	// Reopen the database using the same path
	opts := badger.DefaultOptions(s.path).WithSyncWrites(true).WithLoggingLevel(badger.ERROR)
	db, err := badger.Open(opts)
	if err != nil {
		logger.Error("Failed to reopen BadgerDB during reload", zap.String("path", s.path), zap.Error(err))
		return err
	}

	s.db = db
	s.RecoverStrands()
	logger.Debug("BadgerStore reloaded", zap.String("path", s.path))
	return nil
}

// Save persists a message to BadgerDB with a "msg:" prefix.
func (s *BadgerStore) Save(msg Msg) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("msg:%s:%s", msg.Strand, msg.ID) // Updated key format

	err = s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), data)
	})

	if err == nil {
		logger.Debug("Message saved to BadgerDB", zap.String("strand", msg.Strand), zap.String("msgID", msg.ID))
	}
	return err
}

// Acknowledge marks a message as processed and removes it from BadgerDB.
func (s *BadgerStore) Acknowledge(strandID, msgID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := fmt.Sprintf("msg:%s:%s", strandID, msgID) // Updated key format

	err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})

	if err == nil {
		logger.Debug("Message acknowledged and deleted from BadgerDB", zap.String("strand", strandID), zap.String("msgID", msgID))
	}
	return err
}

// UnackedIterator returns an iterator over all unacknowledged messages across all strands.
func (s *BadgerStore) UnackedIterator() (UnackedMessageIterator, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	txn := s.db.NewTransaction(false)

	// Set iterator options with prefix "msg:"
	itOpts := badger.DefaultIteratorOptions
	itOpts.Prefix = []byte("msg:") // Ensures iteration starts at "msg:"
	it := txn.NewIterator(itOpts)
	it.Rewind()
	if !it.ValidForPrefix(itOpts.Prefix) {
		it.Close()
		txn.Discard()
		return nil, fmt.Errorf("no unacknowledged messages found")
	}

	return &BadgerUnackedIterator{txn: txn, it: it, prefix: itOpts.Prefix}, nil
}

// BadgerUnackedIterator iterates over unacknowledged messages in BadgerDB.
type BadgerUnackedIterator struct {
	txn    *badger.Txn
	it     *badger.Iterator
	prefix []byte
}

// Next retrieves the next unacknowledged message across all strands.
func (it *BadgerUnackedIterator) Next() (*Msg, bool) {
	if it.it.ValidForPrefix(it.prefix) {
		item := it.it.Item()
		var msg Msg
		err := item.Value(func(val []byte) error {
			return json.Unmarshal(val, &msg)
		})
		if err != nil {
			return nil, false
		}

		it.it.Next()
		return &msg, true
	}
	return nil, false
}

// Close cleans up iterator resources.
func (it *BadgerUnackedIterator) Close() error {
	it.it.Close()
	it.txn.Discard()
	return nil
}
