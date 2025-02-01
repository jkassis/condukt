package main

// Store defines the interface for message storage and strand management.
type Store interface {
	// Strand Management
	CreateStrand(StrandID string, config StrandConf) error
	DeleteStrand(StrandID string) error
	HasStrand(StrandID string) bool // Check if a strand exists

	// Message Handling
	Save(msg Msg) error
	Acknowledge(StrandID, msgID string) error

	// Unacked Message Iterator
	UnackedIterator() (UnackedMessageIterator, error)

	// Close the store
	Close() error

	// Close and reopen
	Reload() error

	// Close and reopen
	Reset() error
}

// UnackedMessageIterator defines an interface for iterating over unacknowledged messages.
type UnackedMessageIterator interface {
	Next() (*Msg, bool) // Returns the next message and a bool indicating if more messages exist
	Close() error       // Cleans up the iterator resources
}
