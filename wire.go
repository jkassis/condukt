package main

// Wire defines the interface for sending messages via different transports
type Wire interface {
	SendMessage(msg Msg) error
	ReceiveMessage(channel string) (*Msg, error) // Receiver function restored
}
