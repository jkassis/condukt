package main

type Store interface {
	Save(msg Msg) error
	LoadUnacked(channel string) ([]Msg, error)
	Acknowledge(channel, msgID string) error
	Clear(channel string) error
	Close() error
}
