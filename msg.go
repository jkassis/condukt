package main

type Msg struct {
	ID        string
	Channel   string
	Payload   string
	Acked     bool
	Timestamp int64
}
