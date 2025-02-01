package main

type Msg struct {
	ID        string
	Strand    string
	Payload   string
	Acked     bool
	Timestamp int64
}
