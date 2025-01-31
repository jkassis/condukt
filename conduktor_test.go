package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Test Ordered Queue (FIFO)
func TestOrderedQueue(t *testing.T) {
	store := RamStoreMake()
	wire := GoChanWireMake()
	conduktor := ConduktorMake(store, wire)

	conduktor.ConfStrand("ordered_channel", StrandConf{Durable: false, Ordered: true})

	// Send messages in order
	conduktor.Send("ordered_channel", "Message 1")
	conduktor.Send("ordered_channel", "Message 2")
	conduktor.Send("ordered_channel", "Message 3")

	// Ensure messages are received in order
	msg1 := conduktor.Receive("ordered_channel")
	assert.NotNil(t, msg1)
	assert.Equal(t, "Message 1", msg1.Payload)

	msg2 := conduktor.Receive("ordered_channel")
	assert.NotNil(t, msg2)
	assert.Equal(t, "Message 2", msg2.Payload)

	msg3 := conduktor.Receive("ordered_channel")
	assert.NotNil(t, msg3)
	assert.Equal(t, "Message 3", msg3.Payload)
}

// Test Unordered Queue (Should be Out of Order)
func TestUnorderedQueue(t *testing.T) {
	store := RamStoreMake()
	wire := GoChanWireMake()
	conduktor := ConduktorMake(store, wire)

	conduktor.ConfStrand("unordered_channel", StrandConf{Durable: false, Ordered: false})

	conduktor.Send("unordered_channel", "A")
	conduktor.Send("unordered_channel", "B")
	conduktor.Send("unordered_channel", "C")

	msgs := map[string]bool{}
	for i := 0; i < 3; i++ {
		msg := conduktor.Receive("unordered_channel")
		assert.NotNil(t, msg)
		msgs[msg.Payload] = true
	}

	assert.Len(t, msgs, 3)
	assert.True(t, msgs["A"])
	assert.True(t, msgs["B"])
	assert.True(t, msgs["C"])
}

// Test Durable Queue (BadgerDB Persistence)
func TestDurableQueue(t *testing.T) {
	store, _ := BadgerStoreMake("/tmp/badger_test_db")
	defer store.Close()

	wire := GoChanWireMake()
	conduktor1 := ConduktorMake(store, wire)
	conduktor1.ConfStrand("durable_channel", StrandConf{Durable: true, Ordered: true})

	conduktor1.Send("durable_channel", "Persistent 1")
	conduktor1.Send("durable_channel", "Persistent 2")

	// Simulate restart
	wire2 := GoChanWireMake()
	conduktor1 = ConduktorMake(store, wire2)
	conduktor1.ConfStrand("durable_channel", StrandConf{Durable: true, Ordered: true})
	conduktor1.RecoverUnackedMessages("durable_channel")

	// Make receiver
	store2 := RamStoreMake()
	conduktor2 := ConduktorMake(store2, wire2)
	conduktor2.ConfStrand("durable_channel", StrandConf{Durable: true, Ordered: true})
	conduktor2.RecoverUnackedMessages("durable_channel")

	msg1 := conduktor2.Receive("durable_channel")
	assert.NotNil(t, msg1)
	assert.Equal(t, "Persistent 1", msg1.Payload)

	msg2 := conduktor2.Receive("durable_channel")
	assert.NotNil(t, msg2)
	assert.Equal(t, "Persistent 2", msg2.Payload)
}

// Test Non-Durable Queue (Should Not Persist)
func TestNonDurableQueue(t *testing.T) {
	store, _ := BadgerStoreMake("/tmp/badger_test_db")
	wire := GoChanWireMake()

	conduktor1 := ConduktorMake(store, wire)
	conduktor1.ConfStrand("non_durable_channel", StrandConf{Durable: false, Ordered: true})

	conduktor1.Send("non_durable_channel", "Ephemeral 1")
	conduktor1.Send("non_durable_channel", "Ephemeral 2")

	// Simulate restart
	wire2 := GoChanWireMake()
	conduktor1 = ConduktorMake(store, wire2)

	store2 := RamStoreMake()
	conduktor2 := ConduktorMake(store2, wire2)
	conduktor2.ConfStrand("non_durable_channel", StrandConf{Durable: false, Ordered: true})

	msg1 := conduktor2.Receive("non_durable_channel")
	assert.Nil(t, msg1)
}
