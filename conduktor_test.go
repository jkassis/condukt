package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// ConduktorTestFactory creates two Conduktors (sender & receiver) communicating over the same wire.
func ConduktorTestFactory() (sender *Conduktor, receiver *Conduktor, reload func()) {
	// Create separate stores for sender and receiver
	os.RemoveAll("/tmp/badger_test_db_sender")
	senderVolatileStore := RamStoreMake()
	senderDurableStore, _ := BadgerStoreMake("/tmp/badger_test_db_sender")

	os.RemoveAll("/tmp/badger_test_db_receiver")
	receiverVolatileStore := RamStoreMake()
	receiverDurableStore, _ := BadgerStoreMake("/tmp/badger_test_db_receiver")

	// Create a shared wire for communication
	wire := GoChanWireMake()

	// Initialize sender and receiver Conduktors
	sender = ConduktorMake(senderVolatileStore, senderDurableStore, wire)
	receiver = ConduktorMake(receiverVolatileStore, receiverDurableStore, wire)

	// Reload function to clear both stores
	reload = func() {
		wire.Reset()
		senderVolatileStore.Reload()
		senderDurableStore.Reload()
		receiverVolatileStore.Reload()
		receiverDurableStore.Reload()
	}

	return sender, receiver, reload
}

// Test Ordered Queue (FIFO)
func TestOrderedQueue(t *testing.T) {
	sender, receiver, _ := ConduktorTestFactory()

	sender.StrandAdd("ordered_channel", StrandConf{Durable: false, Ordered: true})

	// Send messages in order
	sender.Send("ordered_channel", "Message 1")
	sender.Send("ordered_channel", "Message 2")
	sender.Send("ordered_channel", "Message 3")

	// Ensure messages are received in order
	msg1, _ := receiver.Receive("ordered_channel")
	assert.NotNil(t, msg1)
	assert.Equal(t, "Message 1", msg1.Payload)

	msg2, _ := receiver.Receive("ordered_channel")
	assert.NotNil(t, msg2)
	assert.Equal(t, "Message 2", msg2.Payload)

	msg3, _ := receiver.Receive("ordered_channel")
	assert.NotNil(t, msg3)
	assert.Equal(t, "Message 3", msg3.Payload)
}

// Test Unordered Queue (Should be Out of Order)
func TestUnorderedQueue(t *testing.T) {
	sender, receiver, _ := ConduktorTestFactory()

	sender.StrandAdd("unordered_channel", StrandConf{Durable: false, Ordered: false})

	sender.Send("unordered_channel", "A")
	sender.Send("unordered_channel", "B")
	sender.Send("unordered_channel", "C")

	msgs := map[string]bool{}
	for i := 0; i < 3; i++ {
		msg, _ := receiver.Receive("unordered_channel")
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
	sender, receiver, reload := ConduktorTestFactory()

	sender.StrandAdd("durable_channel", StrandConf{Durable: true, Ordered: true})

	sender.Send("durable_channel", "Persistent 1")
	sender.Send("durable_channel", "Persistent 2")

	// Simulate restart by calling Reload
	reload()

	// Recover unacked messages
	sender.RecoverUnackedMessages()
	receiver.RecoverUnackedMessages()

	msg1, _ := receiver.Receive("durable_channel")
	assert.NotNil(t, msg1)
	assert.Equal(t, "Persistent 1", msg1.Payload)

	msg2, _ := receiver.Receive("durable_channel")
	assert.NotNil(t, msg2)
	assert.Equal(t, "Persistent 2", msg2.Payload)
}

// Test Non-Durable Queue (Should Not Persist)
func TestNonDurableQueue(t *testing.T) {
	sender, receiver, reset := ConduktorTestFactory()
	defer reset()

	sender.StrandAdd("non_durable_channel", StrandConf{Durable: false, Ordered: true})

	sender.Send("non_durable_channel", "Ephemeral 1")
	sender.Send("non_durable_channel", "Ephemeral 2")

	// Simulate restart by calling Reload
	reset()

	msg1, _ := receiver.Receive("non_durable_channel")
	assert.Nil(t, msg1)
}
