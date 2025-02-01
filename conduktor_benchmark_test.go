package main

import (
	"testing"
)

// Benchmark Ordered Sends (Durable)
func BenchmarkOrderedSendDurable(b *testing.B) {
	sender, receiver, reload := ConduktorTestFactory()
	defer reload()

	sender.StrandAdd("ordered_durable", StrandConf{Durable: true, Ordered: true})

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sender.Send("ordered_durable", "Durable Ordered Message")
		receiver.Receive("ordered_durable") // Ensure messages are received
	}
}

// Benchmark Ordered Sends (Non-Durable)
func BenchmarkOrderedSendNonDurable(b *testing.B) {
	sender, receiver, _ := ConduktorTestFactory()
	sender.StrandAdd("ordered_non_durable", StrandConf{Durable: false, Ordered: true})

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sender.Send("ordered_non_durable", "Non-Durable Ordered Message")
		receiver.Receive("ordered_non_durable") // Ensure messages are received
	}
}

// Benchmark Unordered Sends (Durable)
func BenchmarkUnorderedSendDurable(b *testing.B) {
	sender, receiver, _ := ConduktorTestFactory()
	sender.StrandAdd("unordered_durable", StrandConf{Durable: true, Ordered: false})

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sender.Send("unordered_durable", "Durable Unordered Message")
		receiver.Receive("unordered_durable") // Ensure messages are received
	}
}

// Benchmark Unordered Sends (Non-Durable)
func BenchmarkUnorderedSendNonDurable(b *testing.B) {
	sender, receiver, _ := ConduktorTestFactory()
	sender.StrandAdd("unordered_non_durable", StrandConf{Durable: false, Ordered: false})

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sender.Send("unordered_non_durable", "Non-Durable Unordered Message")
		receiver.Receive("unordered_non_durable") // Ensure messages are received
	}
}
