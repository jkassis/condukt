package main

import (
	"testing"
)

// Benchmark Ordered Sends (Durable) - Parallel
func BenchmarkOrderedSendDurableParallel(b *testing.B) {
	store, _ := BadgerStoreMake("/tmp/badger_benchmark_parallel_db")
	defer store.Close()

	sender := GoChanWireMake()
	mq := ConduktorMake(store, sender)
	mq.ConfStrand("ordered_durable", StrandConf{Durable: true, Ordered: true})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mq.Send("ordered_durable", "Parallel Durable Ordered")
		}
	})
}

// Benchmark Ordered Sends (Non-Durable) - Parallel
func BenchmarkOrderedSendNonDurableParallel(b *testing.B) {
	store := RamStoreMake()
	sender := GoChanWireMake()
	mq := ConduktorMake(store, sender)
	mq.ConfStrand("ordered_non_durable", StrandConf{Durable: false, Ordered: true})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mq.Send("ordered_non_durable", "Parallel Non-Durable Ordered")
		}
	})
}

// Benchmark Unordered Sends (Durable) - Parallel
func BenchmarkUnorderedSendDurableParallel(b *testing.B) {
	store, _ := BadgerStoreMake("/tmp/badger_benchmark_parallel_db")
	defer store.Close()

	sender := GoChanWireMake()
	mq := ConduktorMake(store, sender)
	mq.ConfStrand("unordered_durable", StrandConf{Durable: true, Ordered: false})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mq.Send("unordered_durable", "Parallel Durable Unordered")
		}
	})
}

// Benchmark Unordered Sends (Non-Durable) - Parallel
func BenchmarkUnorderedSendNonDurableParallel(b *testing.B) {
	store := RamStoreMake()
	sender := GoChanWireMake()
	mq := ConduktorMake(store, sender)
	mq.ConfStrand("unordered_non_durable", StrandConf{Durable: false, Ordered: false})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mq.Send("unordered_non_durable", "Parallel Non-Durable Unordered")
		}
	})
}
