package main

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	messagesSent = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "messages_sent_total", Help: "Total messages sent"},
		[]string{"channel"},
	)

	messagesReceived = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "messages_received_total", Help: "Total messages received"},
		[]string{"channel"},
	)

	queueSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "queue_size", Help: "Current message queue size"},
		[]string{"channel"},
	)
)

func init() {
	prometheus.MustRegister(messagesSent, messagesReceived, queueSize)
}
