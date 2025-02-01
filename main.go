package main

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

var logger *zap.Logger

func init() {
	cfg := zap.NewProductionConfig()
	cfg.OutputPaths = []string{"stdout"}
	cfg.ErrorOutputPaths = []string{"stderr"}

	var err error
	logger, err = cfg.Build()
	if err != nil {
		panic("Failed to initialize logger: " + err.Error())
	}

	// Start Prometheus server
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		logger.Info("Prometheus metrics server started on :9090")
		http.ListenAndServe(":9090", nil)
	}()
}

func main() {
	// Choose transport (UDP, Channels, or WebSockets)
	// sender, _ := NewUDPSender("localhost:8081", 1400)
	// sender := NewChannelSender()
	sender := WSWireMake()

	// Choose storage (RAM or BadgerDB)
	// store := NewRamMessageStore()
	vStore := RamStoreMake()
	dStore, _ := BadgerStoreMake("/tmp/badgerdb")

	// Initialize Message Queue
	mq := ConduktorMake(vStore, dStore, sender)
	mq.StrandAdd("test_channel", StrandConf{Durable: true, Ordered: true})

	// Send and Receive Messages
	mq.Send("test_channel", "Hello via WebSockets!")

	go func() {
		receiver, _ := UDPWireMake("localhost:8081")
		for {
			receiver.ReceiveMessage("test_channel")
		}
	}()

	select {}
}
