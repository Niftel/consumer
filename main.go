package main

import (
	"context"
	"log"

	"github.com/praetordev/consumer/core"
	"github.com/praetordev/crypto"
	"github.com/praetordev/db"
	"github.com/praetordev/env"
	"github.com/praetordev/eventbus"
	"github.com/praetordev/metrics"
	"github.com/praetordev/plog"
)

func main() {
	plog.Configure("consumer")
	log.Println("Starting Event Consumer Service...")

	// Fail fast on a missing/invalid encryption key (used to decrypt
	// notification target URLs before dispatch).
	if err := crypto.ValidateSecrets(false); err != nil {
		log.Fatalf("secrets misconfigured: %v", err)
	}

	// 1. Connect to DB
	database, err := db.Connect(env.String("DATABASE_URL", db.DefaultDSN))
	if err != nil {
		log.Fatalf("Failed to connect to DB: %v", err)
	}

	// 2. Setup Infrastructure
	bus, err := eventbus.NewBus(env.String("NATS_URL", eventbus.DefaultURL))
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer bus.Close()

	metrics.Serve("")

	// 3. Create Consumer (with notification dispatch on lifecycle events)
	writer := core.NewDBWriter(database)
	writer.Notifier = core.NewNotifier(database)
	go func() {
		if err := writer.Notifier.Run(context.Background()); err != nil {
			log.Printf("Notification worker stopped: %v", err)
		}
	}()
	consumer := core.NewConsumer(bus, writer)

	// 4. Start
	if err := consumer.Start(); err != nil {
		log.Fatalf("Consumer failed: %v", err)
	}
}
