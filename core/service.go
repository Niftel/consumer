package core

import (
	"context"
	"github.com/praetordev/plog"

	"github.com/praetordev/events"
)

// logger is the consumer package component logger (handler installed by pkg/plog).
var logger = plog.New("consumer")

// EventConsumer delivers job events and log-chunk references to handlers and
// acknowledges each one only when its handler returns nil. A non-nil return
// keeps the message in the durable stream for redelivery, so the database is
// the gate on acknowledgement.
type EventConsumer interface {
	ConsumeJobEvents(handler func(events.JobEvent) error) error
	ConsumeLogChunks(handler func(events.LogChunk) error) error
}

type Consumer struct {
	Subscriber EventConsumer
	Writer     *DBWriter
}

func NewConsumer(sub EventConsumer, writer *DBWriter) *Consumer {
	return &Consumer{
		Subscriber: sub,
		Writer:     writer,
	}
}

func (c *Consumer) Start() error {
	logger.Info("consumer started, waiting for events")

	// The handler's error is the ack signal: returning nil acks the message,
	// returning an error (e.g. the DB is unavailable) leaves it in the stream
	// for redelivery once we recover.
	if err := c.Subscriber.ConsumeJobEvents(func(evt events.JobEvent) error {
		if err := c.processEvent(evt); err != nil {
			logger.Error("process event failed (will retry)", "seq", evt.Seq, "run_id", evt.ExecutionRunID, "err", err)
			return err
		}
		logger.Info("processed event", "event_type", evt.EventType, "seq", evt.Seq, "job_id", evt.UnifiedJobID)
		return nil
	}); err != nil {
		return err
	}

	// Log-chunk references are indexed on the same ack-after-commit contract.
	if err := c.Subscriber.ConsumeLogChunks(func(chunk events.LogChunk) error {
		if err := c.Writer.WriteLogChunk(context.Background(), chunk); err != nil {
			logger.Error("index log chunk failed (will retry)", "seq", chunk.Seq, "run_id", chunk.ExecutionRunID, "err", err)
			return err
		}
		logger.Info("indexed log chunk", "seq", chunk.Seq, "run_id", chunk.ExecutionRunID)
		return nil
	}); err != nil {
		return err
	}

	// Delivery runs on background goroutines; block forever.
	select {}
}

func (c *Consumer) processEvent(evt events.JobEvent) error {
	// Delegate to DBWriter
	ctx := context.Background()
	return c.Writer.WriteEvent(ctx, evt)
}
