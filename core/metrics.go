package core

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// EventsProjected counts newly-projected job events (redeliveries excluded).
	EventsProjected = promauto.NewCounter(prometheus.CounterOpts{
		Name: "praetor_consumer_events_projected_total",
		Help: "Job events newly written to the DB (idempotent redeliveries excluded).",
	})

	// TerminalTransitions counts runs reaching a terminal status, by status.
	TerminalTransitions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "praetor_consumer_terminal_transitions_total",
		Help: "Runs transitioned to a terminal status by the consumer, by status.",
	}, []string{"status"})
)
