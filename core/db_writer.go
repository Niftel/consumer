package core

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/praetordev/events"
)

type DBWriter struct {
	DB       *sqlx.DB
	Notifier *Notifier // optional; fires notifications on newly-projected lifecycle events

	// jobIDByRun caches execution_run_id -> unified_job_id. The consumer (the
	// Postgres boundary) resolves unified_job_id authoritatively from the run id
	// rather than trusting the value on the event, so ingestion needs no DB lookup
	// and stays available while Postgres is down (#16). The mapping is immutable
	// for a run, so the cache is grow-only and never needs invalidation.
	jobIDByRun sync.Map // uuid.UUID -> int64
}

func NewDBWriter(db *sqlx.DB) *DBWriter {
	return &DBWriter{DB: db}
}

// resolveJobID returns the unified_job_id owning runID, authoritatively from the
// DB (cached). This is the single place unified_job_id is trusted, keyed on the
// token-authenticated run id — not on a client-supplied field.
func (w *DBWriter) resolveJobID(ctx context.Context, runID uuid.UUID) (int64, error) {
	if v, ok := w.jobIDByRun.Load(runID); ok {
		return v.(int64), nil
	}
	var id int64
	if err := w.DB.GetContext(ctx, &id,
		`SELECT unified_job_id FROM execution_runs WHERE id = $1`, runID); err != nil {
		return 0, fmt.Errorf("resolve unified_job_id for run %s: %w", runID, err)
	}
	w.jobIDByRun.Store(runID, id)
	return id, nil
}

// WriteLogChunk indexes a log-chunk reference into job_output_chunks. The chunk
// bytes already live durably in the object store; this row is the pointer. The
// ON CONFLICT makes it idempotent so a redelivered or re-uploaded chunk is a
// no-op, which is what lets the consumer ack-after-commit.
func (w *DBWriter) WriteLogChunk(ctx context.Context, chunk events.LogChunk) error {
	_, err := w.DB.ExecContext(ctx, `
		INSERT INTO job_output_chunks (execution_run_id, seq, storage_key, byte_length, created_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (execution_run_id, seq) DO NOTHING`,
		chunk.ExecutionRunID, chunk.Seq, chunk.StorageKey, chunk.ByteLength, chunk.Timestamp,
	)
	if err != nil {
		return fmt.Errorf("insert job_output_chunk failed: %w", err)
	}
	return nil
}

// WriteEvent projects a JobEvent into the database.
func (w *DBWriter) WriteEvent(ctx context.Context, evt events.JobEvent) error {
	// Resolve unified_job_id authoritatively from the run id (not from the event's
	// own field, which ingestion no longer sets). This is the DB dependency that
	// ingestion shed to stay available during a Postgres outage (#16); it lives
	// here because the consumer is the Postgres boundary anyway.
	jobID, err := w.resolveJobID(ctx, evt.ExecutionRunID)
	if err != nil {
		return err
	}
	evt.UnifiedJobID = jobID

	tx, err := w.DB.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// 1. Insert into job_event table
	// Note: We used int64 for ID in models, but typically events might be inserted with DEFAULT id.
	// We need to map JobEvent fields to DB columns.
	eventDataJSON, _ := json.Marshal(evt.EventData)

	// ON CONFLICT makes the write idempotent: the (execution_run_id, seq) unique
	// constraint means a redelivered or replayed event is silently skipped
	// rather than failing the transaction. This is what allows the consumer to
	// safely ack-after-commit and tolerate at-least-once delivery.
	res, err := tx.ExecContext(ctx, `
		INSERT INTO job_events (
			unified_job_id, execution_run_id, seq, event_type,
			host_id, task_name, play_name, event_data, stdout_snippet, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (execution_run_id, seq) DO NOTHING`,
		evt.UnifiedJobID, evt.ExecutionRunID, evt.Seq, evt.EventType,
		nil, evt.TaskName, evt.PlayName, eventDataJSON, evt.StdoutSnippet, evt.Timestamp,
	)
	if err != nil {
		return fmt.Errorf("insert job_event failed: %w", err)
	}
	// Whether this event is new (not a redelivery). Notifications fire only for
	// newly-projected events so at-least-once delivery doesn't double-send.
	newlyInserted := false
	if n, _ := res.RowsAffected(); n > 0 {
		newlyInserted = true
	}

	// 2. Update execution_run state — only for a newly-projected event. A
	// redelivered event is deduped at the INSERT above (ON CONFLICT DO NOTHING);
	// its state transition was already applied on first delivery. Re-running it
	// here would let a duplicate JOB_STARTED regress a reconciler-set 'lost'/'error'
	// run back to 'running' (those states are intentionally non-terminal so a real
	// recovering terminal event can win — but a stale duplicate must not).
	if newlyInserted {
		if err := w.updateRunState(ctx, tx, evt); err != nil {
			return fmt.Errorf("update run state failed: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	if newlyInserted {
		EventsProjected.Inc()
		switch evt.EventType {
		case "JOB_COMPLETED":
			TerminalTransitions.WithLabelValues("successful").Inc()
		case "JOB_FAILED":
			TerminalTransitions.WithLabelValues("failed").Inc()
		case "JOB_CANCELED":
			TerminalTransitions.WithLabelValues("canceled").Inc()
		}
		w.Notifier.Dispatch(evt) // no-op on a nil notifier; sends in the background
	}
	return nil
}

func (w *DBWriter) updateRunState(ctx context.Context, tx *sqlx.Tx, evt events.JobEvent) error {
	var newState string
	var newStatus string // for unified_job
	finished := false

	switch evt.EventType {
	case "JOB_STARTED":
		newState = "running"
		newStatus = "running"
	case "JOB_COMPLETED":
		// JOB_COMPLETED means success: the host-runner inspects ansible-playbook's
		// exit code and emits JOB_FAILED on any non-zero rc, JOB_COMPLETED only on
		// rc 0 (cmd/host-runner/runner.go). So no rc re-check is needed here.
		newState = "successful"
		newStatus = "successful"
		finished = true
	case "JOB_FAILED":
		newState = "failed"
		newStatus = "failed"
		finished = true
	case "JOB_CANCELED":
		newState = "canceled"
		newStatus = "canceled"
		finished = true
	default:
		// Normal task events don't change state
		return nil
	}

	// Compute the finish timestamp only for terminal events; COALESCE keeps the
	// earliest started_at / first finished_at across duplicate or replayed
	// events.
	var finishedAt interface{}
	if finished {
		finishedAt = evt.Timestamp
	}

	// The `state NOT IN (<terminal>)` guard makes the projection monotonic: once
	// a run reaches a true terminal state we never overwrite it. Combined with
	// COALESCE/GREATEST this means an out-of-order or replayed event (e.g. a
	// redelivered JOB_STARTED arriving after JOB_COMPLETED) cannot regress final
	// state. Crucially, 'lost' (run) and 'error' (job) are NOT terminal: they are
	// the reconciler's provisional verdict for "the host stopped heartbeating".
	// If that host reboots, resumes the play, and reports a real terminal event,
	// it must win — so those provisional states are excluded from the guard and
	// recoverable. finished_at uses COALESCE($3, finished_at) so a recovering
	// terminal event replaces the reconciler's lost-detection timestamp with the
	// actual completion time (events are deduped by seq, so no double-write).
	if _, err := tx.ExecContext(ctx, `
		UPDATE execution_runs SET
			state = $1,
			started_at = COALESCE(started_at, $2),
			finished_at = COALESCE($3, finished_at),
			last_event_seq = GREATEST(last_event_seq, $4)
		WHERE id = $5
		  AND NOT run_is_terminal(state)`,
		newState, evt.Timestamp, finishedAt, evt.Seq, evt.ExecutionRunID,
	); err != nil {
		logger.Error("update execution_run failed", "run_id", evt.ExecutionRunID, "err", err)
		return err
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE unified_jobs SET
			status = $1,
			started_at = COALESCE(started_at, $2),
			finished_at = COALESCE($3, finished_at)
		WHERE id = $4
		  AND NOT job_is_terminal(status)`,
		newStatus, evt.Timestamp, finishedAt, evt.UnifiedJobID,
	); err != nil {
		logger.Error("update unified_job failed", "job_id", evt.UnifiedJobID, "err", err)
		return err
	}

	return nil
}
