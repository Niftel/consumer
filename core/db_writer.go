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
	eventData := interface{}(evt.EventData)
	if evt.Diagnostic != nil {
		eventData = evt.Diagnostic
	}
	eventDataJSON, _ := json.Marshal(eventData)

	var hostID interface{}
	if evt.Host != nil && *evt.Host != "" {
		var resolvedHostID int64
		if err := tx.GetContext(ctx, &resolvedHostID, `
			SELECT h.id
			FROM unified_jobs uj
			JOIN job_templates jt ON jt.unified_job_template_id = uj.unified_job_template_id
			JOIN hosts h ON h.inventory_id = jt.inventory_id
			WHERE uj.id = $1 AND h.name = $2`, evt.UnifiedJobID, *evt.Host); err == nil {
			hostID = resolvedHostID
		}
	}

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
		hostID, evt.TaskName, evt.PlayName, eventDataJSON, evt.StdoutSnippet, evt.Timestamp,
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
	transitioned := false
	if newlyInserted {
		if evt.Diagnostic != nil && hostID != nil && evt.Diagnostic.Outcome != "" {
			if err := updateHostSummary(ctx, tx, evt, hostID.(int64)); err != nil {
				return err
			}
		}
		// Sequence progress is independent of lifecycle state. Task and narration
		// events advance it even though they do not change state.
		if _, err := tx.ExecContext(ctx, `
			UPDATE execution_runs
			SET last_event_seq = GREATEST(last_event_seq, $1)
			WHERE id = $2`, evt.Seq, evt.ExecutionRunID); err != nil {
			return fmt.Errorf("update run event sequence failed: %w", err)
		}

		transitioned, err = w.updateRunState(ctx, tx, evt)
		if err != nil {
			return fmt.Errorf("update run state failed: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	if newlyInserted {
		EventsProjected.Inc()
		if transitioned {
			if transition, ok := TransitionForEvent(evt.EventType); ok && transition.Terminal {
				TerminalTransitions.WithLabelValues(transition.RunState).Inc()
			}
			w.Notifier.Dispatch(evt) // only an accepted transition has lifecycle side effects
		}
	}
	return nil
}

func updateHostSummary(ctx context.Context, tx *sqlx.Tx, evt events.JobEvent, hostID int64) error {
	var changed, failed, okCount, skipped, unreachable int
	switch evt.Diagnostic.Outcome {
	case "changed":
		changed = 1
	case "failed":
		failed = 1
	case "ok":
		okCount = 1
	case "skipped":
		skipped = 1
	case "unreachable":
		unreachable = 1
	default:
		return nil
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO job_host_summaries
			(unified_job_id, host_id, changed, failed, ok, skipped, unreachable, last_event_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (unified_job_id, host_id) DO UPDATE SET
			changed = job_host_summaries.changed + EXCLUDED.changed,
			failed = job_host_summaries.failed + EXCLUDED.failed,
			ok = job_host_summaries.ok + EXCLUDED.ok,
			skipped = job_host_summaries.skipped + EXCLUDED.skipped,
			unreachable = job_host_summaries.unreachable + EXCLUDED.unreachable,
			last_event_at = GREATEST(job_host_summaries.last_event_at, EXCLUDED.last_event_at)`,
		evt.UnifiedJobID, hostID, changed, failed, okCount, skipped, unreachable, evt.Timestamp)
	if err != nil {
		return fmt.Errorf("update job host summary failed: %w", err)
	}
	return nil
}

func (w *DBWriter) updateRunState(ctx context.Context, tx *sqlx.Tx, evt events.JobEvent) (bool, error) {
	transition, lifecycle := TransitionForEvent(evt.EventType)
	if !lifecycle {
		return false, nil
	}

	// Lock the run so the decision, projection, and lifecycle side effects all
	// agree even when the reconciler or scheduler is updating the same row.
	var currentState string
	if err := tx.GetContext(ctx, &currentState,
		`SELECT state FROM execution_runs WHERE id = $1 FOR UPDATE`, evt.ExecutionRunID); err != nil {
		return false, err
	}
	newState, accepted := NextRunState(currentState, evt.EventType)
	if !accepted {
		return false, nil
	}

	// Compute the finish timestamp only for terminal events; COALESCE keeps the
	// earliest started_at / first finished_at across duplicate or replayed
	// events.
	var finishedAt interface{}
	if transition.Terminal {
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
			finished_at = COALESCE($3, finished_at)
		WHERE id = $4
		  AND NOT run_is_terminal(state)`,
		newState, evt.Timestamp, finishedAt, evt.ExecutionRunID,
	); err != nil {
		logger.Error("update execution_run failed", "run_id", evt.ExecutionRunID, "err", err)
		return false, err
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE unified_jobs SET
			status = $1,
			started_at = COALESCE(started_at, $2),
			finished_at = COALESCE($3, finished_at)
		WHERE id = $4
		  AND NOT job_is_terminal(status)`,
		transition.JobState, evt.Timestamp, finishedAt, evt.UnifiedJobID,
	); err != nil {
		logger.Error("update unified_job failed", "job_id", evt.UnifiedJobID, "err", err)
		return false, err
	}

	return true, nil
}
