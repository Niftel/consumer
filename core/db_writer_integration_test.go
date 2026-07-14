package core_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/praetordev/consumer/core"
	"github.com/praetordev/events"
)

// TestDBWriterResilience verifies the two P0 guarantees of the event consumer
// against a real Postgres instance:
//
//  1. Idempotency  — replaying the same (execution_run_id, seq) event is a no-op,
//     so at-least-once redelivery from the durable stream never duplicates rows
//     or fails the transaction.
//  2. Monotonicity — an out-of-order or replayed non-terminal event cannot
//     regress a run that has already reached a terminal state.
//
// Set TEST_DATABASE_URL to a migrated praetor database to run it; it skips
// otherwise so the unit-test suite stays infra-free.
func TestDBWriterResilience(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping DB integration test")
	}

	db, err := sqlx.Connect("postgres", dbURL)
	if err != nil {
		t.Skipf("cannot reach TEST_DATABASE_URL: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	writer := core.NewDBWriter(db)

	// --- Fixture: one job + one run ---
	var jobID int64
	if err := db.QueryRow(
		`INSERT INTO unified_jobs (name, status) VALUES ('p0-resilience-test', 'pending') RETURNING id`,
	).Scan(&jobID); err != nil {
		t.Fatalf("insert unified_job: %v", err)
	}
	runID := uuid.New()
	if _, err := db.Exec(
		`INSERT INTO execution_runs (id, unified_job_id, state) VALUES ($1, $2, 'pending')`,
		runID, jobID,
	); err != nil {
		t.Fatalf("insert execution_run: %v", err)
	}
	t.Cleanup(func() {
		// ON DELETE CASCADE removes the run and its events.
		_, _ = db.Exec(`DELETE FROM unified_jobs WHERE id = $1`, jobID)
	})

	started := events.JobEvent{
		ExecutionRunID: runID,
		UnifiedJobID:   jobID,
		Seq:            1,
		EventType:      "JOB_STARTED",
		Timestamp:      time.Now(),
	}

	// 1. Idempotency: write the same event twice.
	if err := writer.WriteEvent(ctx, started); err != nil {
		t.Fatalf("first JOB_STARTED write: %v", err)
	}
	if err := writer.WriteEvent(ctx, started); err != nil {
		t.Fatalf("replayed JOB_STARTED must not error (idempotent): %v", err)
	}

	var eventCount int
	if err := db.Get(&eventCount, `SELECT count(*) FROM job_events WHERE execution_run_id = $1 AND seq = 1`, runID); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if eventCount != 1 {
		t.Fatalf("expected exactly 1 row for (run, seq=1) after replay, got %d", eventCount)
	}

	if got := runState(t, db, runID); got != "running" {
		t.Fatalf("expected run state 'running' after JOB_STARTED, got %q", got)
	}

	// 2. Reach a terminal state.
	completed := events.JobEvent{
		ExecutionRunID: runID,
		UnifiedJobID:   jobID,
		Seq:            5,
		EventType:      "JOB_COMPLETED",
		Timestamp:      time.Now(),
	}
	if err := writer.WriteEvent(ctx, completed); err != nil {
		t.Fatalf("JOB_COMPLETED write: %v", err)
	}
	if got := runState(t, db, runID); got != "successful" {
		t.Fatalf("expected run state 'successful' after JOB_COMPLETED, got %q", got)
	}

	// 3. Monotonicity: a late/duplicate JOB_STARTED must NOT regress the run.
	lateStart := started
	lateStart.Seq = 2 // a different seq so the event row itself is allowed in
	lateStart.Timestamp = time.Now()
	if err := writer.WriteEvent(ctx, lateStart); err != nil {
		t.Fatalf("late JOB_STARTED write: %v", err)
	}
	if got := runState(t, db, runID); got != "successful" {
		t.Fatalf("monotonicity violated: run regressed from terminal to %q", got)
	}

	// 4. A conflicting terminal event is retained for diagnosis but cannot replace
	// the first accepted executor outcome. Its sequence still advances the run's
	// observed event cursor.
	conflictingFailure := events.JobEvent{
		ExecutionRunID: runID,
		UnifiedJobID:   jobID,
		Seq:            6,
		EventType:      "JOB_FAILED",
		Timestamp:      time.Now(),
	}
	if err := writer.WriteEvent(ctx, conflictingFailure); err != nil {
		t.Fatalf("conflicting terminal write: %v", err)
	}
	if got := runState(t, db, runID); got != "successful" {
		t.Fatalf("conflicting terminal event replaced executor outcome with %q", got)
	}
	var lastSeq int64
	if err := db.Get(&lastSeq, `SELECT last_event_seq FROM execution_runs WHERE id = $1`, runID); err != nil {
		t.Fatalf("get last_event_seq: %v", err)
	}
	if lastSeq != 6 {
		t.Fatalf("last_event_seq = %d, want 6", lastSeq)
	}

	var jobStatus string
	if err := db.Get(&jobStatus, `SELECT status FROM unified_jobs WHERE id = $1`, jobID); err != nil {
		t.Fatalf("get job status: %v", err)
	}
	if jobStatus != "successful" {
		t.Fatalf("expected unified_job status 'successful', got %q", jobStatus)
	}
}

func runState(t *testing.T, db *sqlx.DB, runID uuid.UUID) string {
	t.Helper()
	var state string
	if err := db.Get(&state, `SELECT state FROM execution_runs WHERE id = $1`, runID); err != nil {
		t.Fatalf("get run state: %v", err)
	}
	return state
}
