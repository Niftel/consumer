package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/praetordev/events"
	"github.com/praetordev/notify"
)

type notifierFixture struct {
	db                *sqlx.DB
	orgID             int64
	targetID          int64
	unifiedTemplateID int64
	templateID        int64
	jobID             int64
	sourceID          int64
	syncJobID         int64
}

func openNotifierTestDB(t *testing.T) *sqlx.DB {
	t.Helper()
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping notifier integration test")
	}
	db, err := sqlx.Connect("postgres", dbURL)
	if err != nil {
		t.Skipf("cannot reach TEST_DATABASE_URL: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func newNotifierFixture(t *testing.T, db *sqlx.DB, config json.RawMessage, inventory bool) notifierFixture {
	t.Helper()
	uniq := time.Now().UnixNano()
	f := notifierFixture{db: db}
	if err := db.Get(&f.orgID, `INSERT INTO organizations (name) VALUES ($1) RETURNING id`, fmt.Sprintf("consumer-notify-org-%d", uniq)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM organizations WHERE id=$1`, f.orgID) })
	if err := db.Get(&f.targetID, `
		INSERT INTO notification_templates (organization_id,name,notification_type,config)
		VALUES ($1,$2,'webhook',$3) RETURNING id`,
		f.orgID, fmt.Sprintf("consumer-notify-target-%d", uniq), config); err != nil {
		t.Fatal(err)
	}
	if err := db.Get(&f.unifiedTemplateID, `INSERT INTO unified_job_templates (name) VALUES ($1) RETURNING id`, fmt.Sprintf("consumer-notify-ujt-%d", uniq)); err != nil {
		t.Fatal(err)
	}
	if err := db.Get(&f.templateID, `
		INSERT INTO job_templates (organization_id,name,playbook,unified_job_template_id)
		VALUES ($1,$2,'site.yml',$3) RETURNING id`,
		f.orgID, fmt.Sprintf("consumer-notify-template-%d", uniq), f.unifiedTemplateID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO notification_policies (
			organization_id,notification_template_id,resource_type,resource_id,event
		) VALUES ($1,$2,'job_template',$3,'success')`,
		f.orgID, f.targetID, f.templateID); err != nil {
		t.Fatal(err)
	}
	if err := db.Get(&f.jobID, `
		INSERT INTO unified_jobs (unified_job_template_id,name,status)
		VALUES ($1,'Deploy application','successful') RETURNING id`, f.unifiedTemplateID); err != nil {
		t.Fatal(err)
	}
	if !inventory {
		return f
	}

	var inventoryID int64
	if err := db.Get(&inventoryID, `INSERT INTO inventories (organization_id,name) VALUES ($1,$2) RETURNING id`, f.orgID, fmt.Sprintf("consumer-notify-inventory-%d", uniq)); err != nil {
		t.Fatal(err)
	}
	if err := db.Get(&f.sourceID, `INSERT INTO inventory_sources (inventory_id,name) VALUES ($1,$2) RETURNING id`, inventoryID, fmt.Sprintf("consumer-notify-source-%d", uniq)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO notification_policies (
			organization_id,notification_template_id,resource_type,resource_id,event
		) VALUES ($1,$2,'inventory_source',$3,'error')`,
		f.orgID, f.targetID, f.sourceID); err != nil {
		t.Fatal(err)
	}
	if err := db.Get(&f.syncJobID, `
		INSERT INTO unified_jobs (name,status,job_args)
		VALUES ('Inventory synchronization','failed',jsonb_build_object('inventory_source_id',$1::bigint))
		RETURNING id`, f.sourceID); err != nil {
		t.Fatal(err)
	}
	return f
}

func enqueueNotification(t *testing.T, notifier *Notifier, evt events.JobEvent) {
	t.Helper()
	tx, err := notifier.DB.BeginTxx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := notifier.Enqueue(context.Background(), tx, evt); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func jobLifecycleEvent(jobID int64, eventType string, seq int64) events.JobEvent {
	return events.JobEvent{
		UnifiedJobID: jobID, ExecutionRunID: uuid.New(),
		EventType: eventType, Seq: seq, Timestamp: time.Now().UTC(),
	}
}

func TestNotifierEnqueuesAndDeliversJobAndInventoryPolicies(t *testing.T) {
	db := openNotifierTestDB(t)
	got := make(chan map[string]interface{}, 4)
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var message map[string]interface{}
		_ = json.Unmarshal(body, &message)
		got <- message
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(receiver.Close)
	receiverURL, _ := url.Parse(receiver.URL)
	t.Setenv("PRAETOR_NOTIFICATION_ALLOWED_HOSTS", receiverURL.Hostname())
	config, _ := json.Marshal(map[string]string{"url": receiver.URL})
	f := newNotifierFixture(t, db, config, true)
	notifier := NewNotifier(db)

	jobEvent := jobLifecycleEvent(f.jobID, "JOB_COMPLETED", 5)
	syncEvent := jobLifecycleEvent(f.syncJobID, "JOB_FAILED", 8)
	enqueueNotification(t, notifier, jobEvent)
	enqueueNotification(t, notifier, jobEvent) // event redelivery is idempotent
	enqueueNotification(t, notifier, syncEvent)

	for i := 0; i < 2; i++ {
		worked, err := notifier.ProcessNext(context.Background())
		if err != nil || !worked {
			t.Fatalf("ProcessNext() = (%t, %v), want work delivered", worked, err)
		}
	}
	messages := map[string]map[string]interface{}{}
	for len(messages) < 2 {
		select {
		case message := <-got:
			kind, _ := message["kind"].(string)
			if kind == "" {
				kind = "job"
			}
			messages[kind] = message
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for notification deliveries: %#v", messages)
		}
	}
	if messages["job"]["event"] != "success" || messages["job"]["job_name"] != "Deploy application" {
		t.Fatalf("job notification = %#v", messages["job"])
	}
	if _, added := messages["job"]["kind"]; added {
		t.Fatalf("ordinary job notification changed its wire shape: %#v", messages["job"])
	}
	if messages["inventory sync"]["event"] != "error" {
		t.Fatalf("inventory notification = %#v", messages["inventory sync"])
	}
	var deliveries, attempts int
	if err := db.Get(&deliveries, `SELECT count(*) FROM notification_deliveries WHERE organization_id=$1`, f.orgID); err != nil {
		t.Fatal(err)
	}
	if err := db.Get(&attempts, `
		SELECT count(*) FROM notification_delivery_attempts a
		JOIN notification_deliveries d ON d.id=a.delivery_id
		WHERE d.organization_id=$1 AND a.outcome='delivered'`, f.orgID); err != nil {
		t.Fatal(err)
	}
	if deliveries != 2 || attempts != 2 {
		t.Fatalf("deliveries=%d attempts=%d, want 2/2", deliveries, attempts)
	}
}

func TestNotifierEnqueueRollsBackWithProjectionTransaction(t *testing.T) {
	db := openNotifierTestDB(t)
	f := newNotifierFixture(t, db, json.RawMessage(`{"url":"https://example.invalid"}`), false)
	notifier := NewNotifier(db)
	event := jobLifecycleEvent(f.jobID, "JOB_COMPLETED", 11)
	tx, err := db.BeginTxx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := notifier.Enqueue(context.Background(), tx, event); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.Get(&count, `SELECT count(*) FROM notification_deliveries WHERE organization_id=$1`, f.orgID); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("rolled-back projection left %d notification delivery rows", count)
	}
}

func TestNotifierRetriesWithDeterministicBackoffAndRedaction(t *testing.T) {
	db := openNotifierTestDB(t)
	f := newNotifierFixture(t, db, json.RawMessage(`{"url":"https://example.invalid"}`), false)
	notifier := NewNotifier(db)
	now := time.Date(2026, 7, 23, 8, 0, 0, 0, time.UTC)
	notifier.Now = func() time.Time { return now }
	var calls atomic.Int32
	notifier.Send = func(context.Context, string, json.RawMessage, notify.Message) error {
		if calls.Add(1) == 1 {
			return errors.New("transport failed for https://secret.example/token and credential=secret")
		}
		return nil
	}
	enqueueNotification(t, notifier, jobLifecycleEvent(f.jobID, "JOB_COMPLETED", 1))

	if worked, err := notifier.ProcessNext(context.Background()); err != nil || !worked {
		t.Fatalf("first ProcessNext() = (%t, %v)", worked, err)
	}
	var status, code, reason string
	var next time.Time
	if err := db.QueryRowx(`
		SELECT status,failure_code,failure_reason,next_attempt_at
		FROM notification_deliveries WHERE organization_id=$1`, f.orgID).
		Scan(&status, &code, &reason, &next); err != nil {
		t.Fatal(err)
	}
	if status != "retrying" || code != "unclassified_failure" || !next.Equal(now.Add(5*time.Second)) {
		t.Fatalf("retry state=(%s,%s,%s), next=%s", status, code, reason, next)
	}
	if reason != "notification delivery failed" {
		t.Fatalf("unclassified error was not redacted: %q", reason)
	}
	if worked, err := notifier.ProcessNext(context.Background()); err != nil || worked {
		t.Fatalf("early ProcessNext() = (%t, %v), want no due work", worked, err)
	}
	now = now.Add(5 * time.Second)
	if worked, err := notifier.ProcessNext(context.Background()); err != nil || !worked {
		t.Fatalf("retry ProcessNext() = (%t, %v)", worked, err)
	}
	if err := db.Get(&status, `SELECT status FROM notification_deliveries WHERE organization_id=$1`, f.orgID); err != nil {
		t.Fatal(err)
	}
	if status != "delivered" || calls.Load() != 2 {
		t.Fatalf("final status=%s calls=%d, want delivered/2", status, calls.Load())
	}
}

func TestNotifierClassifiesPermanentAndExhaustedFailures(t *testing.T) {
	db := openNotifierTestDB(t)
	t.Run("permanent", func(t *testing.T) {
		f := newNotifierFixture(t, db, json.RawMessage(`{"url":"https://example.invalid"}`), false)
		notifier := NewNotifier(db)
		notifier.Send = func(ctx context.Context, _ string, _ json.RawMessage, message notify.Message) error {
			return notify.SendOne(ctx, "unsupported", json.RawMessage(`{}`), message)
		}
		enqueueNotification(t, notifier, jobLifecycleEvent(f.jobID, "JOB_COMPLETED", 1))
		if worked, err := notifier.ProcessNext(context.Background()); err != nil || !worked {
			t.Fatalf("ProcessNext() = (%t, %v)", worked, err)
		}
		var status, code string
		if err := db.QueryRowx(`SELECT status,failure_code FROM notification_deliveries WHERE organization_id=$1`, f.orgID).Scan(&status, &code); err != nil {
			t.Fatal(err)
		}
		if status != "failed" || code != string(notify.FailureUnknownBackend) {
			t.Fatalf("permanent state=(%s,%s)", status, code)
		}
	})

	t.Run("exhausted", func(t *testing.T) {
		f := newNotifierFixture(t, db, json.RawMessage(`{"url":"https://example.invalid"}`), false)
		notifier := NewNotifier(db)
		now := time.Date(2026, 7, 23, 9, 0, 0, 0, time.UTC)
		notifier.Now = func() time.Time { return now }
		notifier.Send = func(context.Context, string, json.RawMessage, notify.Message) error {
			return errors.New("temporary secret transport details")
		}
		enqueueNotification(t, notifier, jobLifecycleEvent(f.jobID, "JOB_COMPLETED", 1))
		if _, err := db.Exec(`UPDATE notification_deliveries SET max_attempts=2 WHERE organization_id=$1`, f.orgID); err != nil {
			t.Fatal(err)
		}
		if _, err := notifier.ProcessNext(context.Background()); err != nil {
			t.Fatal(err)
		}
		now = now.Add(5 * time.Second)
		if _, err := notifier.ProcessNext(context.Background()); err != nil {
			t.Fatal(err)
		}
		var status, code, reason string
		if err := db.QueryRowx(`SELECT status,failure_code,failure_reason FROM notification_deliveries WHERE organization_id=$1`, f.orgID).Scan(&status, &code, &reason); err != nil {
			t.Fatal(err)
		}
		if status != "failed" || code != "attempts_exhausted" || reason != "notification delivery attempts were exhausted" {
			t.Fatalf("exhausted state=(%s,%s,%s)", status, code, reason)
		}
	})
}

func TestNotifierClaimExcludesReplicaAndRecoversExpiredLease(t *testing.T) {
	db := openNotifierTestDB(t)
	f := newNotifierFixture(t, db, json.RawMessage(`{"url":"https://example.invalid"}`), false)
	now := time.Date(2026, 7, 23, 10, 0, 0, 0, time.UTC)
	worker1 := NewNotifier(db)
	worker1.WorkerID = "worker-one"
	worker1.Now = func() time.Time { return now }
	worker1.Lease = 45 * time.Second
	enqueueNotification(t, worker1, jobLifecycleEvent(f.jobID, "JOB_COMPLETED", 1))

	claimed, err := worker1.claim(context.Background())
	if err != nil || claimed == nil {
		t.Fatalf("worker one claim = (%v,%v)", claimed, err)
	}
	worker2 := NewNotifier(db)
	worker2.WorkerID = "worker-two"
	worker2.Now = func() time.Time { return now }
	var sends atomic.Int32
	worker2.Send = func(context.Context, string, json.RawMessage, notify.Message) error {
		sends.Add(1)
		return nil
	}
	if worked, err := worker2.ProcessNext(context.Background()); err != nil || worked {
		t.Fatalf("concurrent worker ProcessNext() = (%t,%v), want excluded", worked, err)
	}

	now = now.Add(46 * time.Second)
	if worked, err := worker2.ProcessNext(context.Background()); err != nil || !worked {
		t.Fatalf("recovery ProcessNext() = (%t,%v)", worked, err)
	}
	var status string
	var attemptCount, expiredCount int
	if err := db.QueryRowx(`
		SELECT d.status,d.attempt_count,
		       count(*) FILTER (WHERE a.failure_code='worker_lease_expired')
		FROM notification_deliveries d
		LEFT JOIN notification_delivery_attempts a ON a.delivery_id=d.id
		WHERE d.organization_id=$1 GROUP BY d.id`, f.orgID).
		Scan(&status, &attemptCount, &expiredCount); err != nil {
		t.Fatal(err)
	}
	if status != "delivered" || attemptCount != 2 || expiredCount != 1 || sends.Load() != 1 {
		t.Fatalf("recovery status=%s attempts=%d expired=%d sends=%d", status, attemptCount, expiredCount, sends.Load())
	}
}

func TestNotifierTargetDeletionBecomesTerminalWithoutSending(t *testing.T) {
	db := openNotifierTestDB(t)
	f := newNotifierFixture(t, db, json.RawMessage(`{"url":"https://example.invalid/secret"}`), false)
	notifier := NewNotifier(db)
	var sends atomic.Int32
	notifier.Send = func(context.Context, string, json.RawMessage, notify.Message) error {
		sends.Add(1)
		return nil
	}
	enqueueNotification(t, notifier, jobLifecycleEvent(f.jobID, "JOB_COMPLETED", 1))
	if _, err := db.Exec(`DELETE FROM notification_templates WHERE id=$1`, f.targetID); err != nil {
		t.Fatal(err)
	}
	if worked, err := notifier.ProcessNext(context.Background()); err != nil || !worked {
		t.Fatalf("ProcessNext() = (%t,%v)", worked, err)
	}
	var status, code, reason string
	if err := db.QueryRowx(`
		SELECT status,failure_code,failure_reason
		FROM notification_deliveries WHERE organization_id=$1`, f.orgID).
		Scan(&status, &code, &reason); err != nil {
		t.Fatal(err)
	}
	if status != "failed" || code != "target_unavailable" ||
		reason != "notification target is no longer available" || sends.Load() != 0 {
		t.Fatalf("target deletion state=(%s,%s,%s) sends=%d", status, code, reason, sends.Load())
	}
}
