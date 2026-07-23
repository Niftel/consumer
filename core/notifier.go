package core

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/praetordev/events"
	"github.com/praetordev/notify"
)

const (
	defaultNotificationPollInterval = time.Second
	defaultNotificationLease        = 45 * time.Second
	defaultNotificationSendTimeout  = 30 * time.Second
)

type notificationSender func(context.Context, string, json.RawMessage, notify.Message) error

// Notifier is the single durable delivery worker. Producers only enqueue
// bounded metadata in the event-projection transaction; this worker alone reads
// encrypted target configuration and performs network delivery.
type Notifier struct {
	DB           *sqlx.DB
	WorkerID     string
	PollInterval time.Duration
	Lease        time.Duration
	SendTimeout  time.Duration
	Now          func() time.Time
	Send         notificationSender
}

func NewNotifier(db *sqlx.DB) *Notifier {
	return &Notifier{
		DB:           db,
		WorkerID:     uuid.NewString(),
		PollInterval: defaultNotificationPollInterval,
		Lease:        defaultNotificationLease,
		SendTimeout:  defaultNotificationSendTimeout,
		Now:          time.Now,
		Send:         notify.SendOne,
	}
}

// notifyEvent maps a job event type to a notification lifecycle event and a
// human verb, or ("","") if the event type doesn't trigger notifications.
func notifyEvent(eventType string) (event, verb string) {
	switch eventType {
	case "JOB_STARTED":
		return "started", "started"
	case "JOB_COMPLETED":
		return "success", "succeeded"
	case "JOB_FAILED":
		return "error", "failed"
	}
	return "", ""
}

// Enqueue inserts one logical delivery for every matching policy. It must be
// called inside the event-projection transaction so accepted transitions and
// their notification work are committed atomically. The idempotency key is
// stable across event redelivery and process restart.
func (n *Notifier) Enqueue(ctx context.Context, tx *sqlx.Tx, evt events.JobEvent) error {
	if n == nil || tx == nil {
		return nil
	}
	event, _ := notifyEvent(evt.EventType)
	if event == "" {
		return nil
	}
	occurrenceID := fmt.Sprintf("%s:%d", evt.ExecutionRunID, evt.Seq)
	keyPrefix := "job-event:" + occurrenceID + ":policy:"
	_, err := tx.ExecContext(ctx, `
		INSERT INTO notification_deliveries (
			idempotency_key, organization_id, team_id,
			notification_policy_id, notification_template_id,
			target_name, target_type,
			resource_type, resource_id, event,
			occurrence_type, occurrence_id,
			subject_id, subject_name, subject_kind
		)
		SELECT $3 || np.id::text, np.organization_id, np.team_id,
		       np.id, nt.id, nt.name, nt.notification_type,
		       np.resource_type, np.resource_id, np.event,
		       'job_event', $4, uj.id,
		       LEFT(CASE WHEN np.resource_type = 'inventory_source'
		                 THEN org.name || ' / ' || src.name
		                 ELSE uj.name END, 255),
		       CASE WHEN np.resource_type = 'inventory_source'
		            THEN 'inventory sync' ELSE 'job' END
		  FROM unified_jobs uj
		  LEFT JOIN job_templates jt
		    ON jt.unified_job_template_id = uj.unified_job_template_id
		  LEFT JOIN inventory_sources src
		    ON src.id = CASE
		      WHEN jsonb_typeof(uj.job_args->'inventory_source_id') = 'number'
		      THEN (uj.job_args->>'inventory_source_id')::bigint END
		  LEFT JOIN inventories inv ON inv.id = src.inventory_id
		  LEFT JOIN organizations org ON org.id = inv.organization_id
		  JOIN notification_policies np
		    ON np.event = $2 AND np.team_id IS NULL
		   AND ((np.resource_type = 'job_template' AND np.resource_id = jt.id)
		     OR (np.resource_type = 'inventory_source' AND np.resource_id = src.id))
		  JOIN notification_templates nt ON nt.id = np.notification_template_id
		 WHERE uj.id = $1
		ON CONFLICT (idempotency_key) DO NOTHING`,
		evt.UnifiedJobID, event, keyPrefix, occurrenceID)
	if err != nil {
		return fmt.Errorf("enqueue notification deliveries: %w", err)
	}
	return nil
}

type deliveryWork struct {
	ID                 int64          `db:"id"`
	AttemptNumber      int16          `db:"attempt_number"`
	MaxAttempts        int16          `db:"max_attempts"`
	NotificationType   sql.NullString `db:"notification_type"`
	Config             []byte         `db:"config"`
	SubjectID          int64          `db:"subject_id"`
	SubjectName        string         `db:"subject_name"`
	SubjectKind        string         `db:"subject_kind"`
	Event              string         `db:"event"`
	TargetAvailable    bool           `db:"target_available"`
	RecoveredLease     bool           `db:"recovered_lease"`
	RecoveredAttempt   int16          `db:"recovered_attempt"`
	RecoveredStartedAt sql.NullTime   `db:"recovered_started_at"`
}

// claim locks one due delivery without blocking another worker replica. A
// crashed worker's expired lease is recorded as a transient attempt before the
// delivery is reclaimed.
func (n *Notifier) claim(ctx context.Context) (*deliveryWork, error) {
	now := n.Now().UTC()
	tx, err := n.DB.BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var work deliveryWork
	var priorStatus string
	if err := tx.QueryRowxContext(ctx, `
		SELECT d.id, d.attempt_count + 1 AS attempt_number, d.max_attempts,
		       nt.notification_type, nt.config,
		       d.subject_id, d.subject_name, d.subject_kind, d.event,
		       (nt.id IS NOT NULL) AS target_available,
		       (d.status = 'sending') AS recovered_lease,
		       d.attempt_count AS recovered_attempt,
		       d.last_attempt_at AS recovered_started_at,
		       d.status
		  FROM notification_deliveries d
		  LEFT JOIN notification_templates nt ON nt.id = d.notification_template_id
		 WHERE (
		       d.status IN ('pending', 'retrying') AND d.next_attempt_at <= $1
		   ) OR (
		       d.status = 'sending' AND d.lease_expires_at <= $1
		   )
		 ORDER BY CASE WHEN d.status = 'sending' THEN d.lease_expires_at ELSE d.next_attempt_at END, d.id
		 FOR UPDATE OF d SKIP LOCKED
		 LIMIT 1`, now).Scan(
		&work.ID, &work.AttemptNumber, &work.MaxAttempts,
		&work.NotificationType, &work.Config,
		&work.SubjectID, &work.SubjectName, &work.SubjectKind, &work.Event,
		&work.TargetAvailable, &work.RecoveredLease, &work.RecoveredAttempt,
		&work.RecoveredStartedAt, &priorStatus,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	if priorStatus == "sending" {
		started := now
		if work.RecoveredStartedAt.Valid {
			started = work.RecoveredStartedAt.Time
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO notification_delivery_attempts (
				delivery_id, attempt_number, outcome,
				failure_code, failure_reason, started_at, finished_at
			) VALUES ($1,$2,'transient_failure','worker_lease_expired',
			          'notification worker stopped before recording an outcome',$3,$4)
			ON CONFLICT (delivery_id, attempt_number) DO NOTHING`,
			work.ID, work.RecoveredAttempt, started, now); err != nil {
			return nil, err
		}
		if work.RecoveredAttempt >= work.MaxAttempts {
			if _, err := tx.ExecContext(ctx, `
				UPDATE notification_deliveries
				   SET status='failed', failed_at=$2,
				       failure_code='attempts_exhausted',
				       failure_reason='notification delivery attempts were exhausted',
				       lease_owner=NULL, lease_expires_at=NULL, updated_at=$2
				 WHERE id=$1`, work.ID, now); err != nil {
				return nil, err
			}
			if err := tx.Commit(); err != nil {
				return nil, err
			}
			return nil, nil
		}
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE notification_deliveries
		   SET status='sending', attempt_count=attempt_count+1,
		       first_attempt_at=COALESCE(first_attempt_at,$2),
		       last_attempt_at=$2, lease_owner=$3, lease_expires_at=$4,
		       failure_code=NULL, failure_reason=NULL, updated_at=$2
		 WHERE id=$1`,
		work.ID, now, n.WorkerID, now.Add(n.Lease)); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &work, nil
}

func retryDelay(attempt int16) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := 5 * time.Second * time.Duration(1<<uint(attempt-1))
	if delay > 5*time.Minute {
		return 5 * time.Minute
	}
	return delay
}

type deliveryOutcome struct {
	outcome       string
	failureCode   string
	failureReason string
	retryable     bool
}

func classifyDeliveryOutcome(err error) deliveryOutcome {
	if err == nil {
		return deliveryOutcome{outcome: "delivered"}
	}
	if failure, ok := notify.Failure(err); ok {
		return deliveryOutcome{
			outcome:       map[bool]string{true: "transient_failure", false: "permanent_failure"}[failure.Retryable],
			failureCode:   string(failure.Code),
			failureReason: failure.Error(),
			retryable:     failure.Retryable,
		}
	}
	return deliveryOutcome{
		outcome:       "transient_failure",
		failureCode:   "unclassified_failure",
		failureReason: "notification delivery failed",
		retryable:     true,
	}
}

func (n *Notifier) finish(ctx context.Context, work *deliveryWork, outcome deliveryOutcome) error {
	now := n.Now().UTC()
	tx, err := n.DB.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var ownedID int64
	if err := tx.GetContext(ctx, &ownedID, `
		SELECT id FROM notification_deliveries
		 WHERE id=$1 AND status='sending' AND lease_owner=$2
		 FOR UPDATE`, work.ID, n.WorkerID); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("notification delivery %d lease is no longer owned", work.ID)
		}
		return err
	}

	var failureCode, failureReason interface{}
	if outcome.failureCode != "" {
		failureCode = outcome.failureCode
		failureReason = outcome.failureReason
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO notification_delivery_attempts (
			delivery_id, attempt_number, outcome,
			failure_code, failure_reason, started_at, finished_at
		) VALUES ($1,$2,$3,$4,$5,
		          (SELECT last_attempt_at FROM notification_deliveries WHERE id=$1),$6)`,
		work.ID, work.AttemptNumber, outcome.outcome,
		failureCode, failureReason, now); err != nil {
		return err
	}

	switch {
	case outcome.outcome == "delivered":
		_, err = tx.ExecContext(ctx, `
			UPDATE notification_deliveries
			   SET status='delivered', delivered_at=$2, failed_at=NULL,
			       failure_code=NULL, failure_reason=NULL,
			       lease_owner=NULL, lease_expires_at=NULL, updated_at=$2
			 WHERE id=$1 AND lease_owner=$3`, work.ID, now, n.WorkerID)
	case outcome.retryable && work.AttemptNumber < work.MaxAttempts:
		_, err = tx.ExecContext(ctx, `
			UPDATE notification_deliveries
			   SET status='retrying', next_attempt_at=$2,
			       failure_code=$3, failure_reason=$4,
			       lease_owner=NULL, lease_expires_at=NULL, updated_at=$5
			 WHERE id=$1 AND lease_owner=$6`,
			work.ID, now.Add(retryDelay(work.AttemptNumber)),
			outcome.failureCode, outcome.failureReason, now, n.WorkerID)
	default:
		code := outcome.failureCode
		reason := outcome.failureReason
		if outcome.retryable {
			code = "attempts_exhausted"
			reason = "notification delivery attempts were exhausted"
		}
		_, err = tx.ExecContext(ctx, `
			UPDATE notification_deliveries
			   SET status='failed', failed_at=$2, delivered_at=NULL,
			       failure_code=$3, failure_reason=$4,
			       lease_owner=NULL, lease_expires_at=NULL, updated_at=$2
			 WHERE id=$1 AND lease_owner=$5`,
			work.ID, now, code, reason, n.WorkerID)
	}
	if err != nil {
		return err
	}
	return tx.Commit()
}

// ProcessNext claims and processes at most one due delivery. It is public so
// deterministic integration tests and operational probes can drain work
// without running a background polling loop.
func (n *Notifier) ProcessNext(ctx context.Context) (bool, error) {
	if n == nil || n.DB == nil {
		return false, nil
	}
	work, err := n.claim(ctx)
	if err != nil || work == nil {
		return false, err
	}

	var outcome deliveryOutcome
	if !work.TargetAvailable || !work.NotificationType.Valid {
		outcome = deliveryOutcome{
			outcome:       "permanent_failure",
			failureCode:   "target_unavailable",
			failureReason: "notification target is no longer available",
		}
	} else {
		sendCtx, cancel := context.WithTimeout(ctx, n.SendTimeout)
		sendErr := n.Send(sendCtx, work.NotificationType.String, json.RawMessage(work.Config), notify.Message{
			JobID: work.SubjectID, JobName: work.SubjectName,
			Event: work.Event, Status: notificationVerb(work.Event), Kind: notificationMessageKind(work.SubjectKind),
		})
		cancel()
		outcome = classifyDeliveryOutcome(sendErr)
	}
	if err := n.finish(ctx, work, outcome); err != nil {
		return true, err
	}
	return true, nil
}

func notificationVerb(event string) string {
	switch event {
	case "success":
		return "succeeded"
	case "error":
		return "failed"
	case "approval":
		return "needs approval"
	case "approved":
		return "approved"
	case "denied":
		return "denied"
	case "timeout":
		return "timed out"
	case "started":
		return "started"
	default:
		return event
	}
}

func notificationMessageKind(subjectKind string) string {
	if subjectKind == "job" {
		return ""
	}
	return subjectKind
}

// Run drains due work and waits for new work until ctx is cancelled.
func (n *Notifier) Run(ctx context.Context) error {
	for {
		worked, err := n.ProcessNext(ctx)
		if err != nil {
			logger.Error("notification delivery worker failed", "err", err)
		}
		if worked {
			continue
		}
		timer := time.NewTimer(n.PollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
