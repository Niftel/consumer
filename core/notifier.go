package core

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/praetordev/events"
	"github.com/praetordev/notify"
)

// Notifier dispatches notifications when a job reaches a lifecycle event. It is
// driven off the event projection and fired only when a terminal event is newly
// projected, so each notification is sent exactly once despite at-least-once
// delivery. HTTP sends run in the background so projection latency is unaffected.
//
// Delivery is delegated to pkg/notify: the notification_type selects a
// self-registered Backend, so adding a backend needs no change here.
type Notifier struct {
	DB *sqlx.DB
}

func NewNotifier(db *sqlx.DB) *Notifier {
	return &Notifier{DB: db}
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

// Dispatch fires any notifications attached to the job's template for evt's
// lifecycle event. Safe to call inline after a commit (and on a nil receiver).
func (n *Notifier) Dispatch(evt events.JobEvent) {
	if n == nil || n.DB == nil {
		return
	}
	ev, verb := notifyEvent(evt.EventType)
	if ev == "" {
		return
	}
	go n.send(evt.UnifiedJobID, ev, verb)
}

func (n *Notifier) send(jobID int64, ev, verb string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	type row struct {
		Type         string          `db:"notification_type"`
		Config       json.RawMessage `db:"config"`
		JobName      string          `db:"name"`
		ResourceType string          `db:"resource_type"`
	}
	var rows []row
	if err := n.DB.SelectContext(ctx, &rows, `
		SELECT nt.notification_type, nt.config,
		       CASE WHEN np.resource_type = 'inventory_source'
		            THEN org.name || ' / ' || src.name
		            ELSE uj.name END AS name,
		       np.resource_type
		FROM unified_jobs uj
		LEFT JOIN job_templates jt ON jt.unified_job_template_id = uj.unified_job_template_id
		LEFT JOIN inventory_sources src ON src.id = CASE
		  WHEN jsonb_typeof(uj.job_args->'inventory_source_id') = 'number'
		  THEN (uj.job_args->>'inventory_source_id')::bigint END
		LEFT JOIN inventories inv ON inv.id = src.inventory_id
		LEFT JOIN organizations org ON org.id = inv.organization_id
		JOIN notification_policies np
		  ON np.event = $2 AND np.team_id IS NULL
		 AND ((np.resource_type = 'job_template' AND np.resource_id = jt.id)
		   OR (np.resource_type = 'inventory_source' AND np.resource_id = src.id))
		JOIN notification_templates nt ON nt.id = np.notification_template_id
		WHERE uj.id = $1`, jobID, ev); err != nil {
		logger.Error("notifier lookup failed", "job_id", jobID, "err", err)
		return
	}

	for _, r := range rows {
		kind := ""
		if r.ResourceType == "inventory_source" {
			kind = "inventory sync"
		}
		if err := notify.SendOne(ctx, r.Type, r.Config, notify.Message{
			JobID: jobID, JobName: r.JobName, Event: ev, Status: verb, Kind: kind,
		}); err != nil {
			logger.Error("notifier send failed", "type", r.Type, "job_id", jobID, "err", err)
			continue
		}
		logger.Info("notifier sent notification", "type", r.Type, "job_id", jobID, "event", ev)
	}
}
