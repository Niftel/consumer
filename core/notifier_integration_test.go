package core

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/praetordev/events"
)

func TestNotifierRoutesJobAndInventorySourcePolicies(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping notifier integration test")
	}
	db, err := sqlx.Connect("postgres", dbURL)
	if err != nil {
		t.Skipf("cannot reach TEST_DATABASE_URL: %v", err)
	}
	defer db.Close()

	got := make(chan map[string]interface{}, 4)
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var message map[string]interface{}
		_ = json.Unmarshal(body, &message)
		got <- message
		w.WriteHeader(http.StatusNoContent)
	}))
	defer receiver.Close()
	receiverURL, err := url.Parse(receiver.URL)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("PRAETOR_NOTIFICATION_ALLOWED_HOSTS", receiverURL.Hostname())

	uniq := time.Now().UnixNano()
	var orgID, targetID, inventoryID, sourceID, unifiedTemplateID, templateID int64
	if err := db.Get(&orgID, `INSERT INTO organizations (name) VALUES ($1) RETURNING id`, fmt.Sprintf("consumer-notify-org-%d", uniq)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM organizations WHERE id=$1`, orgID) })
	config, _ := json.Marshal(map[string]string{"url": receiver.URL})
	if err := db.Get(&targetID, `INSERT INTO notification_templates (organization_id,name,notification_type,config) VALUES ($1,$2,'webhook',$3) RETURNING id`, orgID, fmt.Sprintf("consumer-notify-target-%d", uniq), config); err != nil {
		t.Fatal(err)
	}
	if err := db.Get(&inventoryID, `INSERT INTO inventories (organization_id,name) VALUES ($1,$2) RETURNING id`, orgID, fmt.Sprintf("consumer-notify-inventory-%d", uniq)); err != nil {
		t.Fatal(err)
	}
	if err := db.Get(&sourceID, `INSERT INTO inventory_sources (inventory_id,name) VALUES ($1,$2) RETURNING id`, inventoryID, fmt.Sprintf("consumer-notify-source-%d", uniq)); err != nil {
		t.Fatal(err)
	}
	if err := db.Get(&unifiedTemplateID, `INSERT INTO unified_job_templates (name) VALUES ($1) RETURNING id`, fmt.Sprintf("consumer-notify-ujt-%d", uniq)); err != nil {
		t.Fatal(err)
	}
	if err := db.Get(&templateID, `INSERT INTO job_templates (organization_id,name,playbook,unified_job_template_id) VALUES ($1,$2,'site.yml',$3) RETURNING id`, orgID, fmt.Sprintf("consumer-notify-template-%d", uniq), unifiedTemplateID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO notification_policies (organization_id,notification_template_id,resource_type,resource_id,event) VALUES
		($1,$2,'job_template',$3,'success'),($1,$2,'inventory_source',$4,'error')`, orgID, targetID, templateID, sourceID); err != nil {
		t.Fatal(err)
	}

	var jobID, syncJobID int64
	if err := db.Get(&jobID, `INSERT INTO unified_jobs (unified_job_template_id,name,status) VALUES ($1,'Deploy application','successful') RETURNING id`, unifiedTemplateID); err != nil {
		t.Fatal(err)
	}
	if err := db.Get(&syncJobID, `INSERT INTO unified_jobs (name,status,job_args) VALUES ('Inventory synchronization','failed',jsonb_build_object('inventory_source_id',$1::bigint)) RETURNING id`, sourceID); err != nil {
		t.Fatal(err)
	}

	notifier := NewNotifier(db)
	notifier.Dispatch(events.JobEvent{UnifiedJobID: jobID, EventType: "JOB_COMPLETED"})
	notifier.Dispatch(events.JobEvent{UnifiedJobID: syncJobID, EventType: "JOB_FAILED"})

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
	wantSourceName := fmt.Sprintf("consumer-notify-org-%d / consumer-notify-source-%d", uniq, uniq)
	if messages["inventory sync"]["event"] != "error" || messages["inventory sync"]["job_name"] != wantSourceName {
		t.Fatalf("inventory notification = %#v, want source name %q", messages["inventory sync"], wantSourceName)
	}
	encoded, _ := json.Marshal(messages["inventory sync"])
	if string(encoded) == "" || strings.Contains(string(encoded), "source_kind") || strings.Contains(string(encoded), "credential") || strings.Contains(string(encoded), "job_args") {
		t.Fatalf("inventory notification exposed source configuration: %s", encoded)
	}
}
