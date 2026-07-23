package core

import (
	"errors"
	"testing"
	"time"

	"github.com/praetordev/notify"
)

func TestRetryDelayIsBoundedExponential(t *testing.T) {
	tests := []struct {
		attempt int16
		want    time.Duration
	}{
		{attempt: 1, want: 5 * time.Second},
		{attempt: 2, want: 10 * time.Second},
		{attempt: 3, want: 20 * time.Second},
		{attempt: 7, want: 5 * time.Minute},
		{attempt: 10, want: 5 * time.Minute},
	}
	for _, test := range tests {
		if got := retryDelay(test.attempt); got != test.want {
			t.Errorf("retryDelay(%d)=%s, want %s", test.attempt, got, test.want)
		}
	}
}

func TestNotificationMessageKindPreservesJobWireShape(t *testing.T) {
	if got := notificationMessageKind("job"); got != "" {
		t.Fatalf("job message kind = %q, want empty", got)
	}
	if got := notificationMessageKind("inventory sync"); got != "inventory sync" {
		t.Fatalf("inventory message kind = %q", got)
	}
}

func TestClassifyDeliveryOutcomeRedactsUntypedErrors(t *testing.T) {
	outcome := classifyDeliveryOutcome(errors.New("https://secret.example/path credential=secret"))
	if !outcome.retryable || outcome.failureCode != "unclassified_failure" ||
		outcome.failureReason != "notification delivery failed" {
		t.Fatalf("untyped outcome = %#v", outcome)
	}

	permanent := classifyDeliveryOutcome(notify.SendOne(nil, "missing-secret-name", nil, notify.Message{}))
	if permanent.retryable || permanent.failureCode != string(notify.FailureUnknownBackend) ||
		permanent.failureReason == "" {
		t.Fatalf("typed outcome = %#v", permanent)
	}
}
