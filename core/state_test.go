package core

import "testing"

func TestNextRunState(t *testing.T) {
	tests := []struct {
		name    string
		current string
		event   string
		want    string
		changed bool
	}{
		{"start pending run", "pending", "JOB_STARTED", "running", true},
		{"duplicate start", "running", "JOB_STARTED", "running", false},
		{"task event is inert", "running", "TASK_OK", "running", false},
		{"complete running run", "running", "JOB_COMPLETED", "successful", true},
		{"fail running run", "running", "JOB_FAILED", "failed", true},
		{"cancel running run", "running", "JOB_CANCELED", "canceled", true},
		{"late start cannot regress success", "successful", "JOB_STARTED", "successful", false},
		{"conflicting failure cannot replace success", "successful", "JOB_FAILED", "successful", false},
		{"conflicting success cannot replace failure", "failed", "JOB_COMPLETED", "failed", false},
		{"late completion cannot replace cancellation", "canceled", "JOB_COMPLETED", "canceled", false},
		{"executor truth recovers lost run", "lost", "JOB_COMPLETED", "successful", true},
		{"executor truth recovers reconciling run", "reconciling", "JOB_FAILED", "failed", true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, changed := NextRunState(test.current, test.event)
			if got != test.want || changed != test.changed {
				t.Fatalf("NextRunState(%q, %q) = (%q, %v), want (%q, %v)",
					test.current, test.event, got, changed, test.want, test.changed)
			}
		})
	}
}

func TestTransitionForEvent(t *testing.T) {
	for event, want := range map[string]LifecycleTransition{
		"JOB_STARTED":   {RunState: "running", JobState: "running"},
		"JOB_COMPLETED": {RunState: "successful", JobState: "successful", Terminal: true},
		"JOB_FAILED":    {RunState: "failed", JobState: "failed", Terminal: true},
		"JOB_CANCELED":  {RunState: "canceled", JobState: "canceled", Terminal: true},
	} {
		got, ok := TransitionForEvent(event)
		if !ok || got != want {
			t.Errorf("TransitionForEvent(%q) = (%+v, %v), want %+v", event, got, ok, want)
		}
	}
	if _, ok := TransitionForEvent("CHECKPOINT_SAVED"); ok {
		t.Fatal("narration event must not have a state transition")
	}
}
