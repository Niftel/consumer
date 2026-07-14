package core

// LifecycleTransition is the state projection caused by a lifecycle event.
// Unknown and task/narration events have no transition.
type LifecycleTransition struct {
	RunState string
	JobState string
	Terminal bool
}

var lifecycleTransitions = map[string]LifecycleTransition{
	"JOB_STARTED":   {RunState: "running", JobState: "running"},
	"JOB_COMPLETED": {RunState: "successful", JobState: "successful", Terminal: true},
	"JOB_FAILED":    {RunState: "failed", JobState: "failed", Terminal: true},
	"JOB_CANCELED":  {RunState: "canceled", JobState: "canceled", Terminal: true},
}

// TransitionForEvent returns the state projection for a lifecycle event.
func TransitionForEvent(eventType string) (LifecycleTransition, bool) {
	transition, ok := lifecycleTransitions[eventType]
	return transition, ok
}

// RunStateIsTerminal mirrors the database run_is_terminal helper. Lost is
// deliberately provisional: executor truth may recover it to a real terminal
// outcome later.
func RunStateIsTerminal(state string) bool {
	switch state {
	case "successful", "failed", "canceled":
		return true
	default:
		return false
	}
}

// NextRunState applies the consumer's monotonic projection policy. The returned
// bool means a real transition was accepted, and therefore lifecycle side
// effects such as notifications and transition metrics may run.
func NextRunState(currentState, eventType string) (string, bool) {
	transition, ok := TransitionForEvent(eventType)
	if !ok || RunStateIsTerminal(currentState) || currentState == transition.RunState {
		return currentState, false
	}
	return transition.RunState, true
}
