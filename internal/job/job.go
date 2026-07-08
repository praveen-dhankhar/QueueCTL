package job

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// State is a job's lifecycle state. Valid transitions are enforced both
// here (via the Mark*/RetryFromDLQ methods) and in SQL via a CHECK
// constraint on the jobs.state column:
//
//	pending  -> processing (claim)
//	failed   -> processing (claim, after next_retry_at elapses)
//	processing -> completed
//	processing -> failed (retries remain) or dead (retries exhausted)
//	dead     -> pending (manual DLQ retry)
type State string

const (
	StatePending    State = "pending"
	StateProcessing State = "processing"
	StateCompleted  State = "completed"
	StateFailed     State = "failed"
	StateDead       State = "dead"
)

var validStates = map[State]struct{}{
	StatePending:    {},
	StateProcessing: {},
	StateCompleted:  {},
	StateFailed:     {},
	StateDead:       {},
}

// Job is one unit of work: a shell command tracked through the state
// machine above, with retry bookkeeping (Attempts/MaxRetries/NextRetryAt)
// and lock ownership (LockedBy/LockedAt/LockedPGID) used to fence
// claim/complete/fail updates against concurrent workers and the reaper.
type Job struct {
	ID          string
	Command     string
	State       State
	Attempts    int
	MaxRetries  int
	NextRetryAt *time.Time
	LockedBy    *string
	LockedAt    *time.Time
	// LockedPGID is the OS process-group ID leading the job's running "sh
	// -c" command, recorded once the worker starts it. It lets the reaper
	// kill the whole group (not just record the job as recovered) when it
	// reclaims a stale lock, even if the reaper is running in a different
	// queuectl process than the one that started the command (e.g. after a
	// crashed supervisor was restarted).
	LockedPGID *int
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// ValidateState reports an error if state is not one of the five known
// job states.
func ValidateState(state State) error {
	if _, ok := validStates[state]; !ok {
		return fmt.Errorf("invalid job state %q", state)
	}
	return nil
}

// ParseState converts a raw string (as stored in SQLite) into a State,
// validating it against the known set.
func ParseState(raw string) (State, error) {
	state := State(raw)
	if err := ValidateState(state); err != nil {
		return "", err
	}
	return state, nil
}

// AllStates returns every valid job state, in the order the CLI displays
// them (status, list --state).
func AllStates() []State {
	return []State{StatePending, StateProcessing, StateCompleted, StateFailed, StateDead}
}

// New constructs a pending Job. It returns an error if command is blank or
// maxRetries is less than 1.
func New(id string, command string, maxRetries int, now time.Time) (Job, error) {
	if strings.TrimSpace(command) == "" {
		return Job{}, errors.New("command is required")
	}
	if maxRetries < 1 {
		return Job{}, errors.New("max_retries must be >= 1")
	}
	return Job{
		ID:         id,
		Command:    command,
		State:      StatePending,
		Attempts:   0,
		MaxRetries: maxRetries,
		CreatedAt:  now.UTC(),
		UpdatedAt:  now.UTC(),
	}, nil
}

// MarkProcessing transitions a pending or failed job to processing and
// records the claiming worker's lock. It errors on any other starting
// state. This mirrors the SQL claim in Store.ClaimNextJob; it is not
// itself used on the claim path (which claims atomically in SQL) but keeps
// the Go-side state machine self-consistent for tests and direct callers.
func (j *Job) MarkProcessing(workerID string, now time.Time) error {
	if j.State != StatePending && j.State != StateFailed {
		return fmt.Errorf("cannot claim job in state %q", j.State)
	}
	j.State = StateProcessing
	j.LockedBy = &workerID
	lockedAt := now.UTC()
	j.LockedAt = &lockedAt
	j.LockedPGID = nil
	j.UpdatedAt = lockedAt
	return nil
}

// MarkCompleted transitions a processing job to completed, clearing its
// lock and retry state. It errors if the job is not currently processing.
func (j *Job) MarkCompleted(now time.Time) error {
	if j.State != StateProcessing {
		return fmt.Errorf("cannot complete job in state %q", j.State)
	}
	j.Attempts++
	j.State = StateCompleted
	j.NextRetryAt = nil
	j.LockedBy = nil
	j.LockedAt = nil
	j.LockedPGID = nil
	j.UpdatedAt = now.UTC()
	return nil
}

// MarkFailedOrDead transitions a processing job to failed (with
// nextRetryAt set, if attempts remain below MaxRetries) or dead (once
// exhausted), clearing its lock either way. It errors if the job is not
// currently processing.
func (j *Job) MarkFailedOrDead(nextRetryAt *time.Time, now time.Time) error {
	if j.State != StateProcessing {
		return fmt.Errorf("cannot fail job in state %q", j.State)
	}
	j.Attempts++
	if j.Attempts < j.MaxRetries {
		j.State = StateFailed
		j.NextRetryAt = nextRetryAt
	} else {
		j.State = StateDead
		j.NextRetryAt = nil
	}
	j.LockedBy = nil
	j.LockedAt = nil
	j.LockedPGID = nil
	j.UpdatedAt = now.UTC()
	return nil
}

// RetryFromDLQ transitions a dead job back to pending, resetting attempts
// and clearing lock/retry state. It errors if the job is not currently
// dead.
func (j *Job) RetryFromDLQ(now time.Time) error {
	if j.State != StateDead {
		return fmt.Errorf("cannot retry job in state %q", j.State)
	}
	j.State = StatePending
	j.Attempts = 0
	j.NextRetryAt = nil
	j.LockedBy = nil
	j.LockedAt = nil
	j.LockedPGID = nil
	j.UpdatedAt = now.UTC()
	return nil
}
