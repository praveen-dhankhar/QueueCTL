package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"runtime/debug"
	"sync"
	"time"

	appconfig "queuectl/internal/config"
	"queuectl/internal/job"
	"queuectl/internal/storage"
)

const (
	// claimErrorSleep is how long a worker backs off after a failed claim
	// or config read, so a transient DB error doesn't spin the poll loop.
	// It intentionally does not use poll-interval-ms: that config controls
	// the steady-state "no job available" cadence, whereas this is a
	// fallback for when reading that same config value has itself failed.
	claimErrorSleep = 500 * time.Millisecond
)

// executeCommandFn is ExecuteCommand, indirected through a package-level var
// so tests can inject a panic to exercise executeJob's panic-recovery path
// without actually needing a buggy command or store call to trigger one.
var executeCommandFn = ExecuteCommand

// Pool runs count worker goroutines against store, each independently
// claiming and executing jobs, alongside heartbeat, lease-renewal, and
// reaper background loops.
type Pool struct {
	store   *storage.Store
	count   int
	pidPath string
	logger  *slog.Logger
}

// NewPool constructs a Pool. If logger is nil, a default stderr text
// logger is used.
func NewPool(store *storage.Store, count int, pidPath string, logger *slog.Logger) *Pool {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	return &Pool{
		store:   store,
		count:   count,
		pidPath: pidPath,
		logger:  logger,
	}
}

// Start writes the worker PID file, runs the startup reaper pass, launches
// the reaper loop and count worker goroutines, then blocks until ctx is
// canceled. On cancellation it waits for all in-flight jobs to finish
// (workers stop claiming new jobs but do not abandon a running one) before
// stopping the reaper, cleaning up worker rows, and removing the PID file.
func (p *Pool) Start(ctx context.Context) error {
	if p.count < 1 {
		return errors.New("worker count must be >= 1")
	}
	if err := appconfig.EnsureQueueDir(); err != nil {
		return fmt.Errorf("create .queuectl directory: %w", err)
	}
	if err := ClaimSupervisorPIDFile(p.pidPath); err != nil {
		return err
	}
	defer os.Remove(p.pidPath)

	if _, err := RunReaperOnce(ctx, p.store, p.logger); err != nil {
		return fmt.Errorf("startup reaper: %w", err)
	}

	reaperCtx, stopReaper := context.WithCancel(context.Background())
	defer stopReaper()
	go RunReaperLoop(reaperCtx, p.store, p.logger)

	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("get hostname: %w", err)
	}

	var wg sync.WaitGroup
	for i := 1; i <= p.count; i++ {
		workerID := fmt.Sprintf("%s:%d:%d", hostname, os.Getpid(), i)
		if err := p.store.RegisterWorker(context.Background(), workerID, os.Getpid(), hostname); err != nil {
			return err
		}
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			p.runWorker(ctx, id)
		}(workerID)
	}

	<-ctx.Done()
	p.logger.Info("shutdown signal received; workers will finish in-flight jobs")
	wg.Wait()
	stopReaper()
	return nil
}

func (p *Pool) runWorker(ctx context.Context, workerID string) {
	defer func() {
		if err := p.store.DeleteWorker(context.Background(), workerID); err != nil {
			p.logger.Error("delete worker failed", "worker_id", workerID, "error", err)
		}
	}()

	heartbeatCtx, stopHeartbeat := context.WithCancel(context.Background())
	defer stopHeartbeat()
	go p.heartbeatLoop(heartbeatCtx, workerID)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		claimed, ok, err := p.store.ClaimNextJob(context.Background(), workerID)
		if err != nil {
			p.logger.Error("claim job failed", "worker_id", workerID, "error", err)
			p.sleep(ctx, claimErrorSleep)
			continue
		}
		if !ok {
			pollInterval, err := p.pollInterval(ctx)
			if err != nil {
				p.logger.Error("read poll interval failed", "worker_id", workerID, "error", err)
				pollInterval = claimErrorSleep
			}
			p.sleep(ctx, pollInterval)
			continue
		}

		p.executeJob(workerID, claimed)
	}
}

// executeJob runs claimed.Command and records its outcome. It recovers from
// any panic raised while doing so (in ExecuteCommand or in the store calls
// below) so that a single bad job can't take down the whole worker pool: the
// panic is logged and the job is simply abandoned mid-claim, left for the
// reaper to reclaim once its lock goes stale, the same recovery path used
// for a worker that crashes outright. The lease-renewal goroutine is always
// stopped via a deferred call (in addition to the explicit stop on the
// normal path below, which needs the final lockedAt value before that
// point) so a panic can't leak it renewing a lock forever and starving the
// reaper of a stale row to recover.
func (p *Pool) executeJob(workerID string, claimed job.Job) {
	p.logger.Info("executing job", "worker_id", workerID, "job_id", claimed.ID)
	lease := newJobLease(claimed.LockedAt)
	leaseCtx, stopLease := context.WithCancel(context.Background())
	leaseDone := make(chan struct{})
	go p.renewJobLease(leaseCtx, workerID, claimed.ID, lease, leaseDone)
	defer stopLease()

	defer func() {
		if r := recover(); r != nil {
			p.logger.Error("panic while executing job; abandoning claim for the reaper to recover",
				"worker_id", workerID, "job_id", claimed.ID, "panic", r, "stack", string(debug.Stack()))
		}
	}()

	onStart := func(pid int) {
		if err := p.store.SetJobLockPGID(context.Background(), claimed.ID, workerID, pid); err != nil {
			p.logRecordError("record job process group failed", workerID, claimed.ID, err)
		}
	}
	result := executeCommandFn(context.Background(), claimed.Command, onStart)
	stopLease()
	<-leaseDone
	if lockedAt := lease.currentLockedAt(); lockedAt != nil {
		claimed.LockedAt = lockedAt
	}

	run := storage.JobRun{
		WorkerID:   workerID,
		ExitCode:   &result.ExitCode,
		Stdout:     result.Stdout,
		Stderr:     result.Stderr,
		StartedAt:  result.StartedAt,
		FinishedAt: result.FinishedAt,
	}

	if result.ExitCode == 0 {
		if err := p.store.RecordJobSuccess(context.Background(), claimed, run); err != nil {
			p.logRecordError("record job success failed", workerID, claimed.ID, err)
		}
		return
	}

	backoffBase, err := p.store.GetConfigInt(context.Background(), appconfig.KeyBackoffBase)
	if err != nil {
		p.logger.Error("read backoff base failed", "worker_id", workerID, "job_id", claimed.ID, "error", err)
		backoffBase = appconfig.Defaults[appconfig.KeyBackoffBase]
	}

	attempts := claimed.Attempts + 1
	nextState := job.StateDead
	var nextRetryAt *time.Time
	if attempts < claimed.MaxRetries {
		nextState = job.StateFailed
		retryAt := time.Now().UTC().Add(BackoffDelay(backoffBase, attempts))
		nextRetryAt = &retryAt
	}
	if err := p.store.RecordJobFailure(context.Background(), claimed, run, nextState, nextRetryAt); err != nil {
		p.logRecordError("record job failure failed", workerID, claimed.ID, err)
	}
}

func (p *Pool) logRecordError(message string, workerID string, jobID string, err error) {
	if errors.Is(err, storage.ErrJobLockLost) {
		p.logger.Warn(message, "worker_id", workerID, "job_id", jobID, "error", err)
		return
	}
	p.logger.Error(message, "worker_id", workerID, "job_id", jobID, "error", err)
}

func (p *Pool) renewJobLease(ctx context.Context, workerID string, jobID string, lease *jobLease, done chan<- struct{}) {
	defer close(done)

	ticker := time.NewTicker(p.lockRenewalInterval())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			lockedAt, ok, err := p.store.RenewJobLock(context.Background(), jobID, workerID)
			if err != nil {
				p.logger.Error("renew job lock failed", "worker_id", workerID, "job_id", jobID, "error", err)
				continue
			}
			if !ok {
				p.logger.Warn("job lock lost during execution", "worker_id", workerID, "job_id", jobID)
				return
			}
			lease.update(lockedAt)
		}
	}
}

func (p *Pool) lockRenewalInterval() time.Duration {
	lockTimeoutSeconds, err := p.store.GetConfigInt(context.Background(), appconfig.KeyLockTimeoutSeconds)
	if err != nil {
		p.logger.Error("read lock timeout failed", "error", err)
		return 5 * time.Second
	}
	interval := time.Duration(lockTimeoutSeconds) * time.Second / 3
	if interval < 250*time.Millisecond {
		return 250 * time.Millisecond
	}
	if interval > 5*time.Second {
		return 5 * time.Second
	}
	return interval
}

type jobLease struct {
	mu      sync.Mutex
	current *time.Time
}

func newJobLease(initial *time.Time) *jobLease {
	var lockedAt *time.Time
	if initial != nil {
		copy := *initial
		lockedAt = &copy
	}
	return &jobLease{current: lockedAt}
}

func (l *jobLease) update(lockedAt time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	copy := lockedAt.UTC()
	l.current = &copy
}

func (l *jobLease) currentLockedAt() *time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.current == nil {
		return nil
	}
	copy := *l.current
	return &copy
}

func (p *Pool) heartbeatLoop(ctx context.Context, workerID string) {
	ticker := time.NewTicker(appconfig.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := p.store.HeartbeatWorker(context.Background(), workerID); err != nil {
				p.logger.Error("worker heartbeat failed", "worker_id", workerID, "error", err)
			}
		}
	}
}

func (p *Pool) pollInterval(ctx context.Context) (time.Duration, error) {
	value, err := p.store.GetConfigInt(ctx, appconfig.KeyPollIntervalMS)
	if err != nil {
		return 0, err
	}
	return time.Duration(value) * time.Millisecond, nil
}

func (p *Pool) sleep(ctx context.Context, duration time.Duration) {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
