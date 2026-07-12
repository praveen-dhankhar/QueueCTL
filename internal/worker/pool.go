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

// registerWorkerFn is Store.RegisterWorker, indirected through a package-level
// var so tests can fail a registration partway through Pool.Start's launch
// loop - the one path where Start has to clean up workers it already started.
var registerWorkerFn = (*storage.Store).RegisterWorker

// Pool runs count worker goroutines against store, each independently
// claiming and executing jobs, alongside heartbeat, lease-renewal, and
// reaper background loops.
type Pool struct {
	store  *storage.Store
	count  int
	pidDir string
	logger *slog.Logger
}

// NewPool constructs a Pool. pidDir is the directory this supervisor
// process registers its own PID file into (see RegisterSupervisor); any
// number of Pools/processes may point at the same pidDir concurrently,
// including ones started from separate terminals. If logger is nil, a
// default stderr text logger is used.
func NewPool(store *storage.Store, count int, pidDir string, logger *slog.Logger) *Pool {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	return &Pool{
		store:  store,
		count:  count,
		pidDir: pidDir,
		logger: logger,
	}
}

// Start registers this process's worker PID file, runs the startup reaper
// pass, launches the reaper loop and count worker goroutines, then blocks
// until ctx is canceled. On cancellation it waits for all in-flight jobs to
// finish (workers stop claiming new jobs but do not abandon a running one)
// before stopping the reaper, cleaning up worker rows, and removing its own
// PID file. Multiple Pools (in separate processes) may run this
// concurrently against the same pidDir/database; each only ever manages its
// own PID file and its own worker goroutines.
func (p *Pool) Start(ctx context.Context) error {
	if p.count < 1 {
		return errors.New("worker count must be >= 1")
	}
	pidFile, err := RegisterSupervisor(p.pidDir)
	if err != nil {
		return err
	}
	defer os.Remove(pidFile)

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

	// Workers run under a context this function can cancel on its own, not
	// under ctx directly, so that a failure partway through the launch loop can
	// stop the workers already running. Cancelling ctx still cancels this one.
	workerCtx, stopWorkers := context.WithCancel(ctx)
	defer stopWorkers()

	var wg sync.WaitGroup
	for i := 1; i <= p.count; i++ {
		workerID := fmt.Sprintf("%s:%d:%d", hostname, os.Getpid(), i)
		if err := registerWorkerFn(p.store, context.Background(), workerID, os.Getpid(), hostname); err != nil {
			// Returning here without cleaning up would leave every worker
			// launched by an earlier iteration running: still claiming jobs,
			// still executing them, still holding locks - outliving the Start
			// call that owns them and answering to nobody. That is survivable
			// today only because main exits immediately on this error, which
			// makes it a bug waiting for the first caller that doesn't.
			stopWorkers()
			wg.Wait()
			return err
		}
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			p.runWorker(workerCtx, id)
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

	// Losing the lock means the reaper has already requeued this job for
	// someone else to run. The command this worker started must die, or the
	// job's side effects happen twice: once here, once in whoever claims the
	// requeued row. Canceling execCtx kills its process group (see
	// ExecuteCommand). The reaper also kills the group via the persisted
	// locked_pgid, but it can only do that if SetJobLockPGID actually landed -
	// and that call is fenced on the very lock we just lost, so on this path it
	// may well have failed. Killing from the worker side does not depend on it.
	execCtx, cancelExec := context.WithCancel(context.Background())
	defer cancelExec()

	// A job's own timeout hangs off that same context, so expiry and lost-lock
	// cancellation both reach the command through one cmd.Cancel - meaning a
	// timeout kills the whole process group, not just the "sh" leader. Note
	// this deliberately bounds the command, not executeJob as a whole: the
	// store calls that record the outcome afterwards must still run.
	if claimed.TimeoutSeconds > 0 {
		var stopTimeout context.CancelFunc
		execCtx, stopTimeout = context.WithTimeout(execCtx, time.Duration(claimed.TimeoutSeconds)*time.Second)
		defer stopTimeout()
	}

	go p.renewJobLease(leaseCtx, workerID, claimed.ID, lease, leaseDone, cancelExec)
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
	result := executeCommandFn(execCtx, claimed.Command, onStart)

	// A timed-out command was SIGKILLed, so it reports a non-zero (signaled)
	// exit like any other failure and flows down the normal retry path below -
	// charged an attempt, backed off, dead once out of retries. The exit code
	// alone doesn't say why it died, though, so record the reason where
	// "queuectl logs" will show it. The ExitCode check keeps a command that
	// happened to finish successfully in the same instant the deadline expired
	// from being labeled a timeout it didn't actually suffer.
	if result.ExitCode != 0 && errors.Is(execCtx.Err(), context.DeadlineExceeded) {
		p.logger.Warn("job timed out; killed the command's process group",
			"worker_id", workerID, "job_id", claimed.ID, "timeout_seconds", claimed.TimeoutSeconds)
		result.Stderr = appendTimeoutNote(result.Stderr, claimed.TimeoutSeconds)
	}

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
	nextState := job.NextAttemptState(attempts, claimed.MaxRetries)
	var nextRetryAt *time.Time
	if nextState == job.StateFailed {
		retryAt := time.Now().UTC().Add(BackoffDelay(backoffBase, attempts))
		nextRetryAt = &retryAt
	}
	if err := p.store.RecordJobFailure(context.Background(), claimed, run, nextState, nextRetryAt); err != nil {
		p.logRecordError("record job failure failed", workerID, claimed.ID, err)
	}
}

// appendTimeoutNote records why a timed-out attempt died, on the end of
// whatever output the command had managed to produce first. It is appended to
// the already-rendered stderr text rather than written into the capped buffer
// for the same reason ExecuteCommand appends its start-failure text there: a
// command chatty enough to fill the buffer must not be able to truncate away
// the explanation of its own death.
func appendTimeoutNote(stderr string, timeoutSeconds int) string {
	note := fmt.Sprintf("queuectl: attempt timed out after %ds and its process group was killed", timeoutSeconds)
	if stderr == "" {
		return note
	}
	return stderr + "\n" + note
}

func (p *Pool) logRecordError(message string, workerID string, jobID string, err error) {
	if errors.Is(err, storage.ErrJobLockLost) {
		p.logger.Warn(message, "worker_id", workerID, "job_id", jobID, "error", err)
		return
	}
	p.logger.Error(message, "worker_id", workerID, "job_id", jobID, "error", err)
}

// renewJobLease extends the job's lock until ctx is canceled. If the lock is
// lost - the reaper reclaimed the job and requeued it for another worker - it
// calls cancelExec to kill the command still running under this worker, since
// that command's job now belongs to somebody else and letting it run to
// completion would execute the job twice.
func (p *Pool) renewJobLease(ctx context.Context, workerID string, jobID string, lease *jobLease, done chan<- struct{}, cancelExec context.CancelFunc) {
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
				p.logger.Warn("job lock lost during execution; killing the command so the requeued job is not run twice",
					"worker_id", workerID, "job_id", jobID)
				cancelExec()
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
