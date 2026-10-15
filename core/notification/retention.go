package notification

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/usememos/memos/store"
)

const (
	// DefaultRetentionSweepInterval is how often the scheduler applies the
	// notification retention policy.
	DefaultRetentionSweepInterval = time.Hour
	// cleanupRunHistoryKeep bounds the audit table to the newest runs.
	cleanupRunHistoryKeep = 100
	// cleanupFinalizeTimeout bounds the detached audit write when a request is
	// canceled while a manual sweep is in progress.
	cleanupFinalizeTimeout = 10 * time.Second
	// cleanupErrorDetailLimit keeps failure reasons readable in the audit table.
	cleanupErrorDetailLimit = 1000
)

// ErrCleanupAlreadyRunning is returned by Sweep while another sweep on this
// instance is still in progress.
var ErrCleanupAlreadyRunning = errors.New("notification cleanup is already running")

// RetentionSweeper applies the instance notification retention policy and
// keeps the audit trail of every run. A single instance never runs two sweeps
// concurrently.
type RetentionSweeper struct {
	store    *store.Store
	now      func() time.Time
	interval time.Duration

	// afterWatermark runs after the ID watermark snapshot and before any
	// deletion. Production leaves it nil; tests interleave a concurrent
	// dispatch through it.
	afterWatermark func(watermark int32)

	runMu sync.Mutex

	scheduleMu sync.Mutex
	cancel     context.CancelFunc
	done       chan struct{}
	wg         sync.WaitGroup
}

// Option customizes a RetentionSweeper, mainly for tests.
type Option func(*RetentionSweeper)

// WithInterval overrides the scheduler interval.
func WithInterval(interval time.Duration) Option {
	return func(s *RetentionSweeper) {
		s.interval = interval
	}
}

// withClock overrides the time source.
func withClock(now func() time.Time) Option {
	return func(s *RetentionSweeper) {
		s.now = now
	}
}

// WithAfterWatermark installs a hook invoked after the ID watermark snapshot
// and before any deletion. It is an integration hook for tests that interleave
// a concurrent notification dispatch; production callers leave it unset.
func WithAfterWatermark(hook func(watermark int32)) Option {
	return func(s *RetentionSweeper) {
		s.afterWatermark = hook
	}
}

// NewRetentionSweeper creates a retention sweeper for the store.
func NewRetentionSweeper(st *store.Store, opts ...Option) *RetentionSweeper {
	s := &RetentionSweeper{
		store:    st,
		now:      time.Now,
		interval: DefaultRetentionSweepInterval,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Sweep applies the configured retention policy once. Only one sweep runs at a
// time per instance; concurrent callers get ErrCleanupAlreadyRunning. The
// returned run is always persisted (RUNNING followed by SUCCEEDED or FAILED)
// when recordRun is true.
func (s *RetentionSweeper) Sweep(ctx context.Context, trigger store.NotificationCleanupTrigger) (*store.NotificationCleanupRun, error) {
	if !s.runMu.TryLock() {
		return nil, ErrCleanupAlreadyRunning
	}
	defer s.runMu.Unlock()
	return s.sweep(ctx, trigger)
}

func (s *RetentionSweeper) sweep(ctx context.Context, trigger store.NotificationCleanupTrigger) (*store.NotificationCleanupRun, error) {
	setting, err := s.store.GetInstanceNotificationSetting(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get notification setting")
	}
	retention := setting.GetRetention()
	maxAgeDays := retention.GetMaxAgeDays()
	maxPerUser := retention.GetMaxPerUser()

	// A scheduled tick without any policy configured is a complete no-op and
	// leaves no audit row; manual runs are always recorded for operability.
	policyEnabled := maxAgeDays > 0 || maxPerUser > 0
	if trigger == store.CleanupTriggerScheduled && !policyEnabled {
		return nil, nil
	}

	started := s.now()
	run := &store.NotificationCleanupRun{
		StartedTs: started.Unix(),
		Trigger:   trigger,
		Status:    store.CleanupStatusRunning,
		MaxAgeDays: maxAgeDays,
		MaxPerUser: maxPerUser,
	}

	watermark, err := s.store.GetMaxInboxID(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to snapshot inbox watermark")
	}
	run.WatermarkInboxID = watermark

	if maxAgeDays > 0 {
		run.AgeCutoffTs = started.Add(-time.Duration(maxAgeDays) * 24 * time.Hour).Unix()
	}

	run, err = s.store.CreateNotificationCleanupRun(ctx, run)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create cleanup run record")
	}

	if s.afterWatermark != nil {
		s.afterWatermark(watermark)
	}

	if sweepErr := s.applyPolicy(ctx, run, maxAgeDays, maxPerUser); sweepErr != nil {
		s.finalizeRun(run, sweepErr)
		slog.Error("notification retention cleanup failed",
			slog.Any("error", sweepErr),
			slog.String("trigger", string(trigger)),
			slog.Int64("watermark_inbox_id", int64(watermark)),
			slog.Int64("deleted_age_count", int64(run.DeletedAgeCount)),
			slog.Int64("deleted_cap_count", int64(run.DeletedCapCount)),
		)
		return run, sweepErr
	}

	s.finalizeRun(run, nil)
	slog.Info("notification retention cleanup completed",
		slog.String("trigger", string(trigger)),
		slog.Int64("watermark_inbox_id", int64(watermark)),
		slog.Int64("deleted_age_count", int64(run.DeletedAgeCount)),
		slog.Int64("deleted_cap_count", int64(run.DeletedCapCount)),
	)
	return run, nil
}

// applyPolicy performs the deletions. Every delete predicate is bounded by the
// watermark snapshot so notifications dispatched during the sweep are never
// collected.
func (s *RetentionSweeper) applyPolicy(ctx context.Context, run *store.NotificationCleanupRun, maxAgeDays, maxPerUser int32) error {
	if maxAgeDays > 0 {
		cutoff := run.AgeCutoffTs
		affected, lastID, err := s.store.BatchDeleteInboxes(ctx, &store.FindInbox{
			CreatedTsBefore: &cutoff,
			MaxID:           &run.WatermarkInboxID,
		})
		if err != nil {
			return errors.Wrapf(err, "failed to delete inboxes older than %d days after id %d", maxAgeDays, lastID)
		}
		run.DeletedAgeCount = affected
	}

	if maxPerUser > 0 {
		affected, err := s.store.DeleteInboxesOverPerUserLimit(ctx, maxPerUser, run.WatermarkInboxID)
		if err != nil {
			return errors.Wrap(err, "failed to delete inboxes over per-user limit")
		}
		run.DeletedCapCount = affected
	}
	return nil
}

// finalizeRun records the terminal state on a detached context so request
// cancellation cannot lose the audit trail, then prunes old run records.
func (s *RetentionSweeper) finalizeRun(run *store.NotificationCleanupRun, sweepErr error) {
	run.CompleteTs = s.now().Unix()
	if sweepErr == nil {
		run.Status = store.CleanupStatusSucceeded
	} else {
		run.Status = store.CleanupStatusFailed
		detail := sweepErr.Error()
		if len(detail) > cleanupErrorDetailLimit {
			detail = detail[:cleanupErrorDetailLimit]
		}
		run.ErrorDetail = detail
	}

	finalizeCtx, cancel := context.WithTimeout(context.WithoutCancel(context.Background()), cleanupFinalizeTimeout)
	defer cancel()
	if err := s.store.UpdateNotificationCleanupRun(finalizeCtx, run); err != nil {
		slog.Error("failed to finalize notification cleanup run record",
			slog.Any("error", err),
			slog.Int64("run_id", int64(run.ID)))
	}
	if err := s.store.PruneNotificationCleanupRuns(finalizeCtx, cleanupRunHistoryKeep); err != nil {
		slog.Warn("failed to prune notification cleanup run history", slog.Any("error", err))
	}
}

// Start runs the scheduler loop until Stop or ctx cancellation. A no-op when
// already started.
func (s *RetentionSweeper) Start(ctx context.Context) {
	s.scheduleMu.Lock()
	defer s.scheduleMu.Unlock()
	if s.cancel != nil {
		return
	}
	loopCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	s.cancel = cancel
	s.done = done
	s.wg.Add(1)
	go s.loop(loopCtx, done)
}

// Stop cancels the scheduler and waits for the loop goroutine to exit. It does
// not interrupt a sweep already running; that sweep observes the passed
// context through its own request/context handling.
func (s *RetentionSweeper) Stop() {
	s.scheduleMu.Lock()
	cancel := s.cancel
	done := s.done
	s.cancel = nil
	s.done = nil
	s.scheduleMu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

func (s *RetentionSweeper) loop(ctx context.Context, done chan<- struct{}) {
	defer s.wg.Done()
	defer close(done)
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.runScheduled(ctx)
		}
	}
}

func (s *RetentionSweeper) runScheduled(ctx context.Context) {
	run, err := s.Sweep(ctx, store.CleanupTriggerScheduled)
	if errors.Is(err, ErrCleanupAlreadyRunning) {
		slog.Debug("skipping scheduled notification cleanup because another run is in progress")
		return
	}
	if err != nil && run == nil {
		slog.Error("scheduled notification cleanup could not start", slog.Any("error", err))
	}
}
