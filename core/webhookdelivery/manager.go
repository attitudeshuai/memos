package webhookdelivery

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/usememos/memos/internal/webhook"
	"github.com/usememos/memos/store"
)

const (
	defaultPollInterval = time.Second
	defaultClaimBatch   = 16
	defaultConcurrency  = 4
	// shutdownGrace bounds how long in-flight deliveries may finish and write
	// their settlement during Close. Attempts still running afterwards are
	// recovered from the DELIVERING state on the next start.
	shutdownGrace = 10 * time.Second
	// lastErrorLimit keeps ledger rows readable even when a receiver answers
	// with a long error page.
	lastErrorLimit = 2048
)

// Option customizes a Manager, mainly to speed up tests.
type Option func(*Manager)

// WithClock replaces the wall clock used for due times and settlements.
func WithClock(now func() time.Time) Option {
	return func(m *Manager) {
		if now != nil {
			m.now = now
		}
	}
}

// WithPollInterval sets how often idle workers re-check for due deliveries.
func WithPollInterval(interval time.Duration) Option {
	return func(m *Manager) {
		if interval > 0 {
			m.pollInterval = interval
		}
	}
}

// WithConcurrency sets the number of delivery workers.
func WithConcurrency(n int) Option {
	return func(m *Manager) {
		if n > 0 {
			m.concurrency = n
		}
	}
}

// Manager drives the outbound webhook delivery ledger. Its zero value is not
// usable; construct it with New.
type Manager struct {
	store *store.Store
	log   *slog.Logger
	now   func() time.Time

	pollInterval time.Duration
	claimBatch   int
	concurrency  int

	policy atomic.Pointer[Policy]
	wake   chan struct{}

	startOnce sync.Once
	closeOnce sync.Once
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

// New creates a delivery manager. The manager is inert until Start: until then
// every submission takes the legacy fire-and-forget path.
func New(st *store.Store, opts ...Option) *Manager {
	m := &Manager{
		store:        st,
		log:          slog.Default(),
		now:          time.Now,
		pollInterval: defaultPollInterval,
		claimBatch:   defaultClaimBatch,
		concurrency:  defaultConcurrency,
		wake:         make(chan struct{}, 1),
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// Start loads the policy, recovers deliveries interrupted by a previous
// process, and spawns the delivery workers. It is safe to call once.
func (m *Manager) Start(ctx context.Context) error {
	var startErr error
	m.startOnce.Do(func() {
		m.ctx, m.cancel = context.WithCancel(context.Background())
		if loadErr := m.ReloadPolicy(ctx); loadErr != nil {
			startErr = errors.Wrap(loadErr, "failed to load webhook delivery policy")
			return
		}
		if recovered, recoverErr := m.store.RecoverInterruptedDeliveries(ctx, m.now().UnixMilli()); recoverErr != nil {
			startErr = errors.Wrap(recoverErr, "failed to recover interrupted webhook deliveries")
			return
		} else if recovered > 0 {
			m.log.Info("recovered interrupted webhook deliveries", "count", recovered)
		}
		for range m.concurrency {
			m.wg.Add(1)
			go m.worker()
		}
	})
	return startErr
}

// Close stops claiming new deliveries and waits briefly for in-flight
// settlements, then cancels the workers. It is safe to call more than once.
func (m *Manager) Close() {
	m.closeOnce.Do(func() {
		if m.cancel == nil {
			return
		}
		m.cancel()
		done := make(chan struct{})
		go func() {
			m.wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(shutdownGrace):
			m.log.Warn("webhook delivery manager shutdown timed out; in-flight deliveries will be recovered on next start")
		}
	})
}

// Submit registers one asynchronous delivery. When the lifecycle policy is
// enabled the delivery is persisted before scheduling; otherwise it takes the
// legacy non-blocking queue path.
func (m *Manager) Submit(ctx context.Context, request *webhook.Request) {
	if request == nil {
		m.log.Warn("dropped webhook dispatch because request is nil")
		return
	}
	policy := m.currentPolicy()
	if !policy.Enabled {
		webhook.PostAsync(request)
		return
	}

	body, err := json.Marshal(request.Payload)
	if err != nil {
		m.log.Error("failed to encode webhook request body; using legacy async dispatch",
			"url", request.URL, "label", request.Label, "error", err.Error())
		webhook.PostAsync(request)
		return
	}

	nowMilli := m.now().UnixMilli()
	row := &store.WebhookDelivery{
		UserID:        request.UserID,
		WebhookID:     request.WebhookID,
		Label:         request.Label,
		URL:           request.URL,
		Status:        store.WebhookDeliveryPending,
		MaxAttempts:   policy.MaxAttempts,
		NextDueTs:     nowMilli,
		RequestBody:   string(body),
		SigningSecret: request.SigningSecret,
	}
	if _, err := m.store.CreateWebhookDelivery(ctx, row); err != nil {
		// Never silently swallow a submission: fall back to the legacy channel.
		m.log.Error("failed to register webhook delivery; using legacy async dispatch",
			"url", request.URL, "label", request.Label, "error", err.Error())
		webhook.PostAsync(request)
		return
	}
	m.signal()
}

// ReloadPolicy reloads the instance webhook setting and atomically publishes
// the resolved policy.
func (m *Manager) ReloadPolicy(ctx context.Context) error {
	setting, err := m.store.GetInstanceWebhookSetting(ctx)
	if err != nil {
		return err
	}
	policy := PolicyFromSetting(setting)
	m.policy.Store(&policy)
	return nil
}

// Wake nudges the workers to re-scan due deliveries immediately, for example
// after a delivery was replayed through the API.
func (m *Manager) Wake() {
	m.signal()
}

func (m *Manager) currentPolicy() Policy {
	if p := m.policy.Load(); p != nil {
		return *p
	}
	return Policy{}
}

func (m *Manager) signal() {
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

func (m *Manager) worker() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.pollInterval)
	defer ticker.Stop()
	for {
		if m.ctx.Err() != nil {
			return
		}
		claimed, err := m.store.ClaimDueWebhookDeliveries(m.ctx, m.now().UnixMilli(), m.claimBatch)
		if err != nil {
			if m.ctx.Err() != nil {
				return
			}
			m.log.Error("failed to claim due webhook deliveries", "error", err.Error())
			m.wait(ticker)
			continue
		}
		if len(claimed) == 0 {
			m.wait(ticker)
			continue
		}
		for _, delivery := range claimed {
			if m.ctx.Err() != nil {
				return
			}
			m.deliver(delivery)
		}
	}
}

func (m *Manager) wait(ticker *time.Ticker) {
	select {
	case <-m.ctx.Done():
	case <-ticker.C:
	case <-m.wake:
	}
}

func (m *Manager) deliver(row *store.WebhookDelivery) {
	request := &webhook.Request{
		URL:           row.URL,
		Label:         row.Label,
		SigningSecret: row.SigningSecret,
		Payload:       json.RawMessage(row.RequestBody),
		UserID:        row.UserID,
		WebhookID:     row.WebhookID,
	}
	postErr := webhook.Post(request)
	nowMilli := m.now().UnixMilli()

	if postErr == nil {
		settled, err := m.store.SettleWebhookDelivery(m.ctx, &store.SettleWebhookDelivery{
			ID: row.ID, Attempts: row.Attempts, Status: store.WebhookDeliverySucceeded,
			SettledTs: nowMilli, UpdatedTs: nowMilli,
		})
		if err != nil {
			m.log.Error("failed to mark webhook delivery succeeded", "delivery_id", row.ID, "error", err.Error())
			return
		}
		if !settled {
			m.log.Warn("webhook delivery result ignored; record is no longer owned by this worker", "delivery_id", row.ID)
			return
		}
		m.prune(row)
		return
	}

	lastError := truncateError(postErr.Error())
	status := store.WebhookDeliveryRetrying
	nextDue := int64(0)
	if int(row.Attempts) >= int(row.MaxAttempts) {
		status = store.WebhookDeliveryAbandoned
	} else {
		policy := m.currentPolicy()
		nextDue = nowMilli + backoffDelay(row.Attempts, policy.BackoffBase, policy.BackoffMax).Milliseconds()
	}
	settled, err := m.store.SettleWebhookDelivery(m.ctx, &store.SettleWebhookDelivery{
		ID: row.ID, Attempts: row.Attempts, Status: status, NextDueTs: nextDue,
		LastError: lastError, SettledTs: settledTsFor(status, nowMilli), UpdatedTs: nowMilli,
	})
	if err != nil {
		m.log.Error("failed to settle failed webhook delivery", "delivery_id", row.ID, "error", err.Error())
		return
	}
	if !settled {
		m.log.Warn("webhook delivery failure result ignored; record is no longer owned by this worker", "delivery_id", row.ID)
		return
	}

	attrs := []any{"delivery_id", row.ID, "webhook_id", row.WebhookID, "attempts", row.Attempts,
		"max_attempts", row.MaxAttempts, "url", row.URL, "label", row.Label, "error", lastError}
	switch status {
	case store.WebhookDeliveryAbandoned:
		m.log.Warn("webhook delivery abandoned after exhausting attempts", attrs...)
		m.prune(row)
	default:
		m.log.Warn("webhook delivery failed; retry scheduled", append(attrs, "next_due_ts", nextDue)...)
	}
}

func (m *Manager) prune(row *store.WebhookDelivery) {
	if err := m.store.PruneWebhookDeliveries(m.ctx, row.UserID, row.WebhookID, m.currentPolicy().Retention); err != nil {
		m.log.Error("failed to prune webhook delivery ledger",
			"webhook_id", row.WebhookID, "error", err.Error())
	}
}

func settledTsFor(status store.WebhookDeliveryStatus, now int64) int64 {
	if status == store.WebhookDeliverySucceeded || status == store.WebhookDeliveryAbandoned {
		return now
	}
	return 0
}

func truncateError(message string) string {
	if len(message) > lastErrorLimit {
		return message[:lastErrorLimit]
	}
	return message
}
