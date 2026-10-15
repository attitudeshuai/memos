package store

import (
	"context"
	"time"
)

// WebhookDeliveryStatus is the lifecycle state of one outbound webhook delivery.
type WebhookDeliveryStatus string

const (
	// WebhookDeliveryPending means the delivery is registered and waiting for
	// its due time.
	WebhookDeliveryPending WebhookDeliveryStatus = "PENDING"
	// WebhookDeliveryDelivering means a worker has claimed the delivery and the
	// HTTP request is in flight.
	WebhookDeliveryDelivering WebhookDeliveryStatus = "DELIVERING"
	// WebhookDeliverySucceeded means the receiver accepted the delivery. It is
	// terminal and must never be delivered again.
	WebhookDeliverySucceeded WebhookDeliveryStatus = "SUCCEEDED"
	// WebhookDeliveryRetrying means the last attempt failed and another attempt
	// is scheduled at NextDueTs.
	WebhookDeliveryRetrying WebhookDeliveryStatus = "RETRYING"
	// WebhookDeliveryAbandoned means every allowed attempt failed. It is terminal
	// until an explicit replay.
	WebhookDeliveryAbandoned WebhookDeliveryStatus = "ABANDONED"
)

// WebhookDelivery is one lifecycle-tracked outbound webhook delivery.
// Timestamps are unix milliseconds so sub-second backoff intervals are not
// rounded away; zero means the timestamp was never set.
type WebhookDelivery struct {
	ID            int64
	CreatedTs     int64
	UpdatedTs     int64
	UserID        int32
	WebhookID     string
	Label         string
	URL           string
	Status        WebhookDeliveryStatus
	Attempts      int32
	MaxAttempts   int32
	NextDueTs     int64
	LastAttemptTs int64
	SettledTs     int64
	LastError     string
	RequestBody   string
	SigningSecret string
}

// FindWebhookDeliveries selects delivery records. Every field is optional and
// all set fields are combined with AND.
type FindWebhookDeliveries struct {
	ID        *int64
	IDs       []int64
	UserID    *int32
	WebhookID *string
	Status    *WebhookDeliveryStatus

	// Limit and Offset paginate the result, which is always ordered by id DESC.
	Limit  *int
	Offset *int
}

// SettleWebhookDelivery carries the result of one delivery attempt. Only a row
// that is currently DELIVERING with the same Attempts value is settled, so a
// stale or duplicated worker result can never overwrite the ledger.
type SettleWebhookDelivery struct {
	ID        int64
	Attempts  int32
	Status    WebhookDeliveryStatus
	NextDueTs int64
	LastError string
	SettledTs int64
	UpdatedTs int64
}

// CreateWebhookDelivery registers a new delivery record.
func (s *Store) CreateWebhookDelivery(ctx context.Context, create *WebhookDelivery) (*WebhookDelivery, error) {
	nowMilli := time.Now().UnixMilli()
	if create.CreatedTs == 0 {
		create.CreatedTs = nowMilli
	}
	if create.UpdatedTs == 0 {
		create.UpdatedTs = nowMilli
	}
	return s.driver.CreateWebhookDelivery(ctx, create)
}

// ListWebhookDeliveries returns the delivery records matching the filter.
func (s *Store) ListWebhookDeliveries(ctx context.Context, find *FindWebhookDeliveries) ([]*WebhookDelivery, error) {
	return s.driver.ListWebhookDeliveries(ctx, find)
}

// ClaimDueWebhookDeliveries atomically claims up to limit due deliveries
// (PENDING/RETRYING with NextDueTs not after nowTs) and moves them to
// DELIVERING, incrementing Attempts. Concurrent callers never receive the
// same row.
func (s *Store) ClaimDueWebhookDeliveries(ctx context.Context, nowTs int64, limit int) ([]*WebhookDelivery, error) {
	return s.driver.ClaimDueWebhookDeliveries(ctx, nowTs, limit)
}

// SettleWebhookDelivery applies the result of an attempt. It reports whether
// the row was updated; false means the row is no longer owned by the caller.
func (s *Store) SettleWebhookDelivery(ctx context.Context, settle *SettleWebhookDelivery) (bool, error) {
	return s.driver.SettleWebhookDelivery(ctx, settle)
}

// RecoverInterruptedDeliveries moves every stale DELIVERING row back to
// RETRYING and makes it immediately due. Rows in any other state are left
// untouched. It returns the number of recovered rows. nowTs is unix
// milliseconds.
func (s *Store) RecoverInterruptedDeliveries(ctx context.Context, nowTs int64) (int64, error) {
	return s.driver.RecoverInterruptedDeliveries(ctx, nowTs)
}

// ReplayWebhookDelivery requeues an abandoned delivery as a fresh PENDING
// sequence. It returns nil when the record does not exist or is not abandoned.
func (s *Store) ReplayWebhookDelivery(ctx context.Context, id int64) (*WebhookDelivery, error) {
	return s.driver.ReplayWebhookDelivery(ctx, id, time.Now().UnixMilli())
}

// PruneWebhookDeliveries keeps at most keep newest terminal records of one
// webhook and deletes the older ones. Non-terminal rows are never deleted.
func (s *Store) PruneWebhookDeliveries(ctx context.Context, userID int32, webhookID string, keep int) error {
	return s.driver.PruneWebhookDeliveries(ctx, userID, webhookID, keep)
}
