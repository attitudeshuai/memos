package store

import (
	"context"

	storepb "github.com/usememos/memos/proto/gen/store"
)

// InboxStatus represents the status of an inbox notification.
type InboxStatus string

const (
	// UNREAD indicates the notification has not been read by the user.
	UNREAD InboxStatus = "UNREAD"
	// ARCHIVED indicates the notification has been archived/dismissed by the user.
	ARCHIVED InboxStatus = "ARCHIVED"
)

func (s InboxStatus) String() string {
	return string(s)
}

// Inbox represents a notification in a user's inbox.
type Inbox struct {
	ID         int32
	CreatedTs  int64
	SenderID   int32                 // The user who triggered the notification
	ReceiverID int32                 // The user who receives the notification
	Status     InboxStatus           // Current status (unread/archived)
	Message    *storepb.InboxMessage // The notification message content
}

// UpdateInbox contains fields that can be updated for an inbox item.
type UpdateInbox struct {
	ID     int32
	Status InboxStatus
}

// FindInbox specifies filter criteria for querying inbox items.
type FindInbox struct {
	ID          *int32
	SenderID    *int32
	ReceiverID  *int32
	Status      *InboxStatus
	MessageType *storepb.InboxMessage_Type

	// CreatedTsBefore matches only items with created_ts strictly smaller than
	// the value. It backs the "before a point in time" batch scope and the
	// retention age rule.
	CreatedTsBefore *int64
	// MaxID matches only items with ID at most the value. It is the retention
	// cleanup watermark: rows inserted after the watermark snapshot can never
	// match, regardless of their creation timestamp.
	MaxID *int32

	// Pagination
	Limit  *int
	Offset *int
}

// DeleteInbox specifies which inbox item to delete.
type DeleteInbox struct {
	ID int32
}

// CreateInbox creates a new inbox notification.
func (s *Store) CreateInbox(ctx context.Context, create *Inbox) (*Inbox, error) {
	return s.driver.CreateInbox(ctx, create)
}

// ListInboxes retrieves inbox items matching the filter criteria.
func (s *Store) ListInboxes(ctx context.Context, find *FindInbox) ([]*Inbox, error) {
	return s.driver.ListInboxes(ctx, find)
}

// UpdateInbox updates an existing inbox item.
func (s *Store) UpdateInbox(ctx context.Context, update *UpdateInbox) (*Inbox, error) {
	return s.driver.UpdateInbox(ctx, update)
}

// DeleteInbox permanently removes an inbox item.
func (s *Store) DeleteInbox(ctx context.Context, delete *DeleteInbox) error {
	return s.driver.DeleteInbox(ctx, delete)
}

// BatchUpdateInboxStatus sets status on every inbox matching find in bounded,
// independently committed ID-ordered batches. find must carry the predicate
// that keeps already-processed rows out (Status=UNREAD for mark-as-read), so
// retrying the same call after a mid-way failure processes only the remainder.
// It returns the number of rows actually changed and, on failure, the last ID
// of the last committed batch.
func (s *Store) BatchUpdateInboxStatus(ctx context.Context, find *FindInbox, status InboxStatus) (affected int32, lastID int32, err error) {
	return processInboxIDChunks(ctx, inboxBatchChunkSize,
		func(ctx context.Context, limit int) ([]int32, error) {
			return s.driver.ListInboxIDs(ctx, find, limit)
		},
		func(ctx context.Context, ids []int32) (int32, error) {
			return s.driver.UpdateInboxStatusByIDs(ctx, ids, status)
		},
	)
}

// BatchDeleteInboxes permanently removes every inbox matching find in bounded,
// independently committed ID-ordered batches. It returns the number of rows
// actually deleted and, on failure, the last ID of the last committed batch.
// Retrying the same call is idempotent and processes only the remainder.
func (s *Store) BatchDeleteInboxes(ctx context.Context, find *FindInbox) (affected int32, lastID int32, err error) {
	return processInboxIDChunks(ctx, inboxBatchChunkSize,
		func(ctx context.Context, limit int) ([]int32, error) {
			return s.driver.ListInboxIDs(ctx, find, limit)
		},
		func(ctx context.Context, ids []int32) (int32, error) {
			return s.driver.DeleteInboxByIDs(ctx, ids)
		},
	)
}

// GetMaxInboxID returns the largest inbox ID, the monotonic watermark used by
// retention cleanup. It returns 0 when the table is empty.
func (s *Store) GetMaxInboxID(ctx context.Context) (int32, error) {
	return s.driver.GetMaxInboxID(ctx)
}

// DeleteInboxesOverPerUserLimit keeps at most perUserLimit newest inboxes per
// receiver and deletes the rest. Only IDs up to watermark are eligible, so
// notifications inserted after the cleanup snapshot are never collected.
func (s *Store) DeleteInboxesOverPerUserLimit(ctx context.Context, perUserLimit int32, watermark int32) (int32, error) {
	return s.driver.DeleteInboxesOverPerUserLimit(ctx, perUserLimit, watermark)
}

// CreateNotificationCleanupRun persists a new notification cleanup run record.
func (s *Store) CreateNotificationCleanupRun(ctx context.Context, run *NotificationCleanupRun) (*NotificationCleanupRun, error) {
	return s.driver.CreateNotificationCleanupRun(ctx, run)
}

// UpdateNotificationCleanupRun persists the terminal state of a cleanup run.
func (s *Store) UpdateNotificationCleanupRun(ctx context.Context, run *NotificationCleanupRun) error {
	return s.driver.UpdateNotificationCleanupRun(ctx, run)
}

// ListNotificationCleanupRuns returns recent cleanup runs newest first, up to
// limit rows.
func (s *Store) ListNotificationCleanupRuns(ctx context.Context, limit int) ([]*NotificationCleanupRun, error) {
	return s.driver.ListNotificationCleanupRuns(ctx, limit)
}

// PruneNotificationCleanupRuns keeps only the newest keep run records.
func (s *Store) PruneNotificationCleanupRuns(ctx context.Context, keep int) error {
	return s.driver.PruneNotificationCleanupRuns(ctx, keep)
}
