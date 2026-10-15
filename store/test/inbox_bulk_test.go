package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	storepb "github.com/usememos/memos/proto/gen/store"
	"github.com/usememos/memos/store"
)

func createInboxForTest(ctx context.Context, t *testing.T, ts *store.Store, senderID, receiverID int32, status store.InboxStatus, messageType storepb.InboxMessage_Type) *store.Inbox {
	t.Helper()
	inbox, err := ts.CreateInbox(ctx, &store.Inbox{
		SenderID:   senderID,
		ReceiverID: receiverID,
		Status:     status,
		Message:    &storepb.InboxMessage{Type: messageType},
	})
	require.NoError(t, err)
	return inbox
}

// setInboxCreatedTs rewrites an inbox creation timestamp. MySQL stores a
// TIMESTAMP column, the other drivers store unix seconds directly.
func setInboxCreatedTs(ctx context.Context, t *testing.T, ts *store.Store, id int32, unixTs int64) {
	t.Helper()
	query := "UPDATE inbox SET created_ts = ? WHERE id = ?"
	args := []any{unixTs, id}
	if getDriverFromEnv() == "mysql" {
		query = "UPDATE inbox SET created_ts = FROM_UNIXTIME(?) WHERE id = ?"
	}
	_, err := ts.GetDriver().GetDB().ExecContext(ctx, query, args...)
	require.NoError(t, err)
}

func inboxStatusPtr(status store.InboxStatus) *store.InboxStatus {
	return &status
}

func countInboxStatus(inboxes []*store.Inbox, status store.InboxStatus) int {
	count := 0
	for _, inbox := range inboxes {
		if inbox.Status == status {
			count++
		}
	}
	return count
}

func TestInboxBatchUpdateStatus(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)
	defer ts.Close()

	owner, err := createTestingHostUser(ctx, ts)
	require.NoError(t, err)
	other, err := createTestingUserWithRole(ctx, ts, "batch-other", store.RoleUser)
	require.NoError(t, err)

	for range 3 {
		createInboxForTest(ctx, t, ts, 0, owner.ID, store.UNREAD, storepb.InboxMessage_MEMO_COMMENT)
	}
	for range 2 {
		createInboxForTest(ctx, t, ts, 0, owner.ID, store.ARCHIVED, storepb.InboxMessage_MEMO_MENTION)
	}
	createInboxForTest(ctx, t, ts, 0, other.ID, store.UNREAD, storepb.InboxMessage_MEMO_COMMENT)

	ownerID := owner.ID
	affected, _, err := ts.BatchUpdateInboxStatus(ctx, &store.FindInbox{
		ReceiverID: &ownerID,
		Status:     inboxStatusPtr(store.UNREAD),
	}, store.ARCHIVED)
	require.NoError(t, err)
	require.Equal(t, int32(3), affected, "only the three UNREAD rows change")

	ownerInboxes, err := ts.ListInboxes(ctx, &store.FindInbox{ReceiverID: &ownerID})
	require.NoError(t, err)
	require.Len(t, ownerInboxes, 5)
	require.Equal(t, 0, countInboxStatus(ownerInboxes, store.UNREAD))
	require.Equal(t, 5, countInboxStatus(ownerInboxes, store.ARCHIVED))

	otherID := other.ID
	otherInboxes, err := ts.ListInboxes(ctx, &store.FindInbox{ReceiverID: &otherID})
	require.NoError(t, err)
	require.Len(t, otherInboxes, 1)
	require.Equal(t, store.UNREAD, otherInboxes[0].Status, "another receiver must not be affected")

	// The same call again changes nothing.
	affected, _, err = ts.BatchUpdateInboxStatus(ctx, &store.FindInbox{
		ReceiverID: &ownerID,
		Status:     inboxStatusPtr(store.UNREAD),
	}, store.ARCHIVED)
	require.NoError(t, err)
	require.Equal(t, int32(0), affected)
}

func TestInboxBatchDeleteByType(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)
	defer ts.Close()

	owner, err := createTestingHostUser(ctx, ts)
	require.NoError(t, err)
	other, err := createTestingUserWithRole(ctx, ts, "batch-type-other", store.RoleUser)
	require.NoError(t, err)

	commentType := storepb.InboxMessage_MEMO_COMMENT
	for range 3 {
		createInboxForTest(ctx, t, ts, 0, owner.ID, store.UNREAD, storepb.InboxMessage_MEMO_COMMENT)
	}
	createInboxForTest(ctx, t, ts, 0, owner.ID, store.UNREAD, storepb.InboxMessage_MEMO_MENTION)
	createInboxForTest(ctx, t, ts, 0, owner.ID, store.ARCHIVED, storepb.InboxMessage_SPACE_INVITATION)
	createInboxForTest(ctx, t, ts, 0, other.ID, store.UNREAD, storepb.InboxMessage_MEMO_COMMENT)

	ownerID := owner.ID
	affected, _, err := ts.BatchDeleteInboxes(ctx, &store.FindInbox{
		ReceiverID:  &ownerID,
		MessageType: &commentType,
	})
	require.NoError(t, err)
	require.Equal(t, int32(3), affected)

	ownerInboxes, err := ts.ListInboxes(ctx, &store.FindInbox{ReceiverID: &ownerID})
	require.NoError(t, err)
	require.Len(t, ownerInboxes, 2)
	for _, inbox := range ownerInboxes {
		require.NotEqual(t, storepb.InboxMessage_MEMO_COMMENT, inbox.Message.Type)
	}

	otherID := other.ID
	otherInboxes, err := ts.ListInboxes(ctx, &store.FindInbox{ReceiverID: &otherID})
	require.NoError(t, err)
	require.Len(t, otherInboxes, 1, "another receiver must not be affected")
}

func TestInboxBatchScopeBeforeAndCombination(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)
	defer ts.Close()

	owner, err := createTestingHostUser(ctx, ts)
	require.NoError(t, err)
	ownerID := owner.ID

	base := int64(1_700_000_000)
	older := createInboxForTest(ctx, t, ts, 0, owner.ID, store.UNREAD, storepb.InboxMessage_MEMO_COMMENT)
	middle := createInboxForTest(ctx, t, ts, 0, owner.ID, store.UNREAD, storepb.InboxMessage_MEMO_COMMENT)
	newer := createInboxForTest(ctx, t, ts, 0, owner.ID, store.UNREAD, storepb.InboxMessage_MEMO_MENTION)
	setInboxCreatedTs(ctx, t, ts, older.ID, base)
	setInboxCreatedTs(ctx, t, ts, middle.ID, base+100)
	setInboxCreatedTs(ctx, t, ts, newer.ID, base+200)

	// Delete strictly before base+100: only the oldest row matches.
	cutoff := base + 100
	affected, _, err := ts.BatchDeleteInboxes(ctx, &store.FindInbox{
		ReceiverID:      &ownerID,
		CreatedTsBefore: &cutoff,
	})
	require.NoError(t, err)
	require.Equal(t, int32(1), affected)

	// Type + before combination: remaining comment (middle, base+100) matches,
	// mention (newer, base+200) fails both the type and the time predicate.
	commentType := storepb.InboxMessage_MEMO_COMMENT
	latestCutoff := base + 200
	affected, _, err = ts.BatchUpdateInboxStatus(ctx, &store.FindInbox{
		ReceiverID:      &ownerID,
		Status:          inboxStatusPtr(store.UNREAD),
		MessageType:     &commentType,
		CreatedTsBefore: &latestCutoff,
	}, store.ARCHIVED)
	require.NoError(t, err)
	require.Equal(t, int32(1), affected)

	inboxes, err := ts.ListInboxes(ctx, &store.FindInbox{ReceiverID: &ownerID})
	require.NoError(t, err)
	require.Len(t, inboxes, 2)
	statusByID := map[int32]store.InboxStatus{}
	for _, inbox := range inboxes {
		statusByID[inbox.ID] = inbox.Status
	}
	require.Equal(t, store.ARCHIVED, statusByID[middle.ID])
	require.Equal(t, store.UNREAD, statusByID[newer.ID])
}

func TestInboxBatchChunksAcrossBatchBoundary(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)
	defer ts.Close()

	owner, err := createTestingHostUser(ctx, ts)
	require.NoError(t, err)
	ownerID := owner.ID

	// More rows than one default chunk (500), with a non-round remainder.
	const total = 550
	for range total {
		createInboxForTest(ctx, t, ts, 0, owner.ID, store.UNREAD, storepb.InboxMessage_MEMO_COMMENT)
	}

	affected, lastID, err := ts.BatchUpdateInboxStatus(ctx, &store.FindInbox{
		ReceiverID: &ownerID,
		Status:     inboxStatusPtr(store.UNREAD),
	}, store.ARCHIVED)
	require.NoError(t, err)
	require.Equal(t, int32(total), affected)
	require.NotZero(t, lastID)

	inboxes, err := ts.ListInboxes(ctx, &store.FindInbox{ReceiverID: &ownerID})
	require.NoError(t, err)
	require.Len(t, inboxes, total)
	require.Equal(t, total, countInboxStatus(inboxes, store.ARCHIVED))
}

func TestInboxWatermarkProtectsNewRows(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)
	defer ts.Close()

	owner, err := createTestingHostUser(ctx, ts)
	require.NoError(t, err)
	ownerID := owner.ID

	oldTs := int64(1_700_000_000)
	oldIDs := make([]int32, 0, 5)
	for range 5 {
		inbox := createInboxForTest(ctx, t, ts, 0, owner.ID, store.UNREAD, storepb.InboxMessage_MEMO_COMMENT)
		setInboxCreatedTs(ctx, t, ts, inbox.ID, oldTs)
		oldIDs = append(oldIDs, inbox.ID)
	}

	watermark, err := ts.GetMaxInboxID(ctx)
	require.NoError(t, err)
	require.Equal(t, oldIDs[len(oldIDs)-1], watermark)

	// A notification dispatched after the snapshot: newer ID, but its
	// timestamp is forced into the age window. The watermark must still
	// protect it.
	dispatched := createInboxForTest(ctx, t, ts, 0, owner.ID, store.UNREAD, storepb.InboxMessage_MEMO_COMMENT)
	setInboxCreatedTs(ctx, t, ts, dispatched.ID, oldTs)
	require.Greater(t, dispatched.ID, watermark)

	// Empty table returns watermark 0 (checked implicitly on a fresh store
	// inside other tests); here cutoff selects every aged row.
	cutoff := oldTs + 1
	affected, _, err := ts.BatchDeleteInboxes(ctx, &store.FindInbox{
		ReceiverID:      &ownerID,
		CreatedTsBefore: &cutoff,
		MaxID:           &watermark,
	})
	require.NoError(t, err)
	require.Equal(t, int32(5), affected)

	inboxes, err := ts.ListInboxes(ctx, &store.FindInbox{ReceiverID: &ownerID})
	require.NoError(t, err)
	require.Len(t, inboxes, 1)
	require.Equal(t, dispatched.ID, inboxes[0].ID, "the post-watermark dispatch must survive")

	// Watermark at the current max now covers the surviving row.
	maxID, err := ts.GetMaxInboxID(ctx)
	require.NoError(t, err)
	require.Equal(t, dispatched.ID, maxID)
}

func TestInboxDeleteOverPerUserLimit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)
	defer ts.Close()

	alice, err := createTestingHostUser(ctx, ts)
	require.NoError(t, err)
	bob, err := createTestingUserWithRole(ctx, ts, "cap-bob", store.RoleUser)
	require.NoError(t, err)

	base := int64(1_700_000_000)
	// Alice has 5 rows; rows 2 and 3 share a timestamp so ID breaks the tie.
	aliceInboxes := make([]*store.Inbox, 0, 5)
	for range 5 {
		inbox := createInboxForTest(ctx, t, ts, 0, alice.ID, store.UNREAD, storepb.InboxMessage_MEMO_COMMENT)
		aliceInboxes = append(aliceInboxes, inbox)
	}
	setInboxCreatedTs(ctx, t, ts, aliceInboxes[0].ID, base)
	setInboxCreatedTs(ctx, t, ts, aliceInboxes[1].ID, base+100)
	setInboxCreatedTs(ctx, t, ts, aliceInboxes[2].ID, base+100)
	setInboxCreatedTs(ctx, t, ts, aliceInboxes[3].ID, base+200)
	setInboxCreatedTs(ctx, t, ts, aliceInboxes[4].ID, base+300)

	// Bob is already within the cap and must lose nothing.
	for i := range 2 {
		inbox := createInboxForTest(ctx, t, ts, 0, bob.ID, store.UNREAD, storepb.InboxMessage_MEMO_COMMENT)
		setInboxCreatedTs(ctx, t, ts, inbox.ID, base+int64(i)*100)
	}

	// A row arriving after the watermark is excluded even though it would push
	// a fresh receiver over the cap.
	carol, err := createTestingUserWithRole(ctx, ts, "cap-carol", store.RoleUser)
	require.NoError(t, err)

	watermark, err := ts.GetMaxInboxID(ctx)
	require.NoError(t, err)
	lateInbox := createInboxForTest(ctx, t, ts, 0, carol.ID, store.UNREAD, storepb.InboxMessage_MEMO_COMMENT)
	setInboxCreatedTs(ctx, t, ts, lateInbox.ID, base)
	require.Greater(t, lateInbox.ID, watermark)

	affected, err := ts.DeleteInboxesOverPerUserLimit(ctx, 2, watermark)
	require.NoError(t, err)
	require.Equal(t, int32(3), affected, "alice loses the three oldest; bob and carol lose nothing")

	aliceID := alice.ID
	remaining, err := ts.ListInboxes(ctx, &store.FindInbox{ReceiverID: &aliceID})
	require.NoError(t, err)
	require.Len(t, remaining, 2)
	remainingIDs := map[int32]struct{}{}
	for _, inbox := range remaining {
		remainingIDs[inbox.ID] = struct{}{}
	}
	// Newest by created_ts DESC with the higher ID winning the base+100 tie.
	require.Contains(t, remainingIDs, aliceInboxes[4].ID)
	require.Contains(t, remainingIDs, aliceInboxes[3].ID)
	require.NotContains(t, remainingIDs, aliceInboxes[2].ID, "the lower ID loses the timestamp tie")

	bobID := bob.ID
	bobRemaining, err := ts.ListInboxes(ctx, &store.FindInbox{ReceiverID: &bobID})
	require.NoError(t, err)
	require.Len(t, bobRemaining, 2)

	carolID := carol.ID
	carolRemaining, err := ts.ListInboxes(ctx, &store.FindInbox{ReceiverID: &carolID})
	require.NoError(t, err)
	require.Len(t, carolRemaining, 1, "post-watermark rows are outside the cleanup snapshot")
	require.Equal(t, lateInbox.ID, carolRemaining[0].ID)

	// A second run at the new watermark is idempotent for Alice and now
	// collects Carol's single over-cap... with cap 2 she stays within it.
	newWatermark, err := ts.GetMaxInboxID(ctx)
	require.NoError(t, err)
	affected, err = ts.DeleteInboxesOverPerUserLimit(ctx, 2, newWatermark)
	require.NoError(t, err)
	require.Equal(t, int32(0), affected)
}

func TestNotificationCleanupRunLifecycle(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)
	defer ts.Close()

	created := make([]*store.NotificationCleanupRun, 0, 3)
	for i, trigger := range []store.NotificationCleanupTrigger{
		store.CleanupTriggerScheduled,
		store.CleanupTriggerManual,
		store.CleanupTriggerScheduled,
	} {
		run, err := ts.CreateNotificationCleanupRun(ctx, &store.NotificationCleanupRun{
			StartedTs:        int64(1_700_000_000 + int64(i)*100),
			Trigger:          trigger,
			Status:           store.CleanupStatusRunning,
			WatermarkInboxID: int32(10 + i),
			MaxAgeDays:       30,
			MaxPerUser:       100,
		})
		require.NoError(t, err)
		require.NotZero(t, run.ID)
		created = append(created, run)
	}

	finish := created[2]
	finish.Status = store.CleanupStatusSucceeded
	finish.CompleteTs = finish.StartedTs + 1
	finish.AgeCutoffTs = finish.StartedTs - 30*86400
	finish.DeletedAgeCount = 7
	finish.DeletedCapCount = 3
	require.NoError(t, ts.UpdateNotificationCleanupRun(ctx, finish))

	runs, err := ts.ListNotificationCleanupRuns(ctx, 10)
	require.NoError(t, err)
	require.Len(t, runs, 3)
	require.Equal(t, created[2].ID, runs[0].ID, "newest start first")
	require.Equal(t, created[0].ID, runs[2].ID)
	require.Equal(t, store.CleanupStatusSucceeded, runs[0].Status)
	require.Equal(t, int32(7), runs[0].DeletedAgeCount)
	require.Equal(t, int32(3), runs[0].DeletedCapCount)

	require.NoError(t, ts.PruneNotificationCleanupRuns(ctx, 2))
	runs, err = ts.ListNotificationCleanupRuns(ctx, 10)
	require.NoError(t, err)
	require.Len(t, runs, 2)
	prunedIDs := map[int32]struct{}{}
	for _, run := range runs {
		prunedIDs[run.ID] = struct{}{}
	}
	require.Contains(t, prunedIDs, created[2].ID)
	require.Contains(t, prunedIDs, created[1].ID)
	require.NotContains(t, prunedIDs, created[0].ID, "the oldest run is pruned first")
}

func TestGetMaxInboxIDEmptyTable(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)
	defer ts.Close()

	maxID, err := ts.GetMaxInboxID(ctx)
	require.NoError(t, err)
	require.Equal(t, int32(0), maxID, "an empty inbox reports watermark zero")
}
