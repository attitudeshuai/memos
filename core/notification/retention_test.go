package notification

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	storepb "github.com/usememos/memos/proto/gen/store"
	"github.com/usememos/memos/store"
	mysqldb "github.com/usememos/memos/store/db/mysql"
	postgresdb "github.com/usememos/memos/store/db/postgres"
	sqlitedb "github.com/usememos/memos/store/db/sqlite"
	teststore "github.com/usememos/memos/store/test"
)

// setInboxCreatedTs rewrites an inbox creation timestamp for a fixed-clock
// sweep. The driver concrete type tells us the column type (MySQL is
// TIMESTAMP, the others are unix seconds).
func setInboxCreatedTs(ctx context.Context, t *testing.T, st *store.Store, id int32, unixTs int64) {
	t.Helper()
	query := "UPDATE inbox SET created_ts = ? WHERE id = ?"
	args := []any{unixTs, id}
	switch st.GetDriver().(type) {
	case *mysqldb.DB:
		query = "UPDATE inbox SET created_ts = FROM_UNIXTIME(?) WHERE id = ?"
	case *sqlitedb.DB, *postgresdb.DB:
		// Direct unix-seconds columns.
	default:
		t.Fatalf("unknown inbox driver: %T", st.GetDriver())
	}
	_, err := st.GetDriver().GetDB().ExecContext(ctx, query, args...)
	require.NoError(t, err)
}

func createRetentionInbox(ctx context.Context, t *testing.T, st *store.Store, receiverID int32, messageType storepb.InboxMessage_Type) *store.Inbox {
	t.Helper()
	inbox, err := st.CreateInbox(ctx, &store.Inbox{
		SenderID:   0,
		ReceiverID: receiverID,
		Status:     store.UNREAD,
		Message:    &storepb.InboxMessage{Type: messageType},
	})
	require.NoError(t, err)
	return inbox
}

func upsertRetentionPolicy(ctx context.Context, t *testing.T, st *store.Store, maxAgeDays, maxPerUser int32) {
	t.Helper()
	_, err := st.UpsertInstanceSetting(ctx, &storepb.InstanceSetting{
		Key: storepb.InstanceSettingKey_NOTIFICATION,
		Value: &storepb.InstanceSetting_NotificationSetting{
			NotificationSetting: &storepb.InstanceNotificationSetting{
				Retention: &storepb.InstanceNotificationSetting_RetentionSetting{
					MaxAgeDays: maxAgeDays,
					MaxPerUser: maxPerUser,
				},
			},
		},
	})
	require.NoError(t, err)
}

func listRunRecords(ctx context.Context, t *testing.T, st *store.Store) []*store.NotificationCleanupRun {
	t.Helper()
	runs, err := st.ListNotificationCleanupRuns(ctx, 50)
	require.NoError(t, err)
	return runs
}

func TestSweepNoPolicyScheduledIsNoop(t *testing.T) {
	ctx := context.Background()
	st := teststore.NewTestingStore(ctx, t)
	defer st.Close()

	owner, err := st.CreateUser(ctx, &store.User{Username: "sweep-nopolicy", Role: store.RoleAdmin, PasswordHash: "hash"})
	require.NoError(t, err)
	createRetentionInbox(ctx, t, st, owner.ID, storepb.InboxMessage_MEMO_COMMENT)

	sweeper := NewRetentionSweeper(st, withClock(func() time.Time { return time.Unix(2_000_000_000, 0) }))
	run, err := sweeper.Sweep(ctx, store.CleanupTriggerScheduled)
	require.NoError(t, err)
	require.Nil(t, run, "scheduled tick without policy returns no run")
	require.Empty(t, listRunRecords(ctx, t, st), "no audit row for a disabled-policy scheduled tick")

	inboxes, err := st.ListInboxes(ctx, &store.FindInbox{ReceiverID: &owner.ID})
	require.NoError(t, err)
	require.Len(t, inboxes, 1, "writes are untouched without a policy")
}

func TestSweepManualNoPolicyRecordsRun(t *testing.T) {
	ctx := context.Background()
	st := teststore.NewTestingStore(ctx, t)
	defer st.Close()

	sweeper := NewRetentionSweeper(st, withClock(func() time.Time { return time.Unix(2_000_000_000, 0) }))
	run, err := sweeper.Sweep(ctx, store.CleanupTriggerManual)
	require.NoError(t, err)
	require.NotNil(t, run)
	require.Equal(t, store.CleanupStatusSucceeded, run.Status)
	require.Equal(t, store.CleanupTriggerManual, run.Trigger)
	require.Equal(t, int32(0), run.WatermarkInboxID)
	require.Equal(t, int32(0), run.DeletedAgeCount)
	require.Equal(t, int32(0), run.DeletedCapCount)
	require.Len(t, listRunRecords(ctx, t, st), 1)
}

func TestSweepAgeRuleDeletesOldNotificationsIncludingUnread(t *testing.T) {
	ctx := context.Background()
	st := teststore.NewTestingStore(ctx, t)
	defer st.Close()
	owner, err := st.CreateUser(ctx, &store.User{Username: "sweep-age", Role: store.RoleAdmin, PasswordHash: "hash"})
	require.NoError(t, err)

	now := time.Unix(2_000_000_000, 0)
	oldTs := now.Add(-11 * 24 * time.Hour).Unix()
	freshTs := now.Add(-time.Hour).Unix()

	var oldInboxIDs []int32
	for range 3 {
		inbox := createRetentionInbox(ctx, t, st, owner.ID, storepb.InboxMessage_MEMO_COMMENT)
		setInboxCreatedTs(ctx, t, st, inbox.ID, oldTs)
		oldInboxIDs = append(oldInboxIDs, inbox.ID)
	}
	// An already archived old notification is collected too; unread ones are
	// not protected.
	archived := createRetentionInbox(ctx, t, st, owner.ID, storepb.InboxMessage_MEMO_MENTION)
	_, err = st.UpdateInbox(ctx, &store.UpdateInbox{ID: archived.ID, Status: store.ARCHIVED})
	require.NoError(t, err)
	setInboxCreatedTs(ctx, t, st, archived.ID, oldTs)
	oldInboxIDs = append(oldInboxIDs, archived.ID)

	var freshIDs []int32
	for range 2 {
		inbox := createRetentionInbox(ctx, t, st, owner.ID, storepb.InboxMessage_MEMO_MENTION)
		setInboxCreatedTs(ctx, t, st, inbox.ID, freshTs)
		freshIDs = append(freshIDs, inbox.ID)
	}

	upsertRetentionPolicy(ctx, t, st, 10, 0)
	sweeper := NewRetentionSweeper(st, withClock(func() time.Time { return now }))

	run, err := sweeper.Sweep(ctx, store.CleanupTriggerScheduled)
	require.NoError(t, err)
	require.NotNil(t, run, "an enabled policy makes scheduled ticks record a run")
	require.Equal(t, store.CleanupStatusSucceeded, run.Status)
	require.Equal(t, int32(4), run.DeletedAgeCount)
	require.Equal(t, int32(0), run.DeletedCapCount)
	require.Equal(t, now.Add(-10*24*time.Hour).Unix(), run.AgeCutoffTs)
	require.Greater(t, run.CompleteTs, int64(0))

	inboxes, err := st.ListInboxes(ctx, &store.FindInbox{ReceiverID: &owner.ID})
	require.NoError(t, err)
	require.Len(t, inboxes, 2)
	remainingIDs := map[int32]struct{}{}
	for _, inbox := range inboxes {
		remainingIDs[inbox.ID] = struct{}{}
	}
	for _, id := range oldInboxIDs {
		require.NotContains(t, remainingIDs, id)
	}
	for _, id := range freshIDs {
		require.Contains(t, remainingIDs, id)
	}
}

func TestSweepCapRuleKeepsNewestPerUser(t *testing.T) {
	ctx := context.Background()
	st := teststore.NewTestingStore(ctx, t)
	defer st.Close()
	alice, err := st.CreateUser(ctx, &store.User{Username: "sweep-cap-alice", Role: store.RoleAdmin, PasswordHash: "hash"})
	require.NoError(t, err)
	bob, err := st.CreateUser(ctx, &store.User{Username: "sweep-cap-bob", Role: store.RoleUser, PasswordHash: "hash"})
	require.NoError(t, err)

	base := time.Unix(2_000_000_000, 0).Unix()
	aliceInboxes := make([]int32, 0, 4)
	for i := range 4 {
		inbox := createRetentionInbox(ctx, t, st, alice.ID, storepb.InboxMessage_MEMO_COMMENT)
		setInboxCreatedTs(ctx, t, st, inbox.ID, base+int64(i)*100)
		aliceInboxes = append(aliceInboxes, inbox.ID)
	}
	bobInbox := createRetentionInbox(ctx, t, st, bob.ID, storepb.InboxMessage_MEMO_COMMENT)
	setInboxCreatedTs(ctx, t, st, bobInbox.ID, base)

	upsertRetentionPolicy(ctx, t, st, 0, 2)
	sweeper := NewRetentionSweeper(st, withClock(func() time.Time { return time.Unix(2_000_001_000, 0) }))
	run, err := sweeper.Sweep(ctx, store.CleanupTriggerScheduled)
	require.NoError(t, err)
	require.Equal(t, int32(0), run.DeletedAgeCount)
	require.Equal(t, int32(2), run.DeletedCapCount)

	aliceID := alice.ID
	remaining, err := st.ListInboxes(ctx, &store.FindInbox{ReceiverID: &aliceID})
	require.NoError(t, err)
	require.Len(t, remaining, 2)
	remainingIDs := map[int32]struct{}{}
	for _, inbox := range remaining {
		remainingIDs[inbox.ID] = struct{}{}
	}
	require.Contains(t, remainingIDs, aliceInboxes[3])
	require.Contains(t, remainingIDs, aliceInboxes[2])

	bobID := bob.ID
	bobRemaining, err := st.ListInboxes(ctx, &store.FindInbox{ReceiverID: &bobID})
	require.NoError(t, err)
	require.Len(t, bobRemaining, 1)
}

func TestSweepWatermarkProtectsConcurrentDispatch(t *testing.T) {
	ctx := context.Background()
	st := teststore.NewTestingStore(ctx, t)
	defer st.Close()
	owner, err := st.CreateUser(ctx, &store.User{Username: "sweep-race", Role: store.RoleAdmin, PasswordHash: "hash"})
	require.NoError(t, err)

	now := time.Unix(2_000_000_000, 0)
	oldTs := now.Add(-100 * 24 * time.Hour).Unix()
	for range 3 {
		inbox := createRetentionInbox(ctx, t, st, owner.ID, storepb.InboxMessage_MEMO_COMMENT)
		setInboxCreatedTs(ctx, t, st, inbox.ID, oldTs)
	}

	// Cap 1 deletes aggressively; the late receiver only exists because the
	// dispatch races the sweep.
	lateUser, err := st.CreateUser(ctx, &store.User{Username: "sweep-race-late", Role: store.RoleUser, PasswordHash: "hash"})
	require.NoError(t, err)

	upsertRetentionPolicy(ctx, t, st, 30, 1)
	watermarkReached := make(chan struct{})
	release := make(chan struct{})
	sweeper := NewRetentionSweeper(st,
		withClock(func() time.Time { return now }),
		WithAfterWatermark(func(_ int32) {
			close(watermarkReached)
			<-release
		}),
	)

	done := make(chan error, 1)
	go func() {
		_, sweepErr := sweeper.Sweep(ctx, store.CleanupTriggerScheduled)
		done <- sweepErr
	}()
	<-watermarkReached

	// Notification dispatch lands after the watermark snapshot. Give it an old
	// timestamp too: ID ordering, not the clock, must save it.
	late := createRetentionInbox(ctx, t, st, lateUser.ID, storepb.InboxMessage_MEMO_MENTION)
	setInboxCreatedTs(ctx, t, st, late.ID, oldTs)
	close(release)
	require.NoError(t, <-done)

	lateID := lateUser.ID
	remaining, err := st.ListInboxes(ctx, &store.FindInbox{ReceiverID: &lateID})
	require.NoError(t, err)
	require.Len(t, remaining, 1, "the notification dispatched during the sweep is never collected")
	require.Equal(t, late.ID, remaining[0].ID)
}

func TestSweepRejectsConcurrentRun(t *testing.T) {
	ctx := context.Background()
	st := teststore.NewTestingStore(ctx, t)
	defer st.Close()
	upsertRetentionPolicy(ctx, t, st, 30, 0)

	entered := make(chan struct{})
	release := make(chan struct{})
	sweeper := NewRetentionSweeper(st, WithAfterWatermark(func(_ int32) {
		close(entered)
		<-release
	}))

	done := make(chan error, 1)
	go func() {
		_, err := sweeper.Sweep(ctx, store.CleanupTriggerManual)
		done <- err
	}()
	<-entered

	_, err := sweeper.Sweep(ctx, store.CleanupTriggerManual)
	require.ErrorIs(t, err, ErrCleanupAlreadyRunning)

	close(release)
	require.NoError(t, <-done)
}

func TestSweepFailureRecordsFailedRunDespiteCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	st := teststore.NewTestingStore(ctx, t)
	defer st.Close()
	owner, err := st.CreateUser(ctx, &store.User{Username: "sweep-fail", Role: store.RoleAdmin, PasswordHash: "hash"})
	require.NoError(t, err)
	inbox := createRetentionInbox(ctx, t, st, owner.ID, storepb.InboxMessage_MEMO_COMMENT)
	setInboxCreatedTs(ctx, t, st, inbox.ID, time.Unix(1_000_000_000, 0).Unix())
	upsertRetentionPolicy(ctx, t, st, 30, 0)

	sweeper := NewRetentionSweeper(st, WithAfterWatermark(func(_ int32) {
		cancel()
	}))
	run, err := sweeper.Sweep(ctx, store.CleanupTriggerScheduled)
	require.Error(t, err)
	require.NotNil(t, run)
	require.Equal(t, store.CleanupStatusFailed, run.Status)
	require.NotEmpty(t, run.ErrorDetail)

	// The audit write used a detached context, so the FAILED row is queryable.
	runs := listRunRecords(context.Background(), t, st)
	require.Len(t, runs, 1)
	require.Equal(t, store.CleanupStatusFailed, runs[0].Status)
}

func TestRetentionSchedulerRunsOnIntervalAndStops(t *testing.T) {
	ctx := context.Background()
	st := teststore.NewTestingStore(ctx, t)
	defer st.Close()
	upsertRetentionPolicy(ctx, t, st, 30, 0)

	base := time.Unix(2_000_000_000, 0)
	var clockTicks atomic.Int64
	sweeper := NewRetentionSweeper(st,
		WithInterval(15*time.Millisecond),
		withClock(func() time.Time { return base.Add(time.Duration(clockTicks.Add(1)) * time.Second) }),
	)

	startCtx, cancelStart := context.WithCancel(ctx)
	sweeper.Start(startCtx)

	require.Eventually(t, func() bool {
		return len(listRunRecords(ctx, t, st)) >= 2
	}, 5*time.Second, 10*time.Millisecond)

	cancelStart()
	sweeper.Stop()
	countAfterStop := len(listRunRecords(ctx, t, st))
	time.Sleep(60 * time.Millisecond)
	require.Len(t, listRunRecords(ctx, t, st), countAfterStop, "no sweeps after Stop")
}

func TestSweepSecondRunIsIdempotent(t *testing.T) {
	ctx := context.Background()
	st := teststore.NewTestingStore(ctx, t)
	defer st.Close()
	owner, err := st.CreateUser(ctx, &store.User{Username: "sweep-idem", Role: store.RoleAdmin, PasswordHash: "hash"})
	require.NoError(t, err)
	now := time.Unix(2_000_000_000, 0)
	old := createRetentionInbox(ctx, t, st, owner.ID, storepb.InboxMessage_MEMO_COMMENT)
	setInboxCreatedTs(ctx, t, st, old.ID, now.Add(-40*24*time.Hour).Unix())
	upsertRetentionPolicy(ctx, t, st, 30, 0)
	sweeper := NewRetentionSweeper(st, withClock(func() time.Time { return now }))

	first, err := sweeper.Sweep(ctx, store.CleanupTriggerScheduled)
	require.NoError(t, err)
	require.Equal(t, int32(1), first.DeletedAgeCount)
	second, err := sweeper.Sweep(ctx, store.CleanupTriggerScheduled)
	require.NoError(t, err)
	require.Equal(t, int32(0), second.DeletedAgeCount)
	require.Len(t, listRunRecords(ctx, t, st), 2)
}
