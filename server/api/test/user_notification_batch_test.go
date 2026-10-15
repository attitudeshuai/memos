package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	apipb "github.com/usememos/memos/proto/gen/api"
	storepb "github.com/usememos/memos/proto/gen/store"
	"github.com/usememos/memos/store"
	mysqldb "github.com/usememos/memos/store/db/mysql"
	postgresdb "github.com/usememos/memos/store/db/postgres"
	sqlitedb "github.com/usememos/memos/store/db/sqlite"
)

func createInboxForServiceTest(ctx context.Context, t *testing.T, ts *TestService, receiverID int32, status store.InboxStatus, messageType storepb.InboxMessage_Type) *store.Inbox {
	t.Helper()
	inbox, err := ts.Store.CreateInbox(ctx, &store.Inbox{
		SenderID:   0,
		ReceiverID: receiverID,
		Status:     status,
		Message:    &storepb.InboxMessage{Type: messageType},
	})
	require.NoError(t, err)
	return inbox
}

func setServiceInboxCreatedTs(ctx context.Context, t *testing.T, ts *TestService, id int32, unixTs int64) {
	t.Helper()
	query := "UPDATE inbox SET created_ts = ? WHERE id = ?"
	args := []any{unixTs, id}
	switch ts.Store.GetDriver().(type) {
	case *mysqldb.DB:
		query = "UPDATE inbox SET created_ts = FROM_UNIXTIME(?) WHERE id = ?"
	case *sqlitedb.DB, *postgresdb.DB:
	default:
		t.Fatalf("unknown inbox driver: %T", ts.Store.GetDriver())
	}
	_, err := ts.Store.GetDriver().GetDB().ExecContext(ctx, query, args...)
	require.NoError(t, err)
}

func TestBatchUpdateUserNotificationsMarksAllUnread(t *testing.T) {
	ctx := context.Background()
	ts := NewTestService(t)
	defer ts.Cleanup()

	owner, err := ts.CreateRegularUser(ctx, "batch-read-owner")
	require.NoError(t, err)
	ownerCtx := ts.CreateUserContext(ctx, owner.ID)

	for range 3 {
		createInboxForServiceTest(ctx, t, ts, owner.ID, store.UNREAD, storepb.InboxMessage_MEMO_COMMENT)
	}
	for range 2 {
		createInboxForServiceTest(ctx, t, ts, owner.ID, store.ARCHIVED, storepb.InboxMessage_MEMO_MENTION)
	}

	resp, err := ts.Service.BatchUpdateUserNotifications(ownerCtx, &apipb.BatchUpdateUserNotificationsRequest{
		Parent: fmt.Sprintf("users/%s", owner.Username),
		Scope:  &apipb.UserNotificationBatchScope{},
	})
	require.NoError(t, err)
	require.Equal(t, int32(3), resp.AffectedCount)

	// Assert at store level: directly inserted inboxes carry no memo payload, so the
	// notification list converter intentionally skips them, but the status change is
	// what the batch operation owns.
	ownerID := owner.ID
	inboxes, err := ts.Store.ListInboxes(ctx, &store.FindInbox{ReceiverID: &ownerID})
	require.NoError(t, err)
	require.Len(t, inboxes, 5)
	for _, inbox := range inboxes {
		require.Equal(t, store.ARCHIVED, inbox.Status)
	}

	// Idempotent retry: nothing is UNREAD anymore.
	resp, err = ts.Service.BatchUpdateUserNotifications(ownerCtx, &apipb.BatchUpdateUserNotificationsRequest{
		Parent: fmt.Sprintf("users/%s", owner.Username),
		Scope:  &apipb.UserNotificationBatchScope{},
	})
	require.NoError(t, err)
	require.Equal(t, int32(0), resp.AffectedCount)
}

func TestBatchDeleteUserNotificationsByType(t *testing.T) {
	ctx := context.Background()
	ts := NewTestService(t)
	defer ts.Cleanup()

	owner, err := ts.CreateRegularUser(ctx, "batch-delete-type-owner")
	require.NoError(t, err)
	ownerCtx := ts.CreateUserContext(ctx, owner.ID)
	other, err := ts.CreateRegularUser(ctx, "batch-delete-type-other")
	require.NoError(t, err)

	for range 3 {
		createInboxForServiceTest(ctx, t, ts, owner.ID, store.UNREAD, storepb.InboxMessage_MEMO_COMMENT)
	}
	createInboxForServiceTest(ctx, t, ts, owner.ID, store.UNREAD, storepb.InboxMessage_MEMO_MENTION)
	createInboxForServiceTest(ctx, t, ts, owner.ID, store.ARCHIVED, storepb.InboxMessage_SPACE_INVITATION)
	createInboxForServiceTest(ctx, t, ts, other.ID, store.UNREAD, storepb.InboxMessage_MEMO_COMMENT)

	resp, err := ts.Service.BatchDeleteUserNotifications(ownerCtx, &apipb.BatchDeleteUserNotificationsRequest{
		Parent: fmt.Sprintf("users/%s", owner.Username),
		Scope:  &apipb.UserNotificationBatchScope{Type: apipb.UserNotification_MEMO_COMMENT},
	})
	require.NoError(t, err)
	require.Equal(t, int32(3), resp.AffectedCount)

	ownerID := owner.ID
	inboxes, err := ts.Store.ListInboxes(ctx, &store.FindInbox{ReceiverID: &ownerID})
	require.NoError(t, err)
	require.Len(t, inboxes, 2)
	for _, inbox := range inboxes {
		require.NotEqual(t, storepb.InboxMessage_MEMO_COMMENT, inbox.Message.Type)
	}

	otherID := other.ID
	otherInboxes, err := ts.Store.ListInboxes(ctx, &store.FindInbox{ReceiverID: &otherID})
	require.NoError(t, err)
	require.Len(t, otherInboxes, 1, "another receiver's notifications must survive")
}

func TestBatchUserNotificationsBeforeTimeScope(t *testing.T) {
	ctx := context.Background()
	ts := NewTestService(t)
	defer ts.Cleanup()

	owner, err := ts.CreateRegularUser(ctx, "batch-before-owner")
	require.NoError(t, err)
	ownerCtx := ts.CreateUserContext(ctx, owner.ID)

	base := int64(1_700_000_000)
	older := createInboxForServiceTest(ctx, t, ts, owner.ID, store.UNREAD, storepb.InboxMessage_MEMO_COMMENT)
	middle := createInboxForServiceTest(ctx, t, ts, owner.ID, store.UNREAD, storepb.InboxMessage_MEMO_COMMENT)
	newer := createInboxForServiceTest(ctx, t, ts, owner.ID, store.UNREAD, storepb.InboxMessage_MEMO_MENTION)
	setServiceInboxCreatedTs(ctx, t, ts, older.ID, base)
	setServiceInboxCreatedTs(ctx, t, ts, middle.ID, base+100)
	setServiceInboxCreatedTs(ctx, t, ts, newer.ID, base+200)

	// Delete strictly before base+100: only the oldest.
	cutoff := timestamppb.New(time.Unix(base+100, 0))
	resp, err := ts.Service.BatchDeleteUserNotifications(ownerCtx, &apipb.BatchDeleteUserNotificationsRequest{
		Parent: fmt.Sprintf("users/%s", owner.Username),
		Scope:  &apipb.UserNotificationBatchScope{BeforeTime: cutoff},
	})
	require.NoError(t, err)
	require.Equal(t, int32(1), resp.AffectedCount)

	// Type + before combination: only the middle comment is marked read.
	resp2, err := ts.Service.BatchUpdateUserNotifications(ownerCtx, &apipb.BatchUpdateUserNotificationsRequest{
		Parent: fmt.Sprintf("users/%s", owner.Username),
		Scope: &apipb.UserNotificationBatchScope{
			Type:       apipb.UserNotification_MEMO_COMMENT,
			BeforeTime: timestamppb.New(time.Unix(base+200, 0)),
		},
	})
	require.NoError(t, err)
	require.Equal(t, int32(1), resp2.AffectedCount)

	ownerID := owner.ID
	inboxes, err := ts.Store.ListInboxes(ctx, &store.FindInbox{ReceiverID: &ownerID})
	require.NoError(t, err)
	require.Len(t, inboxes, 2)
	statusByID := map[int32]store.InboxStatus{}
	typeByID := map[int32]storepb.InboxMessage_Type{}
	for _, inbox := range inboxes {
		statusByID[inbox.ID] = inbox.Status
		typeByID[inbox.ID] = inbox.Message.Type
	}
	require.Equal(t, store.ARCHIVED, statusByID[middle.ID])
	require.Equal(t, storepb.InboxMessage_MEMO_COMMENT, typeByID[middle.ID])
	require.Equal(t, store.UNREAD, statusByID[newer.ID])
	require.Equal(t, storepb.InboxMessage_MEMO_MENTION, typeByID[newer.ID])
}

func TestBatchUserNotificationsRejectsOtherUsersAndAnonymous(t *testing.T) {
	ctx := context.Background()
	ts := NewTestService(t)
	defer ts.Cleanup()

	owner, err := ts.CreateRegularUser(ctx, "batch-authz-owner")
	require.NoError(t, err)
	other, err := ts.CreateRegularUser(ctx, "batch-authz-other")
	require.NoError(t, err)
	otherCtx := ts.CreateUserContext(ctx, other.ID)
	parent := fmt.Sprintf("users/%s", owner.Username)

	_, err = ts.Service.BatchUpdateUserNotifications(otherCtx, &apipb.BatchUpdateUserNotificationsRequest{
		Parent: parent,
		Scope:  &apipb.UserNotificationBatchScope{},
	})
	require.Equal(t, codes.PermissionDenied, status.Code(err))

	_, err = ts.Service.BatchDeleteUserNotifications(otherCtx, &apipb.BatchDeleteUserNotificationsRequest{
		Parent: parent,
		Scope:  &apipb.UserNotificationBatchScope{},
	})
	require.Equal(t, codes.PermissionDenied, status.Code(err))

	_, err = ts.Service.BatchUpdateUserNotifications(context.Background(), &apipb.BatchUpdateUserNotificationsRequest{
		Parent: parent,
		Scope:  &apipb.UserNotificationBatchScope{},
	})
	require.Equal(t, codes.Unauthenticated, status.Code(err))

	// An unknown enum value is rejected before any store access.
	ownerCtx := ts.CreateUserContext(ctx, owner.ID)
	_, err = ts.Service.BatchDeleteUserNotifications(ownerCtx, &apipb.BatchDeleteUserNotificationsRequest{
		Parent: parent,
		Scope:  &apipb.UserNotificationBatchScope{Type: apipb.UserNotification_Type(99)},
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}
