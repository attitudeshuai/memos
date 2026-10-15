package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	corenotification "github.com/usememos/memos/core/notification"
	apipb "github.com/usememos/memos/proto/gen/api"
	storepb "github.com/usememos/memos/proto/gen/store"
	"github.com/usememos/memos/store"
)

func TestRunNotificationCleanupRequiresAdmin(t *testing.T) {
	ctx := context.Background()
	ts := NewTestService(t)
	defer ts.Cleanup()

	regular, err := ts.CreateRegularUser(ctx, "cleanup-regular")
	require.NoError(t, err)
	regularCtx := ts.CreateUserContext(ctx, regular.ID)

	_, err = ts.Service.RunNotificationCleanup(regularCtx, &emptypb.Empty{})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	_, err = ts.Service.RunNotificationCleanup(context.Background(), &emptypb.Empty{})
	require.Equal(t, codes.Unauthenticated, status.Code(err))

	_, err = ts.Service.ListNotificationCleanupRuns(regularCtx, nil)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
}

func TestRunNotificationCleanupManualWithoutPolicy(t *testing.T) {
	ctx := context.Background()
	ts := NewTestService(t)
	defer ts.Cleanup()

	admin, err := ts.CreateHostUser(ctx, "cleanup-admin")
	require.NoError(t, err)
	adminCtx := ts.CreateUserContext(ctx, admin.ID)

	run, err := ts.Service.RunNotificationCleanup(adminCtx, &emptypb.Empty{})
	require.NoError(t, err)
	require.Equal(t, apipb.NotificationCleanupRunInfo_SUCCEEDED, run.State)
	require.Equal(t, apipb.NotificationCleanupRunInfo_MANUAL, run.Trigger)
	require.Equal(t, int32(0), run.DeletedAgeCount)
	require.Equal(t, int32(0), run.DeletedCapCount)
	require.Equal(t, "notificationCleanupRuns/", run.Name[:len("notificationCleanupRuns/")])
	require.NotNil(t, run.CreateTime)
	require.NotNil(t, run.CompleteTime)

	list, err := ts.Service.ListNotificationCleanupRuns(adminCtx, nil)
	require.NoError(t, err)
	require.Len(t, list.CleanupRuns, 1)
	require.Equal(t, run.Name, list.CleanupRuns[0].Name)
}

func TestRunNotificationCleanupAppliesAgePolicy(t *testing.T) {
	ctx := context.Background()
	ts := NewTestService(t)
	defer ts.Cleanup()

	admin, err := ts.CreateHostUser(ctx, "cleanup-admin-age")
	require.NoError(t, err)
	adminCtx := ts.CreateUserContext(ctx, admin.ID)

	old := createInboxForServiceTest(ctx, t, ts, admin.ID, store.UNREAD, storepb.InboxMessage_MEMO_COMMENT)
	fresh := createInboxForServiceTest(ctx, t, ts, admin.ID, store.UNREAD, storepb.InboxMessage_MEMO_MENTION)
	setServiceInboxCreatedTs(ctx, t, ts, old.ID, 1_000_000_000)

	_, err = ts.Store.UpsertInstanceSetting(ctx, &storepb.InstanceSetting{
		Key: storepb.InstanceSettingKey_NOTIFICATION,
		Value: &storepb.InstanceSetting_NotificationSetting{
			NotificationSetting: &storepb.InstanceNotificationSetting{
				Retention: &storepb.InstanceNotificationSetting_RetentionSetting{MaxAgeDays: 30},
			},
		},
	})
	require.NoError(t, err)

	run, err := ts.Service.RunNotificationCleanup(adminCtx, &emptypb.Empty{})
	require.NoError(t, err)
	require.Equal(t, int32(1), run.DeletedAgeCount)
	require.Equal(t, int32(0), run.DeletedCapCount)
	require.NotNil(t, run.AgeCutoffTime)
	require.Equal(t, int32(30), run.MaxAgeDays)

	adminID := admin.ID
	inboxes, err := ts.Store.ListInboxes(ctx, &store.FindInbox{ReceiverID: &adminID})
	require.NoError(t, err)
	require.Len(t, inboxes, 1)
	require.Equal(t, fresh.ID, inboxes[0].ID)

	list, err := ts.Service.ListNotificationCleanupRuns(adminCtx, nil)
	require.NoError(t, err)
	require.Len(t, list.CleanupRuns, 1, "manual no-policy runs happened on other test services, not this DB")
	require.Equal(t, int32(1), list.CleanupRuns[0].DeletedAgeCount)
}

func TestRunNotificationCleanupRejectsConcurrentRun(t *testing.T) {
	ctx := context.Background()
	ts := NewTestService(t)
	defer ts.Cleanup()

	admin, err := ts.CreateHostUser(ctx, "cleanup-admin-concurrent")
	require.NoError(t, err)
	adminCtx := ts.CreateUserContext(ctx, admin.ID)

	entered := make(chan struct{})
	release := make(chan struct{})
	ts.Service.NotificationRetention = corenotification.NewRetentionSweeper(ts.Store, corenotification.WithAfterWatermark(func(_ int32) {
		close(entered)
		<-release
	}))

	done := make(chan error, 1)
	go func() {
		_, runErr := ts.Service.RunNotificationCleanup(adminCtx, &emptypb.Empty{})
		done <- runErr
	}()
	<-entered

	_, err = ts.Service.RunNotificationCleanup(adminCtx, &emptypb.Empty{})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	close(release)
	require.NoError(t, <-done)
}
