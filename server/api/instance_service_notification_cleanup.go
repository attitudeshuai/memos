package api

import (
	"context"
	"strconv"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/pkg/errors"

	"github.com/usememos/memos/core/notification"
	apipb "github.com/usememos/memos/proto/gen/api"
	"github.com/usememos/memos/store"
)

// notificationCleanupRunHistoryLimit bounds how many audit rows the API
// returns; the store prunes the table to the newest 100.
const notificationCleanupRunHistoryLimit = 100

// RunNotificationCleanup applies the notification retention policy once and
// returns the run record. Admin only.
func (s *APIService) RunNotificationCleanup(ctx context.Context, _ *emptypb.Empty) (*apipb.NotificationCleanupRunInfo, error) {
	if err := s.requireAdmin(ctx); err != nil {
		return nil, err
	}

	run, err := s.NotificationRetention.Sweep(ctx, store.CleanupTriggerManual)
	if err != nil {
		if errors.Is(err, notification.ErrCleanupAlreadyRunning) {
			return nil, status.Errorf(codes.FailedPrecondition, "notification cleanup is already running")
		}
		return nil, status.Errorf(codes.Internal, "failed to run notification cleanup: %v", err)
	}
	return convertNotificationCleanupRunToAPI(run), nil
}

// ListNotificationCleanupRuns lists recent notification cleanup runs newest
// first. Admin only.
func (s *APIService) ListNotificationCleanupRuns(ctx context.Context, _ *apipb.ListNotificationCleanupRunsRequest) (*apipb.ListNotificationCleanupRunsResponse, error) {
	if err := s.requireAdmin(ctx); err != nil {
		return nil, err
	}

	runs, err := s.Store.ListNotificationCleanupRuns(ctx, notificationCleanupRunHistoryLimit)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list notification cleanup runs: %v", err)
	}
	infos := make([]*apipb.NotificationCleanupRunInfo, 0, len(runs))
	for _, run := range runs {
		infos = append(infos, convertNotificationCleanupRunToAPI(run))
	}
	return &apipb.ListNotificationCleanupRunsResponse{CleanupRuns: infos}, nil
}

// requireAdmin verifies the caller is authenticated and has the admin role.
func (s *APIService) requireAdmin(ctx context.Context) error {
	user, err := s.fetchCurrentUser(ctx)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to get current user: %v", err)
	}
	if user == nil {
		return status.Errorf(codes.Unauthenticated, "user not authenticated")
	}
	if user.Role != store.RoleAdmin {
		return status.Errorf(codes.PermissionDenied, "permission denied")
	}
	return nil
}

func convertNotificationCleanupRunToAPI(run *store.NotificationCleanupRun) *apipb.NotificationCleanupRunInfo {
	info := &apipb.NotificationCleanupRunInfo{
		Name:              notificationCleanupRunName(run.ID),
		CreateTime:        timestamppb.New(time.Unix(run.StartedTs, 0)),
		Trigger:           convertNotificationCleanupTrigger(run.Trigger),
		State:             convertNotificationCleanupState(run.Status),
		WatermarkInboxId:  run.WatermarkInboxID,
		MaxAgeDays:        run.MaxAgeDays,
		MaxPerUser:        run.MaxPerUser,
		DeletedAgeCount:   run.DeletedAgeCount,
		DeletedCapCount:   run.DeletedCapCount,
		ErrorDetail:       run.ErrorDetail,
	}
	if run.CompleteTs > 0 {
		info.CompleteTime = timestamppb.New(time.Unix(run.CompleteTs, 0))
	}
	if run.AgeCutoffTs > 0 {
		info.AgeCutoffTime = timestamppb.New(time.Unix(run.AgeCutoffTs, 0))
	}
	return info
}

func notificationCleanupRunName(id int32) string {
	return "notificationCleanupRuns/" + strconv.FormatInt(int64(id), 10)
}

func convertNotificationCleanupTrigger(trigger store.NotificationCleanupTrigger) apipb.NotificationCleanupRunInfo_Trigger {
	switch trigger {
	case store.CleanupTriggerScheduled:
		return apipb.NotificationCleanupRunInfo_SCHEDULED
	case store.CleanupTriggerManual:
		return apipb.NotificationCleanupRunInfo_MANUAL
	default:
		return apipb.NotificationCleanupRunInfo_TRIGGER_UNSPECIFIED
	}
}

func convertNotificationCleanupState(status store.NotificationCleanupStatus) apipb.NotificationCleanupRunInfo_State {
	switch status {
	case store.CleanupStatusRunning:
		return apipb.NotificationCleanupRunInfo_RUNNING
	case store.CleanupStatusSucceeded:
		return apipb.NotificationCleanupRunInfo_SUCCEEDED
	case store.CleanupStatusFailed:
		return apipb.NotificationCleanupRunInfo_FAILED
	default:
		return apipb.NotificationCleanupRunInfo_STATE_UNSPECIFIED
	}
}
