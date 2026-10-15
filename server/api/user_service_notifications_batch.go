package api

import (
	"context"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	apipb "github.com/usememos/memos/proto/gen/api"
	storepb "github.com/usememos/memos/proto/gen/store"
	"github.com/usememos/memos/store"
)

// BatchUpdateUserNotifications marks every notification matched by scope as
// read (ARCHIVED). Only the receiver may call it.
func (s *APIService) BatchUpdateUserNotifications(ctx context.Context, request *apipb.BatchUpdateUserNotificationsRequest) (*apipb.BatchUpdateUserNotificationsResponse, error) {
	user, err := s.resolveUserFromName(ctx, request.GetParent())
	if err != nil {
		return nil, err
	}
	currentUser, err := s.fetchCurrentUser(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get current user: %v", err)
	}
	if currentUser == nil {
		return nil, status.Errorf(codes.Unauthenticated, "user not authenticated")
	}
	if currentUser.ID != user.ID {
		return nil, status.Errorf(codes.PermissionDenied, "permission denied")
	}

	find, err := buildInboxBatchFilter(user.ID, request.GetScope())
	if err != nil {
		return nil, err
	}
	// Only UNREAD rows can be marked as read, which both matches the semantics
	// and keeps affected rows equal to actually changed rows.
	find.Status = &[]store.InboxStatus{store.UNREAD}[0]

	affected, lastID, err := s.Store.BatchUpdateInboxStatus(ctx, find, store.ARCHIVED)
	if err != nil {
		slog.Error("failed to batch mark notifications as read",
			slog.Any("error", err),
			slog.Int64("receiver_id", int64(user.ID)),
			slog.Int64("affected", int64(affected)),
			slog.Int64("last_processed_id", int64(lastID)))
		return nil, status.Errorf(codes.Internal, "failed to batch update notifications: %v", err)
	}
	return &apipb.BatchUpdateUserNotificationsResponse{AffectedCount: affected}, nil
}

// BatchDeleteUserNotifications permanently deletes every notification matched
// by scope regardless of status. Only the receiver may call it.
func (s *APIService) BatchDeleteUserNotifications(ctx context.Context, request *apipb.BatchDeleteUserNotificationsRequest) (*apipb.BatchDeleteUserNotificationsResponse, error) {
	user, err := s.resolveUserFromName(ctx, request.GetParent())
	if err != nil {
		return nil, err
	}
	currentUser, err := s.fetchCurrentUser(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get current user: %v", err)
	}
	if currentUser == nil {
		return nil, status.Errorf(codes.Unauthenticated, "user not authenticated")
	}
	if currentUser.ID != user.ID {
		return nil, status.Errorf(codes.PermissionDenied, "permission denied")
	}

	find, err := buildInboxBatchFilter(user.ID, request.GetScope())
	if err != nil {
		return nil, err
	}

	affected, lastID, err := s.Store.BatchDeleteInboxes(ctx, find)
	if err != nil {
		slog.Error("failed to batch delete notifications",
			slog.Any("error", err),
			slog.Int64("receiver_id", int64(user.ID)),
			slog.Int64("affected", int64(affected)),
			slog.Int64("last_processed_id", int64(lastID)))
		return nil, status.Errorf(codes.Internal, "failed to batch delete notifications: %v", err)
	}
	return &apipb.BatchDeleteUserNotificationsResponse{AffectedCount: affected}, nil
}

// buildInboxBatchFilter converts a notification batch scope into an inbox
// store filter scoped to the receiver. An empty scope matches every
// notification of the user.
func buildInboxBatchFilter(receiverID int32, scope *apipb.UserNotificationBatchScope) (*store.FindInbox, error) {
	find := &store.FindInbox{ReceiverID: &receiverID}
	if scope == nil {
		return find, nil
	}
	switch scope.GetType() {
	case apipb.UserNotification_TYPE_UNSPECIFIED:
		// All types.
	case apipb.UserNotification_MEMO_COMMENT:
		find.MessageType = &[]storepb.InboxMessage_Type{storepb.InboxMessage_MEMO_COMMENT}[0]
	case apipb.UserNotification_MEMO_MENTION:
		find.MessageType = &[]storepb.InboxMessage_Type{storepb.InboxMessage_MEMO_MENTION}[0]
	case apipb.UserNotification_SPACE_INVITATION:
		find.MessageType = &[]storepb.InboxMessage_Type{storepb.InboxMessage_SPACE_INVITATION}[0]
	default:
		return nil, status.Errorf(codes.InvalidArgument, "invalid notification type: %v", scope.GetType())
	}
	if before := scope.GetBeforeTime(); before != nil {
		if !before.IsValid() {
			return nil, status.Errorf(codes.InvalidArgument, "invalid before_time")
		}
		beforeTs := before.AsTime().Unix()
		find.CreatedTsBefore = &beforeTs
	}
	return find, nil
}
