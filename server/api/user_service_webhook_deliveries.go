package api

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	apipb "github.com/usememos/memos/proto/gen/api"
	storepb "github.com/usememos/memos/proto/gen/store"
	"github.com/usememos/memos/store"
)

const (
	webhookDeliveriesDefaultPageSize = 50
	webhookDeliveriesMaxPageSize     = 200
	webhookDeliveriesNamePrefix      = "deliveries/"
)

// ListUserWebhookDeliveries lists the lifecycle delivery records of one
// webhook, newest first. Only the webhook owner and instance admins may call
// it.
func (s *APIService) ListUserWebhookDeliveries(ctx context.Context, request *apipb.ListUserWebhookDeliveriesRequest) (*apipb.ListUserWebhookDeliveriesResponse, error) {
	user, webhookID, err := s.resolveWebhookParent(ctx, request.GetParent())
	if err != nil {
		return nil, err
	}
	if _, err := s.authorizeUserResourceAccess(ctx, user.ID, true); err != nil {
		return nil, err
	}

	currentUser, err := s.fetchCurrentUser(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get current user: %v", err)
	}
	if currentUser == nil {
		return nil, status.Errorf(codes.Unauthenticated, "user not authenticated")
	}
	if currentUser.Role != store.RoleAdmin {
		if _, err := findUserWebhookByID(ctx, s, user.ID, webhookID); err != nil {
			return nil, err
		}
	}

	limit, offset, err := parseWebhookDeliveryPage(request.GetPageSize(), request.GetPageToken())
	if err != nil {
		return nil, err
	}

	find := &store.FindWebhookDeliveries{
		UserID:    &user.ID,
		WebhookID: &webhookID,
		Limit:     &limit,
		Offset:    &offset,
	}
	if state := convertWebhookDeliveryStateToStore(request.GetState()); state != "" {
		find.Status = &state
	}
	rows, err := s.Store.ListWebhookDeliveries(ctx, find)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list webhook deliveries: %v", err)
	}

	response := &apipb.ListUserWebhookDeliveriesResponse{
		Deliveries: make([]*apipb.UserWebhookDelivery, 0, len(rows)),
	}
	for _, row := range rows {
		response.Deliveries = append(response.Deliveries, convertWebhookDeliveryFromStore(user.Username, row))
	}
	// A full page means another page may exist; hand back the next offset.
	if len(rows) == limit {
		response.NextPageToken = encodeWebhookDeliveryPageToken(offset + limit)
	}
	return response, nil
}

// GetUserWebhookDelivery gets one delivery record. Only the webhook owner and
// instance admins may call it.
func (s *APIService) GetUserWebhookDelivery(ctx context.Context, request *apipb.GetUserWebhookDeliveryRequest) (*apipb.UserWebhookDelivery, error) {
	user, _, _, row, err := s.resolveAuthorizedDelivery(ctx, request.GetName())
	if err != nil {
		return nil, err
	}
	return convertWebhookDeliveryFromStore(user.Username, row), nil
}

// ReplayUserWebhookDelivery requeues an abandoned delivery. Succeeded and
// in-flight deliveries cannot be replayed.
func (s *APIService) ReplayUserWebhookDelivery(ctx context.Context, request *apipb.ReplayUserWebhookDeliveryRequest) (*apipb.UserWebhookDelivery, error) {
	user, _, _, row, err := s.resolveAuthorizedDelivery(ctx, request.GetName())
	if err != nil {
		return nil, err
	}
	switch row.Status {
	case store.WebhookDeliveryAbandoned:
		// Requeue below.
	case store.WebhookDeliverySucceeded:
		return nil, status.Errorf(codes.FailedPrecondition, "delivery already succeeded; replaying it would duplicate the delivery")
	default:
		return nil, status.Errorf(codes.FailedPrecondition, "delivery is %s and cannot be replayed yet", row.Status)
	}

	replayed, err := s.Store.ReplayWebhookDelivery(ctx, row.ID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to replay webhook delivery: %v", err)
	}
	if replayed == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "delivery is no longer abandoned")
	}
	s.WebhookDeliveries.Wake()
	return convertWebhookDeliveryFromStore(user.Username, replayed), nil
}

func (s *APIService) resolveWebhookParent(ctx context.Context, parent string) (*store.User, string, error) {
	tokens, err := GetNameParentTokens(parent, UserNamePrefix, WebhookNamePrefix)
	if err != nil {
		return nil, "", status.Errorf(codes.InvalidArgument, "invalid webhook parent %q", parent)
	}
	return s.resolveWebhookOwnership(ctx, tokens[0], tokens[1])
}

// resolveAuthorizedDelivery parses a delivery name, authorizes the caller as
// owner or admin, and loads the record. Deleted-webhook records stay readable
// for admins.
func (s *APIService) resolveAuthorizedDelivery(ctx context.Context, name string) (*store.User, string, int64, *store.WebhookDelivery, error) {
	tokens, err := GetNameParentTokens(name, UserNamePrefix, WebhookNamePrefix, webhookDeliveriesNamePrefix)
	if err != nil {
		return nil, "", 0, nil, status.Errorf(codes.InvalidArgument, "invalid webhook delivery name %q", name)
	}
	deliveryID, err := strconv.ParseInt(tokens[2], 10, 64)
	if err != nil || deliveryID <= 0 {
		return nil, "", 0, nil, status.Errorf(codes.InvalidArgument, "invalid webhook delivery ID %q", tokens[2])
	}
	user, webhookID, err := s.resolveWebhookOwnership(ctx, tokens[0], tokens[1])
	if err != nil {
		return nil, "", 0, nil, err
	}
	if _, err := s.authorizeUserResourceAccess(ctx, user.ID, true); err != nil {
		return nil, "", 0, nil, err
	}
	currentUser, err := s.fetchCurrentUser(ctx)
	if err != nil {
		return nil, "", 0, nil, status.Errorf(codes.Internal, "failed to get current user: %v", err)
	}
	if currentUser == nil {
		return nil, "", 0, nil, status.Errorf(codes.Unauthenticated, "user not authenticated")
	}
	if currentUser.Role != store.RoleAdmin {
		if _, err := findUserWebhookByID(ctx, s, user.ID, webhookID); err != nil {
			return nil, "", 0, nil, err
		}
	}

	rows, err := s.Store.ListWebhookDeliveries(ctx, &store.FindWebhookDeliveries{
		ID: &deliveryID, UserID: &user.ID, WebhookID: &webhookID,
	})
	if err != nil {
		return nil, "", 0, nil, status.Errorf(codes.Internal, "failed to get webhook delivery: %v", err)
	}
	if len(rows) == 0 {
		return nil, "", 0, nil, status.Errorf(codes.NotFound, "webhook delivery not found: %s", name)
	}
	return user, webhookID, deliveryID, rows[0], nil
}

func (s *APIService) resolveWebhookOwnership(ctx context.Context, username, webhookID string) (*store.User, string, error) {
	user, err := ResolveUserByName(ctx, s.Store, BuildUserName(username))
	if user == nil || err != nil {
		return nil, "", status.Errorf(codes.NotFound, "webhook owner not found")
	}
	return user, webhookID, nil
}

func findUserWebhookByID(ctx context.Context, s *APIService, userID int32, webhookID string) (*storepb.WebhooksUserSetting_Webhook, error) {
	webhooks, err := s.Store.GetUserWebhooks(ctx, userID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get user webhooks: %v", err)
	}
	for _, hook := range webhooks {
		if hook.Id == webhookID {
			return hook, nil
		}
	}
	return nil, status.Errorf(codes.NotFound, "webhook not found: %s%s", WebhookNamePrefix, webhookID)
}

func parseWebhookDeliveryPage(pageSize int32, pageToken string) (int, int, error) {
	limit := webhookDeliveriesDefaultPageSize
	if pageSize > 0 {
		limit = int(pageSize)
	}
	if pageSize < 0 || limit > webhookDeliveriesMaxPageSize {
		return 0, 0, status.Errorf(codes.InvalidArgument, "page_size must be between 1 and %d", webhookDeliveriesMaxPageSize)
	}
	offset := 0
	if pageToken != "" {
		decoded, err := base64.RawURLEncoding.DecodeString(pageToken)
		if err != nil {
			return 0, 0, status.Errorf(codes.InvalidArgument, "invalid page token")
		}
		parsed, err := strconv.Atoi(string(decoded))
		if err != nil || parsed < 0 {
			return 0, 0, status.Errorf(codes.InvalidArgument, "invalid page token")
		}
		offset = parsed
	}
	return limit, offset, nil
}

func encodeWebhookDeliveryPageToken(offset int) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.Itoa(offset)))
}

func convertWebhookDeliveryFromStore(username string, row *store.WebhookDelivery) *apipb.UserWebhookDelivery {
	return &apipb.UserWebhookDelivery{
		Name:            fmt.Sprintf("%s%s/%s%s/%s%d", UserNamePrefix, username, WebhookNamePrefix, row.WebhookID, webhookDeliveriesNamePrefix, row.ID),
		State:           convertWebhookDeliveryStateFromStore(row.Status),
		ActivityType:    row.Label,
		Url:             row.URL,
		Attempts:        row.Attempts,
		MaxAttempts:     row.MaxAttempts,
		CreateTime:      unixMilliProto(row.CreatedTs),
		UpdateTime:      unixMilliProto(row.UpdatedTs),
		NextAttemptTime: unixMilliProto(row.NextDueTs),
		LastAttemptTime: unixMilliProto(row.LastAttemptTs),
		SettleTime:      unixMilliProto(row.SettledTs),
		LastError:       row.LastError,
	}
}

func unixMilliProto(ts int64) *timestamppb.Timestamp {
	if ts == 0 {
		return nil
	}
	return timestamppb.New(time.UnixMilli(ts))
}

func convertWebhookDeliveryStateFromStore(state store.WebhookDeliveryStatus) apipb.WebhookDeliveryState {
	switch state {
	case store.WebhookDeliveryPending:
		return apipb.WebhookDeliveryState_PENDING
	case store.WebhookDeliveryDelivering:
		return apipb.WebhookDeliveryState_DELIVERING
	case store.WebhookDeliverySucceeded:
		return apipb.WebhookDeliveryState_SUCCEEDED
	case store.WebhookDeliveryRetrying:
		return apipb.WebhookDeliveryState_RETRYING
	case store.WebhookDeliveryAbandoned:
		return apipb.WebhookDeliveryState_ABANDONED
	default:
		return apipb.WebhookDeliveryState_WEBHOOK_DELIVERY_STATE_UNSPECIFIED
	}
}

func convertWebhookDeliveryStateToStore(state apipb.WebhookDeliveryState) store.WebhookDeliveryStatus {
	switch state {
	case apipb.WebhookDeliveryState_PENDING:
		return store.WebhookDeliveryPending
	case apipb.WebhookDeliveryState_DELIVERING:
		return store.WebhookDeliveryDelivering
	case apipb.WebhookDeliveryState_SUCCEEDED:
		return store.WebhookDeliverySucceeded
	case apipb.WebhookDeliveryState_RETRYING:
		return store.WebhookDeliveryRetrying
	case apipb.WebhookDeliveryState_ABANDONED:
		return store.WebhookDeliveryAbandoned
	default:
		return ""
	}
}
