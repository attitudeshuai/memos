package api

import (
	"context"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/usememos/memos/core/audit"
	"github.com/usememos/memos/internal/clientip"
	apipb "github.com/usememos/memos/proto/gen/api"
	storepb "github.com/usememos/memos/proto/gen/store"
	"github.com/usememos/memos/store"
)

// newAuditLog builds a persistable audit entry for an acting administrator,
// recording the resolved client IP alongside the change.
func newAuditLog(ctx context.Context, actor *store.User, resourceType storepb.AuditLog_ResourceType, action storepb.AuditLog_Action, resourceID string, changes []audit.FieldChange) (*store.AuditLog, error) {
	entry := audit.NewEntry(actor.ID, actor.Username, resourceType, action, resourceID, changes, clientip.FromContext(ctx))
	return entry.ToStore()
}

// ListAuditLogs lists administrative change entries, newest first. Admin only.
func (s *APIService) ListAuditLogs(ctx context.Context, request *apipb.ListAuditLogsRequest) (*apipb.ListAuditLogsResponse, error) {
	if _, err := s.requireAuditAdmin(ctx); err != nil {
		return nil, err
	}

	find := &store.FindAuditLog{}
	if request.ResourceType != apipb.AuditResourceType_AUDIT_RESOURCE_TYPE_UNSPECIFIED {
		resourceType := convertAuditResourceTypeToStore(request.ResourceType)
		find.ResourceType = &resourceType
	}
	if request.Action != apipb.AuditAction_AUDIT_ACTION_UNSPECIFIED {
		action := convertAuditActionToStore(request.Action)
		find.Action = &action
	}
	if request.Resource != "" {
		resourceID, err := auditResourceIDFromName(request.ResourceType, request.Resource)
		if err != nil {
			return nil, err
		}
		find.ResourceID = resourceID
	}
	if request.Actor != "" {
		username, err := parseUsernameFromName(request.Actor)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid actor: %v", err)
		}
		user, err := s.Store.GetUser(ctx, &store.FindUser{Username: &username})
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to get actor: %v", err)
		}
		if user == nil {
			return nil, status.Errorf(codes.NotFound, "actor not found")
		}
		find.ActorID = &user.ID
	}

	var limit, offset int
	if request.PageToken != "" {
		var pageToken apipb.PageToken
		if err := unmarshalPageToken(request.PageToken, &pageToken); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid page token: %v", err)
		}
		limit = normalizePageSize(pageToken.Limit)
		offset = max(int(pageToken.Offset), 0)
	} else {
		limit = normalizePageSize(request.PageSize)
	}
	find.Limit = &limit
	find.Offset = &offset

	rows, err := s.Store.ListAuditLogs(ctx, find)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list audit logs: %v", err)
	}
	response := &apipb.ListAuditLogsResponse{AuditLogs: make([]*apipb.AuditLog, 0, len(rows))}
	for _, row := range rows {
		response.AuditLogs = append(response.AuditLogs, convertAuditLogFromStore(row))
	}
	if len(rows) == limit {
		nextPageToken, err := getPageToken(limit, offset+limit)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to get next page token: %v", err)
		}
		response.NextPageToken = nextPageToken
	}
	return response, nil
}

// GetAuditLogRetention gets the audit ledger retention policy. Admin only.
func (s *APIService) GetAuditLogRetention(ctx context.Context, _ *apipb.GetAuditLogRetentionRequest) (*apipb.AuditLogRetention, error) {
	if _, err := s.requireAuditAdmin(ctx); err != nil {
		return nil, err
	}
	retention, err := s.Store.GetAuditRetention(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get audit retention: %v", err)
	}
	return convertAuditRetentionFromStore(retention), nil
}

// UpdateAuditLogRetention declares the audit ledger retention policy. Admin only.
func (s *APIService) UpdateAuditLogRetention(ctx context.Context, request *apipb.UpdateAuditLogRetentionRequest) (*apipb.AuditLogRetention, error) {
	if _, err := s.requireAuditAdmin(ctx); err != nil {
		return nil, err
	}
	if request.Retention == nil {
		return nil, status.Errorf(codes.InvalidArgument, "retention is required")
	}
	retention := store.AuditRetention{
		MaxRecords: request.Retention.MaxRecords,
		RetainDays: request.Retention.RetainDays,
	}
	if err := retention.Validate(); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid audit retention: %v", err)
	}
	if err := s.Store.UpdateAuditRetention(ctx, retention); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to update audit retention: %v", err)
	}
	return convertAuditRetentionFromStore(retention), nil
}

func (s *APIService) requireAuditAdmin(ctx context.Context) (*store.User, error) {
	user, err := s.fetchCurrentUser(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get current user: %v", err)
	}
	if user == nil {
		return nil, status.Errorf(codes.Unauthenticated, "user not authenticated")
	}
	if user.Role != store.RoleAdmin {
		return nil, status.Errorf(codes.PermissionDenied, "permission denied")
	}
	return user, nil
}

func convertAuditLogFromStore(row *store.AuditLog) *apipb.AuditLog {
	changes := audit.DecodeChanges(row.Changes)
	result := &apipb.AuditLog{
		Name:         auditLogName(row.ID),
		Actor:        UserNamePrefix + row.ActorName,
		CreateTime:   timestamppb.New(time.Unix(row.CreatedTs, 0)),
		ResourceType: convertAuditResourceTypeFromStore(row.ResourceType),
		Resource:     auditResourceName(row.ResourceType, row.ResourceID),
		Action:       convertAuditActionFromStore(row.Action),
		ClientIp:     row.ClientIP,
	}
	result.Changes = make([]*apipb.AuditFieldChange, 0, len(changes))
	for _, change := range changes {
		result.Changes = append(result.Changes, &apipb.AuditFieldChange{
			Field:  change.Field,
			Before: change.Before,
			After:  change.After,
		})
	}
	return result
}

func auditLogName(id int64) string {
	return "audit-logs/" + strconv.FormatInt(id, 10)
}

func auditResourceName(resourceType storepb.AuditLog_ResourceType, resourceID string) string {
	switch resourceType {
	case storepb.AuditLog_INSTANCE_SETTING:
		return InstanceSettingNamePrefix + resourceID
	case storepb.AuditLog_IDENTITY_PROVIDER:
		return IdentityProviderNamePrefix + resourceID
	case storepb.AuditLog_USAGE_LIMIT:
		return UsageLimitNamePrefix + resourceID
	case storepb.AuditLog_USER:
		return UserNamePrefix + resourceID
	default:
		return resourceID
	}
}

// auditResourceIDFromName reverses auditResourceName, requiring the name's
// prefix to match the requested type.
func auditResourceIDFromName(resourceType apipb.AuditResourceType, name string) (string, error) {
	prefixes := map[storepb.AuditLog_ResourceType]string{
		storepb.AuditLog_INSTANCE_SETTING:  InstanceSettingNamePrefix,
		storepb.AuditLog_IDENTITY_PROVIDER: IdentityProviderNamePrefix,
		storepb.AuditLog_USAGE_LIMIT:       UsageLimitNamePrefix,
		storepb.AuditLog_USER:              UserNamePrefix,
	}
	if resourceType != apipb.AuditResourceType_AUDIT_RESOURCE_TYPE_UNSPECIFIED {
		storeType := convertAuditResourceTypeToStore(resourceType)
		prefix := prefixes[storeType]
		if !strings.HasPrefix(name, prefix) {
			return "", status.Errorf(codes.InvalidArgument, "resource must have prefix %q", prefix)
		}
		return strings.TrimPrefix(name, prefix), nil
	}
	for _, prefix := range prefixes {
		if after, ok := strings.CutPrefix(name, prefix); ok {
			return after, nil
		}
	}
	return "", status.Errorf(codes.InvalidArgument, "resource must be a full resource name, such as %sACCESS", InstanceSettingNamePrefix)
}

func convertAuditResourceTypeFromStore(value storepb.AuditLog_ResourceType) apipb.AuditResourceType {
	switch value {
	case storepb.AuditLog_INSTANCE_SETTING:
		return apipb.AuditResourceType_AUDIT_RESOURCE_INSTANCE_SETTING
	case storepb.AuditLog_IDENTITY_PROVIDER:
		return apipb.AuditResourceType_AUDIT_RESOURCE_IDENTITY_PROVIDER
	case storepb.AuditLog_USAGE_LIMIT:
		return apipb.AuditResourceType_AUDIT_RESOURCE_USAGE_LIMIT
	case storepb.AuditLog_USER:
		return apipb.AuditResourceType_AUDIT_RESOURCE_USER
	default:
		return apipb.AuditResourceType_AUDIT_RESOURCE_TYPE_UNSPECIFIED
	}
}

func convertAuditResourceTypeToStore(value apipb.AuditResourceType) storepb.AuditLog_ResourceType {
	switch value {
	case apipb.AuditResourceType_AUDIT_RESOURCE_INSTANCE_SETTING:
		return storepb.AuditLog_INSTANCE_SETTING
	case apipb.AuditResourceType_AUDIT_RESOURCE_IDENTITY_PROVIDER:
		return storepb.AuditLog_IDENTITY_PROVIDER
	case apipb.AuditResourceType_AUDIT_RESOURCE_USAGE_LIMIT:
		return storepb.AuditLog_USAGE_LIMIT
	case apipb.AuditResourceType_AUDIT_RESOURCE_USER:
		return storepb.AuditLog_USER
	default:
		return storepb.AuditLog_RESOURCE_TYPE_UNSPECIFIED
	}
}

func convertAuditActionFromStore(value storepb.AuditLog_Action) apipb.AuditAction {
	switch value {
	case storepb.AuditLog_CREATE:
		return apipb.AuditAction_AUDIT_ACTION_CREATE
	case storepb.AuditLog_UPDATE:
		return apipb.AuditAction_AUDIT_ACTION_UPDATE
	case storepb.AuditLog_DELETE:
		return apipb.AuditAction_AUDIT_ACTION_DELETE
	default:
		return apipb.AuditAction_AUDIT_ACTION_UNSPECIFIED
	}
}

func convertAuditActionToStore(value apipb.AuditAction) storepb.AuditLog_Action {
	switch value {
	case apipb.AuditAction_AUDIT_ACTION_CREATE:
		return storepb.AuditLog_CREATE
	case apipb.AuditAction_AUDIT_ACTION_UPDATE:
		return storepb.AuditLog_UPDATE
	case apipb.AuditAction_AUDIT_ACTION_DELETE:
		return storepb.AuditLog_DELETE
	default:
		return storepb.AuditLog_ACTION_UNSPECIFIED
	}
}

func convertAuditRetentionFromStore(retention store.AuditRetention) *apipb.AuditLogRetention {
	return &apipb.AuditLogRetention{
		MaxRecords: retention.MaxRecords,
		RetainDays: retention.RetainDays,
	}
}
