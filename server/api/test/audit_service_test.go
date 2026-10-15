package test

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	apipb "github.com/usememos/memos/proto/gen/api"
)

func minimalIDP(title string) *apipb.IdentityProvider {
	return &apipb.IdentityProvider{
		Title: title,
		Type:  apipb.IdentityProvider_OAUTH2,
		Config: &apipb.IdentityProviderConfig{
			Config: &apipb.IdentityProviderConfig_Oauth2Config{
				Oauth2Config: &apipb.OAuth2Config{
					ClientId:     "c",
					AuthUrl:      "a",
					TokenUrl:     "t",
					UserInfoUrl:  "u",
					FieldMapping: &apipb.FieldMapping{Identifier: "sub"},
				},
			},
		},
	}
}

func listAllAuditLogs(ctx context.Context, t *testing.T, ts *TestService) []*apipb.AuditLog {
	t.Helper()
	resp, err := ts.Service.ListAuditLogs(ctx, &apipb.ListAuditLogsRequest{PageSize: 1000})
	require.NoError(t, err)
	require.Empty(t, resp.NextPageToken, "test should produce fewer than one page of audit logs")
	return resp.AuditLogs
}

func findAuditChange(log *apipb.AuditLog, field string) (*apipb.AuditFieldChange, bool) {
	for _, change := range log.Changes {
		if change.Field == field {
			return change, true
		}
	}
	return nil, false
}

func TestAuditLedgerRecordsAdministrativeChanges(t *testing.T) {
	ctx := context.Background()
	ts := NewTestService(t)
	defer ts.Cleanup()

	admin, err := ts.CreateHostUser(ctx, "root")
	require.NoError(t, err)
	adminCtx := ts.CreateUserContext(ctx, admin.ID)
	bob, err := ts.CreateRegularUser(ctx, "bob")
	require.NoError(t, err)
	bobCtx := ts.CreateUserContext(ctx, bob.ID)

	// Instance setting: access policy changes are recorded with values on both sides.
	_, err = ts.Service.UpdateInstanceSetting(adminCtx, &apipb.UpdateInstanceSettingRequest{
		InstanceSetting: &apipb.InstanceSetting{
			Name: "instance/settings/ACCESS",
			Value: &apipb.InstanceSetting_AccessSetting_{
				AccessSetting: &apipb.InstanceSetting_AccessSetting{AccessMode: apipb.InstanceAccessMode_INSTANCE_ACCESS_MODE_PRIVATE},
			},
		},
	})
	require.NoError(t, err)

	// Notification setting: SMTP password must never reach the ledger in clear text.
	const smtpSecret = "smtp-cleartext-secret"
	_, err = ts.Service.UpdateInstanceSetting(adminCtx, &apipb.UpdateInstanceSettingRequest{
		InstanceSetting: &apipb.InstanceSetting{
			Name: "instance/settings/NOTIFICATION",
			Value: &apipb.InstanceSetting_NotificationSetting_{
				NotificationSetting: &apipb.InstanceSetting_NotificationSetting{
					Email: &apipb.InstanceSetting_NotificationSetting_EmailSetting{
						Enabled:      true,
						SmtpHost:     "smtp.example.com",
						SmtpPort:     587,
						SmtpUsername: "bot@example.com",
						SmtpPassword: smtpSecret,
						FromEmail:    "bot@example.com",
						UseTls:       true,
					},
				},
			},
		},
	})
	require.NoError(t, err)

	// Identity provider: client secret is write-only and must stay masked.
	const idpSecret = "idp-cleartext-secret"
	createdIDP, err := ts.Service.CreateIdentityProvider(adminCtx, &apipb.CreateIdentityProviderRequest{
		IdentityProvider: &apipb.IdentityProvider{
			Title: "Example SSO",
			Type:  apipb.IdentityProvider_OAUTH2,
			Config: &apipb.IdentityProviderConfig{
				Config: &apipb.IdentityProviderConfig_Oauth2Config{
					Oauth2Config: &apipb.OAuth2Config{
						ClientId:     "client-123",
						ClientSecret: idpSecret,
						AuthUrl:      "https://example.com/auth",
						TokenUrl:     "https://example.com/token",
						UserInfoUrl:  "https://example.com/userinfo",
						FieldMapping: &apipb.FieldMapping{Identifier: "sub"},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	// Usage limits: create, update, delete each leave an entry.
	createdLimit, err := ts.Service.CreateUsageLimit(adminCtx, &apipb.CreateUsageLimitRequest{
		UsageLimitId: "audit-limit",
		UsageLimit:   &apipb.UsageLimit{Metric: apipb.UsageLimit_MEMO_COUNT, Value: 12345},
	})
	require.NoError(t, err)
	_, err = ts.Service.UpdateUsageLimit(adminCtx, &apipb.UpdateUsageLimitRequest{
		UsageLimit: &apipb.UsageLimit{Name: createdLimit.Name, Value: 54321, Metric: apipb.UsageLimit_MEMO_COUNT},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"value"}},
	})
	require.NoError(t, err)
	_, err = ts.Service.DeleteUsageLimit(adminCtx, &apipb.DeleteUsageLimitRequest{Name: createdLimit.Name})
	require.NoError(t, err)

	// User role promotion and account deactivation/reactivation.
	updatedUser, err := ts.Service.UpdateUser(adminCtx, &apipb.UpdateUserRequest{
		User:       &apipb.User{Name: "users/bob", Role: apipb.User_ADMIN},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"role"}},
	})
	require.NoError(t, err)
	require.Equal(t, apipb.User_ADMIN, updatedUser.Role)
	_, err = ts.Service.UpdateUser(adminCtx, &apipb.UpdateUserRequest{
		User:       &apipb.User{Name: "users/bob", State: apipb.State_ARCHIVED},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"state"}},
	})
	require.NoError(t, err)
	_, err = ts.Service.UpdateUser(adminCtx, &apipb.UpdateUserRequest{
		User:       &apipb.User{Name: "users/bob", State: apipb.State_NORMAL},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"state"}},
	})
	require.NoError(t, err)

	entries := listAllAuditLogs(adminCtx, t, ts)

	// Ordinary memo operations by a regular user never enter the ledger.
	_, err = ts.Service.CreateMemo(bobCtx, &apipb.CreateMemoRequest{Memo: &apipb.Memo{Content: "hello audit", Visibility: apipb.Visibility_PRIVATE}})
	require.NoError(t, err)
	require.Len(t, listAllAuditLogs(adminCtx, t, ts), len(entries), "memo creation must not be audited")

	// No clear-text secret anywhere in the ledger.
	for _, log := range listAllAuditLogs(adminCtx, t, ts) {
		for _, change := range log.Changes {
			require.NotContains(t, change.Before, smtpSecret)
			require.NotContains(t, change.After, smtpSecret)
			require.NotContains(t, change.Before, idpSecret)
			require.NotContains(t, change.After, idpSecret)
		}
	}

	// Every entry identifies its actor and carries a timestamp.
	for _, log := range entries {
		require.Equal(t, "users/root", log.Actor)
		require.NotNil(t, log.CreateTime)
		require.True(t, log.CreateTime.Seconds > 0)
	}

	// Access setting entry.
	var accessLog *apipb.AuditLog
	for _, log := range entries {
		if log.Resource == "instance/settings/ACCESS" {
			accessLog = log
			break
		}
	}
	require.NotNil(t, accessLog)
	require.Equal(t, apipb.AuditAction_AUDIT_ACTION_UPDATE, accessLog.Action)
	require.Equal(t, apipb.AuditResourceType_AUDIT_RESOURCE_INSTANCE_SETTING, accessLog.ResourceType)
	accessChange, ok := findAuditChange(accessLog, "accessMode")
	require.True(t, ok)
	require.Equal(t, "INSTANCE_ACCESS_MODE_PUBLIC", accessChange.Before)
	require.Equal(t, "INSTANCE_ACCESS_MODE_PRIVATE", accessChange.After)

	// Notification entry masks the SMTP password but keeps other fields.
	var notificationLog *apipb.AuditLog
	for _, log := range entries {
		if log.Resource == "instance/settings/NOTIFICATION" {
			notificationLog = log
			break
		}
	}
	require.NotNil(t, notificationLog)
	passwordChange, ok := findAuditChange(notificationLog, "email.smtpPassword")
	require.True(t, ok)
	require.Equal(t, "***", passwordChange.After)
	hostChange, ok := findAuditChange(notificationLog, "email.smtpHost")
	require.True(t, ok)
	require.Equal(t, "smtp.example.com", hostChange.After)

	// Identity provider entry masks the client secret.
	var idpLog *apipb.AuditLog
	for _, log := range entries {
		if log.Resource == createdIDP.Name {
			idpLog = log
			break
		}
	}
	require.NotNil(t, idpLog)
	require.Equal(t, apipb.AuditAction_AUDIT_ACTION_CREATE, idpLog.Action)
	secretChange, ok := findAuditChange(idpLog, "config.oauth2Config.clientSecret")
	require.True(t, ok)
	require.Equal(t, "***", secretChange.After)
	clientIDChange, ok := findAuditChange(idpLog, "config.oauth2Config.clientId")
	require.True(t, ok)
	require.Equal(t, "client-123", clientIDChange.After)

	// Usage limit entries cover all three actions and record values.
	var limitCreate, limitUpdate, limitDelete *apipb.AuditLog
	for _, log := range entries {
		if log.Resource != createdLimit.Name {
			continue
		}
		switch log.Action {
		case apipb.AuditAction_AUDIT_ACTION_CREATE:
			limitCreate = log
		case apipb.AuditAction_AUDIT_ACTION_UPDATE:
			limitUpdate = log
		case apipb.AuditAction_AUDIT_ACTION_DELETE:
			limitDelete = log
		default:
		}
	}
	require.NotNil(t, limitCreate)
	require.NotNil(t, limitUpdate)
	require.NotNil(t, limitDelete)
	valueCreate, ok := findAuditChange(limitCreate, "value")
	require.True(t, ok)
	require.Equal(t, "12345", valueCreate.After)
	valueUpdate, ok := findAuditChange(limitUpdate, "value")
	require.True(t, ok)
	require.Equal(t, "12345", valueUpdate.Before)
	require.Equal(t, "54321", valueUpdate.After)
	targetChange, ok := findAuditChange(limitCreate, "target")
	require.True(t, ok)
	require.Equal(t, "all_users", targetChange.After)

	// User role and state entries.
	userLogs := []*apipb.AuditLog{}
	for _, log := range entries {
		if log.Resource == "users/bob" {
			userLogs = append(userLogs, log)
		}
	}
	require.Len(t, userLogs, 3)
	var roleChange *apipb.AuditFieldChange
	stateChanges := []*apipb.AuditFieldChange{}
	for _, log := range userLogs {
		if change, ok := findAuditChange(log, "role"); ok {
			roleChange = change
		}
		if change, ok := findAuditChange(log, "state"); ok {
			stateChanges = append(stateChanges, change)
		}
	}
	require.Equal(t, "USER", roleChange.Before)
	require.Equal(t, "ADMIN", roleChange.After)
	require.Len(t, stateChanges, 2)
}

func TestAuditLedgerFiltersAndPaging(t *testing.T) {
	ctx := context.Background()
	ts := NewTestService(t)
	defer ts.Cleanup()

	admin, err := ts.CreateHostUser(ctx, "root")
	require.NoError(t, err)
	adminCtx := ts.CreateUserContext(ctx, admin.ID)

	_, err = ts.Service.UpdateInstanceSetting(adminCtx, &apipb.UpdateInstanceSettingRequest{
		InstanceSetting: &apipb.InstanceSetting{
			Name: "instance/settings/ACCESS",
			Value: &apipb.InstanceSetting_AccessSetting_{
				AccessSetting: &apipb.InstanceSetting_AccessSetting{AccessMode: apipb.InstanceAccessMode_INSTANCE_ACCESS_MODE_PRIVATE},
			},
		},
	})
	require.NoError(t, err)
	_, err = ts.Service.CreateIdentityProvider(adminCtx, &apipb.CreateIdentityProviderRequest{
		IdentityProvider: minimalIDP("P"),
	})
	require.NoError(t, err)

	// Filter by object.
	resp, err := ts.Service.ListAuditLogs(adminCtx, &apipb.ListAuditLogsRequest{Resource: "instance/settings/ACCESS"})
	require.NoError(t, err)
	require.NotEmpty(t, resp.AuditLogs)
	for _, log := range resp.AuditLogs {
		require.Equal(t, "instance/settings/ACCESS", log.Resource)
	}

	// Filter by resource type.
	resp, err = ts.Service.ListAuditLogs(adminCtx, &apipb.ListAuditLogsRequest{ResourceType: apipb.AuditResourceType_AUDIT_RESOURCE_IDENTITY_PROVIDER})
	require.NoError(t, err)
	require.NotEmpty(t, resp.AuditLogs)
	for _, log := range resp.AuditLogs {
		require.Equal(t, apipb.AuditResourceType_AUDIT_RESOURCE_IDENTITY_PROVIDER, log.ResourceType)
	}

	// Filter by actor.
	resp, err = ts.Service.ListAuditLogs(adminCtx, &apipb.ListAuditLogsRequest{Actor: "users/root"})
	require.NoError(t, err)
	require.Len(t, resp.AuditLogs, 2)
	for _, log := range resp.AuditLogs {
		require.Equal(t, "users/root", log.Actor)
	}

	// Unknown actor is a not-found error rather than an empty list.
	_, err = ts.Service.ListAuditLogs(adminCtx, &apipb.ListAuditLogsRequest{Actor: "users/ghost"})
	require.Equal(t, codes.NotFound, status.Code(err))

	// Paging.
	page1, err := ts.Service.ListAuditLogs(adminCtx, &apipb.ListAuditLogsRequest{PageSize: 1})
	require.NoError(t, err)
	require.Len(t, page1.AuditLogs, 1)
	require.NotEmpty(t, page1.NextPageToken)
	page2, err := ts.Service.ListAuditLogs(adminCtx, &apipb.ListAuditLogsRequest{PageToken: page1.NextPageToken})
	require.NoError(t, err)
	require.Len(t, page2.AuditLogs, 1)
	require.NotEqual(t, page1.AuditLogs[0].Name, page2.AuditLogs[0].Name)
	// Newest first: the first page holds the higher entry ID even when two
	// entries share the same timestamp second.
	firstID, err := strconv.ParseInt(strings.TrimPrefix(page1.AuditLogs[0].Name, "audit-logs/"), 10, 64)
	require.NoError(t, err)
	secondID, err := strconv.ParseInt(strings.TrimPrefix(page2.AuditLogs[0].Name, "audit-logs/"), 10, 64)
	require.NoError(t, err)
	require.Greater(t, firstID, secondID)
}

func TestAuditLedgerRequiresAdmin(t *testing.T) {
	ctx := context.Background()
	ts := NewTestService(t)
	defer ts.Cleanup()

	user, err := ts.CreateRegularUser(ctx, "nobody")
	require.NoError(t, err)
	userCtx := ts.CreateUserContext(ctx, user.ID)

	_, err = ts.Service.ListAuditLogs(userCtx, &apipb.ListAuditLogsRequest{})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	_, err = ts.Service.GetAuditLogRetention(userCtx, &apipb.GetAuditLogRetentionRequest{})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	_, err = ts.Service.UpdateAuditLogRetention(userCtx, &apipb.UpdateAuditLogRetentionRequest{
		Retention: &apipb.AuditLogRetention{MaxRecords: 1},
	})
	require.Equal(t, codes.PermissionDenied, status.Code(err))

	_, err = ts.Service.ListAuditLogs(ctx, &apipb.ListAuditLogsRequest{})
	require.Equal(t, codes.Unauthenticated, status.Code(err))
}

func TestAuditRetentionPolicy(t *testing.T) {
	ctx := context.Background()
	ts := NewTestService(t)
	defer ts.Cleanup()

	admin, err := ts.CreateHostUser(ctx, "root")
	require.NoError(t, err)
	adminCtx := ts.CreateUserContext(ctx, admin.ID)

	// Defaults.
	retention, err := ts.Service.GetAuditLogRetention(adminCtx, &apipb.GetAuditLogRetentionRequest{})
	require.NoError(t, err)
	require.Equal(t, int64(10000), retention.MaxRecords)
	require.Equal(t, int32(365), retention.RetainDays)

	// Invalid declarations are refused.
	_, err = ts.Service.UpdateAuditLogRetention(adminCtx, &apipb.UpdateAuditLogRetentionRequest{
		Retention: &apipb.AuditLogRetention{MaxRecords: -1},
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = ts.Service.UpdateAuditLogRetention(adminCtx, &apipb.UpdateAuditLogRetentionRequest{
		Retention: &apipb.AuditLogRetention{RetainDays: -1},
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	// Declare a tight count bound and produce more entries than it keeps.
	_, err = ts.Service.UpdateAuditLogRetention(adminCtx, &apipb.UpdateAuditLogRetentionRequest{
		Retention: &apipb.AuditLogRetention{MaxRecords: 2, RetainDays: 0},
	})
	require.NoError(t, err)
	retention, err = ts.Service.GetAuditLogRetention(adminCtx, &apipb.GetAuditLogRetentionRequest{})
	require.NoError(t, err)
	require.Equal(t, int64(2), retention.MaxRecords)
	require.Equal(t, int32(0), retention.RetainDays)

	for range 5 {
		_, err = ts.Service.CreateIdentityProvider(adminCtx, &apipb.CreateIdentityProviderRequest{
			IdentityProvider: minimalIDP("P"),
		})
		require.NoError(t, err)
	}
	// The retention update itself and the five creations prune oldest first;
	// only the newest two entries survive.
	entries := listAllAuditLogs(adminCtx, t, ts)
	require.Len(t, entries, 2)
	for _, log := range entries {
		require.Equal(t, apipb.AuditResourceType_AUDIT_RESOURCE_IDENTITY_PROVIDER, log.ResourceType)
	}

	// Disabling the count bound afterwards does not resurrect pruned rows.
	_, err = ts.Service.UpdateAuditLogRetention(adminCtx, &apipb.UpdateAuditLogRetentionRequest{
		Retention: &apipb.AuditLogRetention{MaxRecords: 0, RetainDays: 0},
	})
	require.NoError(t, err)
	entries = listAllAuditLogs(adminCtx, t, ts)
	require.Len(t, entries, 2, "pruned entries stay deleted")
}

func TestAuditIDPSecretMaskedOnConfigUpdate(t *testing.T) {
	ctx := context.Background()
	ts := NewTestService(t)
	defer ts.Cleanup()

	admin, err := ts.CreateHostUser(ctx, "root")
	require.NoError(t, err)
	adminCtx := ts.CreateUserContext(ctx, admin.ID)

	const secret = "rotate-me-secret"
	created, err := ts.Service.CreateIdentityProvider(adminCtx, &apipb.CreateIdentityProviderRequest{
		IdentityProvider: &apipb.IdentityProvider{
			Title: "SSO",
			Type:  apipb.IdentityProvider_OAUTH2,
			Config: &apipb.IdentityProviderConfig{
				Config: &apipb.IdentityProviderConfig_Oauth2Config{
					Oauth2Config: &apipb.OAuth2Config{
						ClientId:     "c",
						ClientSecret: secret,
						AuthUrl:      "a",
						TokenUrl:     "t",
						UserInfoUrl:  "u",
						FieldMapping: &apipb.FieldMapping{Identifier: "sub"},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	// Update the title while sending an empty secret: the stored secret is
	// preserved, and the ledger must neither leak it nor record a fake change.
	_, err = ts.Service.UpdateIdentityProvider(adminCtx, &apipb.UpdateIdentityProviderRequest{
		IdentityProvider: &apipb.IdentityProvider{
			Name:  created.Name,
			Title: "SSO Renamed",
			Type:  apipb.IdentityProvider_OAUTH2,
			Config: &apipb.IdentityProviderConfig{
				Config: &apipb.IdentityProviderConfig_Oauth2Config{
					Oauth2Config: &apipb.OAuth2Config{ClientId: "c", AuthUrl: "a", TokenUrl: "t", UserInfoUrl: "u"},
				},
			},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"title"}},
	})
	require.NoError(t, err)

	for _, log := range listAllAuditLogs(adminCtx, t, ts) {
		for _, change := range log.Changes {
			require.False(t, strings.Contains(change.Before, secret) || strings.Contains(change.After, secret))
		}
	}
}
