package audit

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	storepb "github.com/usememos/memos/proto/gen/store"
)

func findChange(changes []FieldChange, field string) (FieldChange, bool) {
	for _, change := range changes {
		if change.Field == field {
			return change, true
		}
	}
	return FieldChange{}, false
}

func TestMessageChangesScalar(t *testing.T) {
	before := &storepb.InstanceAccessSetting{AccessMode: storepb.InstanceAccessMode_INSTANCE_ACCESS_MODE_PRIVATE}
	after := &storepb.InstanceAccessSetting{AccessMode: storepb.InstanceAccessMode_INSTANCE_ACCESS_MODE_PUBLIC}
	changes, err := MessageChanges(before, after)
	require.NoError(t, err)
	change, ok := findChange(changes, "accessMode")
	require.True(t, ok)
	require.Equal(t, "INSTANCE_ACCESS_MODE_PRIVATE", change.Before)
	require.Equal(t, "INSTANCE_ACCESS_MODE_PUBLIC", change.After)
}

func TestMessageChangesEqualIsEmpty(t *testing.T) {
	before := &storepb.InstanceAccessSetting{AccessMode: storepb.InstanceAccessMode_INSTANCE_ACCESS_MODE_PRIVATE}
	after := &storepb.InstanceAccessSetting{AccessMode: storepb.InstanceAccessMode_INSTANCE_ACCESS_MODE_PRIVATE}
	changes, err := MessageChanges(before, after)
	require.NoError(t, err)
	require.Empty(t, changes)
}

func TestMessageChangesCreateMasksClientSecret(t *testing.T) {
	after := &storepb.IdentityProvider{
		Name: "Google",
		Type: storepb.IdentityProvider_OAUTH2,
		Config: &storepb.IdentityProviderConfig{
			Config: &storepb.IdentityProviderConfig_Oauth2Config{
				Oauth2Config: &storepb.OAuth2Config{
					ClientId:     "client-id",
					ClientSecret: "super-secret-value",
				},
			},
		},
	}
	changes, err := MessageChanges(nil, after)
	require.NoError(t, err)

	clientID, ok := findChange(changes, "config.oauth2Config.clientId")
	require.True(t, ok)
	require.Equal(t, "client-id", clientID.After)

	secret, ok := findChange(changes, "config.oauth2Config.clientSecret")
	require.True(t, ok)
	require.Equal(t, "", secret.Before)
	require.Equal(t, "***", secret.After)
	for _, change := range changes {
		require.NotContains(t, change.After, "super-secret-value")
		require.NotContains(t, change.Before, "super-secret-value")
	}
}

func TestMessageChangesRotatedSecretIsMasked(t *testing.T) {
	before := &storepb.IdentityProviderConfig{
		Config: &storepb.IdentityProviderConfig_Oauth2Config{
			Oauth2Config: &storepb.OAuth2Config{ClientId: "id", ClientSecret: "old-secret"},
		},
	}
	after := &storepb.IdentityProviderConfig{
		Config: &storepb.IdentityProviderConfig_Oauth2Config{
			Oauth2Config: &storepb.OAuth2Config{ClientId: "id", ClientSecret: "new-secret"},
		},
	}
	changes, err := MessageChanges(before, after)
	require.NoError(t, err)
	secret, ok := findChange(changes, "oauth2Config.clientSecret")
	require.True(t, ok, "a rotated secret is recorded as a change, but masked on both sides")
	require.Equal(t, "***", secret.Before)
	require.Equal(t, "***", secret.After)
	for _, change := range changes {
		require.NotContains(t, change.Before, "old-secret")
		require.NotContains(t, change.After, "new-secret")
	}

	// An unchanged secret produces no entry at all.
	unchanged, err := MessageChanges(before, proto.CloneOf(before))
	require.NoError(t, err)
	_, ok = findChange(unchanged, "oauth2Config.clientSecret")
	require.False(t, ok)
}

func TestMessageChangesMasksSMTPPasswordAndStorageSecret(t *testing.T) {
	before := &storepb.InstanceNotificationSetting{}
	after := &storepb.InstanceNotificationSetting{
		Email: &storepb.InstanceNotificationSetting_EmailSetting{
			SmtpHost:     "smtp.example.com",
			SmtpPassword: "mail-password",
		},
	}
	changes, err := MessageChanges(before, after)
	require.NoError(t, err)
	password, ok := findChange(changes, "email.smtpPassword")
	require.True(t, ok)
	require.Equal(t, "***", password.After)
	host, ok := findChange(changes, "email.smtpHost")
	require.True(t, ok)
	require.Equal(t, "smtp.example.com", host.After)
	for _, change := range changes {
		require.NotContains(t, change.After, "mail-password")
	}

	storageBefore := &storepb.InstanceStorageSetting{}
	storageAfter := &storepb.InstanceStorageSetting{
		Storages: []*storepb.Storage{{
			Id:   "s3",
			Name: "S3",
			Config: &storepb.Storage_S3Config{S3Config: &storepb.StorageS3Config{
				AccessKeyId:     "AKIA-identifier",
				AccessKeySecret: "aws-secret-key",
			}},
		}},
	}
	storageChanges, err := MessageChanges(storageBefore, storageAfter)
	require.NoError(t, err)
	secretChange, ok := findChange(storageChanges, "storages[0].s3Config.accessKeySecret")
	require.True(t, ok)
	require.Equal(t, "***", secretChange.After)
	idChange, ok := findChange(storageChanges, "storages[0].s3Config.accessKeyId")
	require.True(t, ok)
	require.Equal(t, "AKIA-identifier", idChange.After)
}

func TestEntryToStoreRoundTrip(t *testing.T) {
	entry := NewEntry(7, "alice", storepb.AuditLog_USER, storepb.AuditLog_UPDATE, "bob",
		[]FieldChange{Change("role", "USER", "ADMIN")}, "127.0.0.1")
	row, err := entry.ToStore()
	require.NoError(t, err)
	require.Equal(t, int32(7), row.ActorID)
	require.Equal(t, "bob", row.ResourceID)
	require.Contains(t, row.Changes, `"role"`)

	decoded := DecodeChanges(row.Changes)
	require.Len(t, decoded, 1)
	require.Equal(t, "role", decoded[0].Field)
	require.Equal(t, "ADMIN", decoded[0].After)
}
