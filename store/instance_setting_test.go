package store

import (
	"testing"

	"github.com/stretchr/testify/require"

	storepb "github.com/usememos/memos/proto/gen/store"
)

func TestValidateInstanceNotificationRetentionSetting(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		retention *storepb.InstanceNotificationSetting_RetentionSetting
		wantErr   bool
	}{
		{name: "unset", retention: nil, wantErr: false},
		{name: "both zero disables", retention: &storepb.InstanceNotificationSetting_RetentionSetting{}, wantErr: false},
		{name: "only age", retention: &storepb.InstanceNotificationSetting_RetentionSetting{MaxAgeDays: 30}, wantErr: false},
		{name: "only cap", retention: &storepb.InstanceNotificationSetting_RetentionSetting{MaxPerUser: 500}, wantErr: false},
		{name: "at upper bounds", retention: &storepb.InstanceNotificationSetting_RetentionSetting{
			MaxAgeDays: MaxNotificationRetentionAgeDays, MaxPerUser: MaxNotificationRetentionPerUser,
		}, wantErr: false},
		{name: "negative age", retention: &storepb.InstanceNotificationSetting_RetentionSetting{MaxAgeDays: -1}, wantErr: true},
		{name: "negative cap", retention: &storepb.InstanceNotificationSetting_RetentionSetting{MaxPerUser: -1}, wantErr: true},
		{name: "age too large", retention: &storepb.InstanceNotificationSetting_RetentionSetting{MaxAgeDays: MaxNotificationRetentionAgeDays + 1}, wantErr: true},
		{name: "cap too large", retention: &storepb.InstanceNotificationSetting_RetentionSetting{MaxPerUser: MaxNotificationRetentionPerUser + 1}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			setting := &storepb.InstanceNotificationSetting{Retention: tc.retention}
			err := ValidateInstanceNotificationSetting(setting)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
