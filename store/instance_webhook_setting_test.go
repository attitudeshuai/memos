package store

import (
	"testing"

	"github.com/stretchr/testify/require"

	storepb "github.com/usememos/memos/proto/gen/store"
)

func TestValidateInstanceWebhookSetting(t *testing.T) {
	t.Parallel()

	webhook := func(policy *storepb.WebhookRetryPolicy, retention int32) *storepb.InstanceWebhookSetting {
		return &storepb.InstanceWebhookSetting{RetryPolicy: policy, LedgerRetentionCount: retention}
	}

	for _, tc := range []struct {
		name    string
		setting *storepb.InstanceWebhookSetting
		wantErr bool
	}{
		{name: "nil setting", setting: nil, wantErr: true},
		{name: "unset policy disables ledger", setting: webhook(nil, 0), wantErr: false},
		{name: "zero attempts disables ledger", setting: webhook(&storepb.WebhookRetryPolicy{MaxAttempts: 0}, 0), wantErr: false},
		{name: "ledger only with one attempt", setting: webhook(&storepb.WebhookRetryPolicy{MaxAttempts: 1}, 0), wantErr: false},
		{name: "upper bounds", setting: webhook(&storepb.WebhookRetryPolicy{
			MaxAttempts: WebhookMaxAttemptsLimit, BackoffBaseSeconds: WebhookBackoffBaseMaxSeconds,
			BackoffMaxSeconds: WebhookBackoffMaxMaxSeconds,
		}, WebhookLedgerRetentionLimit), wantErr: false},
		{name: "explicit zero defaults", setting: webhook(&storepb.WebhookRetryPolicy{MaxAttempts: 3}, 0), wantErr: false},
		{name: "max equals base", setting: webhook(&storepb.WebhookRetryPolicy{
			MaxAttempts: 3, BackoffBaseSeconds: 60, BackoffMaxSeconds: 60,
		}, 10), wantErr: false},
		{name: "negative attempts", setting: webhook(&storepb.WebhookRetryPolicy{MaxAttempts: -1}, 0), wantErr: true},
		{name: "attempts above limit", setting: webhook(&storepb.WebhookRetryPolicy{MaxAttempts: WebhookMaxAttemptsLimit + 1}, 0), wantErr: true},
		{name: "negative base", setting: webhook(&storepb.WebhookRetryPolicy{MaxAttempts: 3, BackoffBaseSeconds: -1}, 0), wantErr: true},
		{name: "base above limit", setting: webhook(&storepb.WebhookRetryPolicy{MaxAttempts: 3, BackoffBaseSeconds: WebhookBackoffBaseMaxSeconds + 1}, 0), wantErr: true},
		{name: "negative cap", setting: webhook(&storepb.WebhookRetryPolicy{MaxAttempts: 3, BackoffMaxSeconds: -1}, 0), wantErr: true},
		{name: "cap above limit", setting: webhook(&storepb.WebhookRetryPolicy{MaxAttempts: 3, BackoffMaxSeconds: WebhookBackoffMaxMaxSeconds + 1}, 0), wantErr: true},
		{name: "cap smaller than base", setting: webhook(&storepb.WebhookRetryPolicy{
			MaxAttempts: 3, BackoffBaseSeconds: 60, BackoffMaxSeconds: 30,
		}, 0), wantErr: true},
		{name: "negative retention", setting: webhook(nil, -1), wantErr: true},
		{name: "retention above limit", setting: webhook(nil, WebhookLedgerRetentionLimit+1), wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateInstanceWebhookSetting(tc.setting)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
