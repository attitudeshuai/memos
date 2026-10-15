package webhookdelivery

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	storepb "github.com/usememos/memos/proto/gen/store"
)

func policySetting(maxAttempts int32, base, capSeconds int64, retention int32) *storepb.InstanceWebhookSetting {
	return &storepb.InstanceWebhookSetting{
		RetryPolicy: &storepb.WebhookRetryPolicy{
			MaxAttempts:        maxAttempts,
			BackoffBaseSeconds: base,
			BackoffMaxSeconds:  capSeconds,
		},
		LedgerRetentionCount: retention,
	}
}

func policySettingWithRetry(retry *storepb.WebhookRetryPolicy, retention int32) *storepb.InstanceWebhookSetting {
	return &storepb.InstanceWebhookSetting{RetryPolicy: retry, LedgerRetentionCount: retention}
}

func TestBackoffDelay(t *testing.T) {
	t.Parallel()
	base := time.Second
	capDelay := 10 * time.Second
	for _, tc := range []struct {
		name    string
		attempt int32
		want    time.Duration
	}{
		{name: "first failure waits one base", attempt: 1, want: time.Second},
		{name: "second failure doubles", attempt: 2, want: 2 * time.Second},
		{name: "third failure quadruples", attempt: 3, want: 4 * time.Second},
		{name: "fourth failure keeps growing", attempt: 4, want: 8 * time.Second},
		{name: "growth caps at max", attempt: 5, want: capDelay},
		{name: "later attempts stay capped", attempt: 20, want: capDelay},
		{name: "non-positive attempt uses base", attempt: 0, want: time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, backoffDelay(tc.attempt, base, capDelay))
		})
	}

	// A non-positive base always yields the cap.
	require.Equal(t, capDelay, backoffDelay(2, 0, capDelay))
}

func TestPolicyFromSetting(t *testing.T) {
	t.Parallel()
	require.False(t, PolicyFromSetting(nil).Enabled, "nil setting disables the ledger")
	require.False(t, PolicyFromSetting(policySetting(0, 0, 0, 0)).Enabled, "zero attempts disable the ledger")
	require.False(t, PolicyFromSetting(policySettingWithRetry(nil, 10)).Enabled, "absent retry policy disables the ledger")

	enabled := PolicyFromSetting(policySetting(3, 10, 40, 5))
	require.True(t, enabled.Enabled)
	require.Equal(t, int32(3), enabled.MaxAttempts)
	require.Equal(t, 10*time.Second, enabled.BackoffBase)
	require.Equal(t, 40*time.Second, enabled.BackoffMax)
	require.Equal(t, 5, enabled.Retention)

	defaults := PolicyFromSetting(policySetting(2, 0, 0, 0))
	require.Equal(t, 30*time.Second, defaults.BackoffBase)
	require.Equal(t, 3600*time.Second, defaults.BackoffMax)
	require.Equal(t, 100, defaults.Retention)
}
