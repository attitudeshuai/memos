package webhookdelivery

import (
	"time"

	storepb "github.com/usememos/memos/proto/gen/store"
	"github.com/usememos/memos/store"
)

// Policy is the resolved delivery lifecycle policy applied by a Manager.
type Policy struct {
	// Enabled reports whether new submissions are registered in the ledger.
	// The ledger is always drained while the manager runs, even after the
	// policy is disabled.
	Enabled bool
	// MaxAttempts is the total number of delivery attempts per delivery,
	// including the first one.
	MaxAttempts int32
	// BackoffBase is the delay before the second attempt.
	BackoffBase time.Duration
	// BackoffMax caps the exponential backoff growth.
	BackoffMax time.Duration
	// Retention bounds the terminal records kept per webhook.
	Retention int
}

// PolicyFromSetting resolves the stored instance setting into a Policy.
// Zero-valued fields are replaced with the store-declared defaults.
func PolicyFromSetting(setting *storepb.InstanceWebhookSetting) Policy {
	policy := Policy{
		BackoffBase: store.WebhookBackoffBaseDefaultSeconds * time.Second,
		BackoffMax:  store.WebhookBackoffMaxDefaultSeconds * time.Second,
		Retention:   store.WebhookLedgerRetentionDefault,
	}
	if setting == nil {
		return policy
	}
	if setting.GetLedgerRetentionCount() > 0 {
		policy.Retention = int(setting.GetLedgerRetentionCount())
	}
	retry := setting.GetRetryPolicy()
	if retry == nil || retry.GetMaxAttempts() < 1 {
		return policy
	}
	policy.Enabled = true
	policy.MaxAttempts = retry.GetMaxAttempts()
	if retry.GetBackoffBaseSeconds() > 0 {
		policy.BackoffBase = time.Duration(retry.GetBackoffBaseSeconds()) * time.Second
	}
	if retry.GetBackoffMaxSeconds() > 0 {
		policy.BackoffMax = time.Duration(retry.GetBackoffMaxSeconds()) * time.Second
	}
	if policy.BackoffMax < policy.BackoffBase {
		policy.BackoffMax = policy.BackoffBase
	}
	return policy
}
