package webhookdelivery

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/usememos/memos/internal/webhook"
	storepb "github.com/usememos/memos/proto/gen/store"
	"github.com/usememos/memos/store"
	teststore "github.com/usememos/memos/store/test"
)

// countingReceiver records request arrival times and answers 200/code=0 while
// healthy and 500 while failing.
type countingReceiver struct {
	server  *httptest.Server
	mu      sync.Mutex
	failing bool
	times   []time.Time
}

func newCountingReceiver(t *testing.T, failing bool) *countingReceiver {
	t.Helper()
	r := &countingReceiver{failing: failing}
	r.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		_, _ = io.Copy(io.Discard, req.Body)
		r.mu.Lock()
		r.times = append(r.times, time.Now())
		failing := r.failing
		r.mu.Unlock()
		if failing {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":0}`))
	}))
	t.Cleanup(r.server.Close)
	return r
}

func (r *countingReceiver) setFailing(failing bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failing = failing
}

func (r *countingReceiver) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.times)
}

func (r *countingReceiver) snapshotTimes() []time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]time.Time, len(r.times))
	copy(out, r.times)
	return out
}

// allowLoopbackWebhooks lets the SSRF guard accept httptest receivers for the
// duration of the test.
func allowLoopbackWebhooks(t *testing.T) {
	t.Helper()
	require.NoError(t, webhook.ConfigurePrivateDestinationAllowlist([]string{"127.0.0.1", "::1"}))
	t.Cleanup(func() {
		require.NoError(t, webhook.ConfigurePrivateDestinationAllowlist(nil))
	})
}

func newMemoryStore(ctx context.Context, t *testing.T) *store.Store {
	t.Helper()
	// :memory: creates a database per connection, so pin the pool to one
	// connection; every goroutine then shares the same in-memory database.
	st := teststore.NewTestingStoreWithDSN(ctx, t, "sqlite", ":memory:")
	db := st.GetDriver().GetDB()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	require.NoError(t, st.Migrate(ctx))
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func upsertWebhookSetting(ctx context.Context, t *testing.T, st *store.Store, setting *storepb.InstanceWebhookSetting) {
	t.Helper()
	_, err := st.UpsertInstanceSetting(ctx, &storepb.InstanceSetting{
		Key:   storepb.InstanceSettingKey_WEBHOOK,
		Value: &storepb.InstanceSetting_WebhookSetting{WebhookSetting: setting},
	})
	require.NoError(t, err)
}

func testRequest(url string) *webhook.Request {
	return &webhook.Request{
		URL:       url,
		Label:     "memos.memo.created",
		UserID:    1,
		WebhookID: "hook-1",
		Payload:   map[string]any{"activityType": "memos.memo.created"},
	}
}

func waitForCount(t *testing.T, r *countingReceiver, want int, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if r.count() >= want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	require.Equal(t, want, r.count(), "receiver request count")
}

func TestSubmitDisabledUsesLegacyAsyncQueue(t *testing.T) {
	allowLoopbackWebhooks(t)
	ctx := context.Background()
	st := newMemoryStore(ctx, t)
	receiver := newCountingReceiver(t, false)

	manager := New(st, WithPollInterval(20*time.Millisecond))
	require.NoError(t, manager.Start(ctx))
	t.Cleanup(manager.Close)

	manager.Submit(ctx, testRequest(receiver.server.URL))
	waitForCount(t, receiver, 1, 3*time.Second)

	rows, err := st.ListWebhookDeliveries(ctx, &store.FindWebhookDeliveries{})
	require.NoError(t, err)
	require.Empty(t, rows, "no ledger rows are written while the policy is disabled")
}

func TestEnabledDeliverySucceeds(t *testing.T) {
	allowLoopbackWebhooks(t)
	ctx := context.Background()
	st := newMemoryStore(ctx, t)
	receiver := newCountingReceiver(t, false)
	upsertWebhookSetting(ctx, t, st, policySetting(3, 1, 4, 100))

	manager := New(st, WithPollInterval(20*time.Millisecond))
	require.NoError(t, manager.Start(ctx))
	t.Cleanup(manager.Close)

	manager.Submit(ctx, testRequest(receiver.server.URL))
	waitForCount(t, receiver, 1, 3*time.Second)

	var row *store.WebhookDelivery
	require.Eventually(t, func() bool {
		rows, err := st.ListWebhookDeliveries(ctx, &store.FindWebhookDeliveries{})
		require.NoError(t, err)
		if len(rows) == 1 {
			row = rows[0]
		}
		return len(rows) == 1 && row.Status == store.WebhookDeliverySucceeded
	}, 3*time.Second, 20*time.Millisecond)
	require.Equal(t, int32(1), row.Attempts)
	require.NotZero(t, row.SettledTs)
	require.Empty(t, row.LastError)
}

func TestRetriesExponentiallyThenAbandons(t *testing.T) {
	allowLoopbackWebhooks(t)
	ctx := context.Background()
	st := newMemoryStore(ctx, t)
	receiver := newCountingReceiver(t, true)
	upsertWebhookSetting(ctx, t, st, policySetting(3, 1, 2, 100))

	manager := New(st, WithPollInterval(20*time.Millisecond))
	require.NoError(t, manager.Start(ctx))
	t.Cleanup(manager.Close)

	manager.Submit(ctx, testRequest(receiver.server.URL))
	waitForCount(t, receiver, 3, 10*time.Second)

	times := receiver.snapshotTimes()
	require.Len(t, times, 3)
	require.InDelta(t, 1, times[1].Sub(times[0]).Seconds(), 0.5)
	require.InDelta(t, 2, times[2].Sub(times[1]).Seconds(), 0.5)

	// No fourth attempt is scheduled after the delivery is abandoned.
	time.Sleep(2 * time.Second)
	require.Equal(t, 3, receiver.count())

	rows, err := st.ListWebhookDeliveries(ctx, &store.FindWebhookDeliveries{})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	row := rows[0]
	require.Equal(t, store.WebhookDeliveryAbandoned, row.Status)
	require.Equal(t, int32(3), row.Attempts)
	require.NotZero(t, row.SettledTs)
	require.Contains(t, row.LastError, "500")
}

func TestRetryBecomesSucceedsWhenReceiverRecovers(t *testing.T) {
	allowLoopbackWebhooks(t)
	ctx := context.Background()
	st := newMemoryStore(ctx, t)
	receiver := newCountingReceiver(t, true)
	upsertWebhookSetting(ctx, t, st, policySetting(3, 1, 2, 100))

	manager := New(st, WithPollInterval(20*time.Millisecond))
	require.NoError(t, manager.Start(ctx))
	t.Cleanup(manager.Close)

	manager.Submit(ctx, testRequest(receiver.server.URL))
	waitForCount(t, receiver, 1, 3*time.Second)
	// Recover the receiver before the retry budget is exhausted.
	receiver.setFailing(false)
	waitForCount(t, receiver, 2, 5*time.Second)
	time.Sleep(2 * time.Second)
	require.Equal(t, 2, receiver.count(), "no further attempts follow success")

	rows, err := st.ListWebhookDeliveries(ctx, &store.FindWebhookDeliveries{})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, store.WebhookDeliverySucceeded, rows[0].Status)
	require.Equal(t, int32(2), rows[0].Attempts)
	require.Empty(t, rows[0].LastError)
}

func TestRestartRecoversInterruptedAndNeverReplaysSucceeded(t *testing.T) {
	allowLoopbackWebhooks(t)
	ctx := context.Background()
	st := newMemoryStore(ctx, t)
	receiver := newCountingReceiver(t, false)
	upsertWebhookSetting(ctx, t, st, policySetting(5, 1, 2, 100))

	body := `{"activityType":"memos.memo.created"}`
	seed := func(status store.WebhookDeliveryStatus, nextDueTs, attempts int64) int64 {
		row, err := st.CreateWebhookDelivery(ctx, &store.WebhookDelivery{
			UserID: 1, WebhookID: "hook-restart", Label: "memos.memo.created",
			URL: receiver.server.URL, Status: status, MaxAttempts: 5,
			Attempts: int32(attempts), NextDueTs: nextDueTs, RequestBody: body,
		})
		require.NoError(t, err)
		return row.ID
	}
	nowMilli := time.Now().UnixMilli()
	succeededID := seed(store.WebhookDeliverySucceeded, 0, 1)
	interruptedID := seed(store.WebhookDeliveryDelivering, nowMilli, 1)
	futureID := seed(store.WebhookDeliveryRetrying, nowMilli+3600_000, 1)
	dueID := seed(store.WebhookDeliveryRetrying, nowMilli, 1)

	manager := New(st, WithPollInterval(20*time.Millisecond))
	require.NoError(t, manager.Start(ctx))
	t.Cleanup(manager.Close)

	// interrupted + due rows are delivered (2 requests); the future row waits
	// and the succeeded row must never be delivered again.
	waitForCount(t, receiver, 2, 5*time.Second)
	time.Sleep(500 * time.Millisecond)
	require.Equal(t, 2, receiver.count(), "succeeded and future rows are not re-sent")

	for id, want := range map[int64]store.WebhookDeliveryStatus{
		succeededID:   store.WebhookDeliverySucceeded,
		interruptedID: store.WebhookDeliverySucceeded,
		dueID:         store.WebhookDeliverySucceeded,
		futureID:      store.WebhookDeliveryRetrying,
	} {
		rows, err := st.ListWebhookDeliveries(ctx, &store.FindWebhookDeliveries{ID: &id})
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.Equal(t, want, rows[0].Status, "delivery %d", id)
	}
}

func TestLedgerWriteFailureFallsBackToLegacyQueue(t *testing.T) {
	allowLoopbackWebhooks(t)
	ctx := context.Background()
	st := newMemoryStore(ctx, t)
	receiver := newCountingReceiver(t, false)
	upsertWebhookSetting(ctx, t, st, policySetting(3, 1, 2, 100))

	manager := New(st, WithPollInterval(20*time.Millisecond))
	require.NoError(t, manager.Start(ctx))
	t.Cleanup(manager.Close)

	// Stop workers and close the store so ledger registration fails; the
	// submission must still reach the receiver through the legacy channel.
	manager.Close()
	require.NoError(t, st.Close())
	manager.Submit(ctx, testRequest(receiver.server.URL))
	waitForCount(t, receiver, 1, 3*time.Second)
}

func TestPolicyHotReload(t *testing.T) {
	ctx := context.Background()
	st := newMemoryStore(ctx, t)

	manager := New(st, WithPollInterval(20*time.Millisecond))
	require.NoError(t, manager.Start(ctx))
	t.Cleanup(manager.Close)

	require.False(t, manager.currentPolicy().Enabled)
	upsertWebhookSetting(ctx, t, st, policySetting(2, 1, 2, 10))
	require.NoError(t, manager.ReloadPolicy(ctx))
	require.True(t, manager.currentPolicy().Enabled)
	require.Equal(t, int32(2), manager.currentPolicy().MaxAttempts)
	require.Equal(t, 10, manager.currentPolicy().Retention)
}

func TestWakeDoesNotBlockWhenChannelBusy(t *testing.T) {
	ctx := context.Background()
	st := newMemoryStore(ctx, t)
	manager := New(st)
	require.NoError(t, manager.Start(ctx))
	t.Cleanup(manager.Close)

	done := make(chan struct{})
	go func() {
		manager.Wake()
		manager.Wake()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("wake blocked")
	}
}
