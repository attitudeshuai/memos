package test

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/usememos/memos/store"
)

func newTestDelivery(userID int32, webhookID string, status store.WebhookDeliveryStatus, nextDueTs int64) *store.WebhookDelivery {
	return &store.WebhookDelivery{
		UserID:        userID,
		WebhookID:     webhookID,
		Label:         "memos.memo.created",
		URL:           "http://example.invalid/hook",
		Status:        status,
		MaxAttempts:   3,
		NextDueTs:     nextDueTs,
		RequestBody:   `{"url":"http://example.invalid/hook"}`,
		SigningSecret: "whsec_test",
	}
}

func TestWebhookDeliveryCreateAndList(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)

	created, err := ts.CreateWebhookDelivery(ctx, newTestDelivery(42, "hook-a", store.WebhookDeliveryPending, 100))
	require.NoError(t, err)
	require.NotZero(t, created.ID)
	require.NotZero(t, created.CreatedTs)
	require.NotZero(t, created.UpdatedTs)
	require.Equal(t, store.WebhookDeliveryPending, created.Status)
	require.Equal(t, int32(0), created.Attempts)
	require.Equal(t, `{"url":"http://example.invalid/hook"}`, created.RequestBody)
	require.Equal(t, "whsec_test", created.SigningSecret)

	// A second row for the same hook and one for another hook.
	_, err = ts.CreateWebhookDelivery(ctx, newTestDelivery(42, "hook-a", store.WebhookDeliverySucceeded, 0))
	require.NoError(t, err)
	_, err = ts.CreateWebhookDelivery(ctx, newTestDelivery(42, "hook-b", store.WebhookDeliveryPending, 100))
	require.NoError(t, err)

	userID := int32(42)
	hookA := "hook-a"
	list, err := ts.ListWebhookDeliveries(ctx, &store.FindWebhookDeliveries{UserID: &userID, WebhookID: &hookA})
	require.NoError(t, err)
	require.Len(t, list, 2)
	// Newest first.
	require.Greater(t, list[0].ID, list[1].ID)

	succeeded := store.WebhookDeliverySucceeded
	list, err = ts.ListWebhookDeliveries(ctx, &store.FindWebhookDeliveries{UserID: &userID, WebhookID: &hookA, Status: &succeeded})
	require.NoError(t, err)
	require.Len(t, list, 1)
	require.Equal(t, store.WebhookDeliverySucceeded, list[0].Status)

	// Limit/offset paginate over the two rows.
	limit := 1
	page1, err := ts.ListWebhookDeliveries(ctx, &store.FindWebhookDeliveries{UserID: &userID, WebhookID: &hookA, Limit: &limit})
	require.NoError(t, err)
	require.Len(t, page1, 1)
	offset := 1
	page2, err := ts.ListWebhookDeliveries(ctx, &store.FindWebhookDeliveries{UserID: &userID, WebhookID: &hookA, Limit: &limit, Offset: &offset})
	require.NoError(t, err)
	require.Len(t, page2, 1)
	require.NotEqual(t, page1[0].ID, page2[0].ID)
}

func TestWebhookDeliveryClaimDue(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)

	now := int64(1000)
	due, err := ts.CreateWebhookDelivery(ctx, newTestDelivery(1, "h", store.WebhookDeliveryPending, now))
	require.NoError(t, err)
	future, err := ts.CreateWebhookDelivery(ctx, newTestDelivery(1, "h", store.WebhookDeliveryPending, now+100))
	require.NoError(t, err)
	succeeded, err := ts.CreateWebhookDelivery(ctx, newTestDelivery(1, "h", store.WebhookDeliverySucceeded, now))
	require.NoError(t, err)

	claimed, err := ts.ClaimDueWebhookDeliveries(ctx, now, 10)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	require.Equal(t, due.ID, claimed[0].ID)
	require.Equal(t, store.WebhookDeliveryDelivering, claimed[0].Status)
	require.Equal(t, int32(1), claimed[0].Attempts)
	require.Equal(t, now, claimed[0].LastAttemptTs)

	// Future and succeeded rows stay untouched.
	stored, err := ts.ListWebhookDeliveries(ctx, &store.FindWebhookDeliveries{ID: &future.ID})
	require.NoError(t, err)
	require.Equal(t, store.WebhookDeliveryPending, stored[0].Status)
	stored, err = ts.ListWebhookDeliveries(ctx, &store.FindWebhookDeliveries{ID: &succeeded.ID})
	require.NoError(t, err)
	require.Equal(t, store.WebhookDeliverySucceeded, stored[0].Status)

	// Nothing due now: the claimed row is DELIVERING and the other is still future.
	claimed, err = ts.ClaimDueWebhookDeliveries(ctx, now, 10)
	require.NoError(t, err)
	require.Empty(t, claimed)
}

func TestWebhookDeliveryClaimIsExclusive(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)

	delivery, err := ts.CreateWebhookDelivery(ctx, newTestDelivery(1, "h", store.WebhookDeliveryRetrying, 1))
	require.NoError(t, err)

	const goroutines = 16
	var wg sync.WaitGroup
	results := make(chan int64, goroutines)
	start := make(chan struct{})
	for range goroutines {
		wg.Go(func() {
			<-start
			claimed, claimErr := ts.ClaimDueWebhookDeliveries(ctx, 10, 10)
			require.NoError(t, claimErr)
			for _, row := range claimed {
				results <- row.ID
			}
		})
	}
	close(start)
	wg.Wait()
	close(results)

	winners := map[int64]int{}
	for id := range results {
		winners[id]++
	}
	require.Len(t, winners, 1)
	require.Equal(t, 1, winners[delivery.ID], "exactly one worker claims the row")
}

func TestWebhookDeliverySettleGuards(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)

	delivery, err := ts.CreateWebhookDelivery(ctx, newTestDelivery(1, "h", store.WebhookDeliveryPending, 1))
	require.NoError(t, err)
	claimed, err := ts.ClaimDueWebhookDeliveries(ctx, 1, 10)
	require.NoError(t, err)
	require.Len(t, claimed, 1)

	// A stale settlement with the wrong attempts count is rejected.
	ok, err := ts.SettleWebhookDelivery(ctx, &store.SettleWebhookDelivery{
		ID: delivery.ID, Attempts: 99, Status: store.WebhookDeliverySucceeded, SettledTs: 5,
	})
	require.NoError(t, err)
	require.False(t, ok)

	// Settlement with the current attempts count wins.
	ok, err = ts.SettleWebhookDelivery(ctx, &store.SettleWebhookDelivery{
		ID: delivery.ID, Attempts: 1, Status: store.WebhookDeliverySucceeded, SettledTs: 5,
	})
	require.NoError(t, err)
	require.True(t, ok)

	stored, err := ts.ListWebhookDeliveries(ctx, &store.FindWebhookDeliveries{ID: &delivery.ID})
	require.NoError(t, err)
	require.Equal(t, store.WebhookDeliverySucceeded, stored[0].Status)
	require.Equal(t, int64(5), stored[0].SettledTs)
	require.Empty(t, stored[0].LastError)

	// A second settlement is rejected because the row is no longer DELIVERING.
	ok, err = ts.SettleWebhookDelivery(ctx, &store.SettleWebhookDelivery{
		ID: delivery.ID, Attempts: 1, Status: store.WebhookDeliverySucceeded, SettledTs: 6,
	})
	require.NoError(t, err)
	require.False(t, ok)
}

func TestWebhookDeliveryRetryAndAbandonSettlement(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)

	delivery, err := ts.CreateWebhookDelivery(ctx, newTestDelivery(1, "h", store.WebhookDeliveryPending, 1))
	require.NoError(t, err)
	_, err = ts.ClaimDueWebhookDeliveries(ctx, 1, 10)
	require.NoError(t, err)

	ok, err := ts.SettleWebhookDelivery(ctx, &store.SettleWebhookDelivery{
		ID: delivery.ID, Attempts: 1, Status: store.WebhookDeliveryRetrying, NextDueTs: 42, LastError: "status code: 500",
	})
	require.NoError(t, err)
	require.True(t, ok)
	stored, err := ts.ListWebhookDeliveries(ctx, &store.FindWebhookDeliveries{ID: &delivery.ID})
	require.NoError(t, err)
	require.Equal(t, store.WebhookDeliveryRetrying, stored[0].Status)
	require.Equal(t, int64(42), stored[0].NextDueTs)
	require.Equal(t, "status code: 500", stored[0].LastError)
	require.Zero(t, stored[0].SettledTs)

	// Before due the row is not claimed; after due it is.
	claimed, err := ts.ClaimDueWebhookDeliveries(ctx, 41, 10)
	require.NoError(t, err)
	require.Empty(t, claimed)
	claimed, err = ts.ClaimDueWebhookDeliveries(ctx, 42, 10)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	require.Equal(t, int32(2), claimed[0].Attempts)

	ok, err = ts.SettleWebhookDelivery(ctx, &store.SettleWebhookDelivery{
		ID: delivery.ID, Attempts: 2, Status: store.WebhookDeliveryAbandoned, LastError: "status code: 500", SettledTs: 50,
	})
	require.NoError(t, err)
	require.True(t, ok)
	stored, err = ts.ListWebhookDeliveries(ctx, &store.FindWebhookDeliveries{ID: &delivery.ID})
	require.NoError(t, err)
	require.Equal(t, store.WebhookDeliveryAbandoned, stored[0].Status)
	require.Equal(t, int64(50), stored[0].SettledTs)
}

func TestWebhookDeliveryRecoverInterrupted(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)

	interrupted, err := ts.CreateWebhookDelivery(ctx, newTestDelivery(1, "h", store.WebhookDeliveryDelivering, 1))
	require.NoError(t, err)
	pending, err := ts.CreateWebhookDelivery(ctx, newTestDelivery(1, "h", store.WebhookDeliveryPending, 100))
	require.NoError(t, err)
	retrying, err := ts.CreateWebhookDelivery(ctx, newTestDelivery(1, "h", store.WebhookDeliveryRetrying, 100))
	require.NoError(t, err)
	succeeded, err := ts.CreateWebhookDelivery(ctx, newTestDelivery(1, "h", store.WebhookDeliverySucceeded, 0))
	require.NoError(t, err)

	recovered, err := ts.RecoverInterruptedDeliveries(ctx, 55)
	require.NoError(t, err)
	require.Equal(t, int64(1), recovered)

	got, err := ts.ListWebhookDeliveries(ctx, &store.FindWebhookDeliveries{ID: &interrupted.ID})
	require.NoError(t, err)
	require.Equal(t, store.WebhookDeliveryRetrying, got[0].Status)
	require.Equal(t, int64(55), got[0].NextDueTs)

	for _, id := range []int64{pending.ID, retrying.ID, succeeded.ID} {
		rows, listErr := ts.ListWebhookDeliveries(ctx, &store.FindWebhookDeliveries{ID: &id})
		require.NoError(t, listErr)
		require.NotEqual(t, store.WebhookDeliveryDelivering, rows[0].Status)
		if id == succeeded.ID {
			require.Equal(t, store.WebhookDeliverySucceeded, rows[0].Status)
		}
	}

	// The recovered row is immediately claimable.
	claimed, err := ts.ClaimDueWebhookDeliveries(ctx, 55, 10)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	require.Equal(t, interrupted.ID, claimed[0].ID)
}

func abandonDelivery(ctx context.Context, t *testing.T, ts *store.Store, id int64) {
	t.Helper()
	claimed, err := ts.ClaimDueWebhookDeliveries(ctx, 1, 10)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	ok, err := ts.SettleWebhookDelivery(ctx, &store.SettleWebhookDelivery{
		ID: id, Attempts: claimed[0].Attempts, Status: store.WebhookDeliveryAbandoned, LastError: "boom", SettledTs: 90,
	})
	require.NoError(t, err)
	require.True(t, ok)
}

func TestWebhookDeliveryReplay(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)

	abandoned, err := ts.CreateWebhookDelivery(ctx, newTestDelivery(1, "h", store.WebhookDeliveryPending, 1))
	require.NoError(t, err)
	abandonDelivery(ctx, t, ts, abandoned.ID)

	replayed, err := ts.ReplayWebhookDelivery(ctx, abandoned.ID)
	require.NoError(t, err)
	require.NotNil(t, replayed)
	require.Equal(t, store.WebhookDeliveryPending, replayed.Status)
	require.Equal(t, int32(0), replayed.Attempts)
	require.Empty(t, replayed.LastError)
	require.Zero(t, replayed.LastAttemptTs)
	require.Zero(t, replayed.SettledTs)
	require.NotZero(t, replayed.NextDueTs)
	require.Equal(t, abandoned.CreatedTs, replayed.CreatedTs, "registration time is preserved")

	// The replayed row is schedulable again.
	claimed, err := ts.ClaimDueWebhookDeliveries(ctx, replayed.NextDueTs, 10)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	require.Equal(t, abandoned.ID, claimed[0].ID)

	// A RETRYING row cannot be replayed.
	retrying, err := ts.CreateWebhookDelivery(ctx, newTestDelivery(1, "h", store.WebhookDeliveryPending, 1))
	require.NoError(t, err)
	claimed2, err := ts.ClaimDueWebhookDeliveries(ctx, 1, 10)
	require.NoError(t, err)
	require.Len(t, claimed2, 1)
	require.Equal(t, retrying.ID, claimed2[0].ID)
	ok, err := ts.SettleWebhookDelivery(ctx, &store.SettleWebhookDelivery{
		ID: retrying.ID, Attempts: 1, Status: store.WebhookDeliveryRetrying, NextDueTs: 999999, LastError: "later",
	})
	require.NoError(t, err)
	require.True(t, ok)
	replayed, err = ts.ReplayWebhookDelivery(ctx, retrying.ID)
	require.NoError(t, err)
	require.Nil(t, replayed)

	// Succeeded rows cannot be replayed.
	succeeded, err := ts.CreateWebhookDelivery(ctx, newTestDelivery(1, "h", store.WebhookDeliverySucceeded, 0))
	require.NoError(t, err)
	replayed, err = ts.ReplayWebhookDelivery(ctx, succeeded.ID)
	require.NoError(t, err)
	require.Nil(t, replayed)

	// Replaying a non-existent row reports no match.
	missing, err := ts.ReplayWebhookDelivery(ctx, abandoned.ID+99999)
	require.NoError(t, err)
	require.Nil(t, missing)
}

func TestWebhookDeliveryPrune(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)

	var userID int32 = 7
	const hookA = "hook-a"
	const hookB = "hook-b"
	// 4 terminal rows + 1 in-flight row for hook A; 2 terminal rows for hook B.
	var oldestTerminalA int64
	for i := range 4 {
		row, err := ts.CreateWebhookDelivery(ctx, newTestDelivery(userID, hookA, store.WebhookDeliverySucceeded, 0))
		require.NoError(t, err)
		if i == 0 {
			oldestTerminalA = row.ID
		}
	}
	inflight, err := ts.CreateWebhookDelivery(ctx, newTestDelivery(userID, hookA, store.WebhookDeliveryRetrying, 100))
	require.NoError(t, err)
	for range 2 {
		_, err = ts.CreateWebhookDelivery(ctx, newTestDelivery(userID, hookB, store.WebhookDeliveryAbandoned, 0))
		require.NoError(t, err)
	}

	require.NoError(t, ts.PruneWebhookDeliveries(ctx, userID, hookA, 3))

	hookA2 := hookA
	remaining, err := ts.ListWebhookDeliveries(ctx, &store.FindWebhookDeliveries{UserID: &userID, WebhookID: &hookA2})
	require.NoError(t, err)
	require.Len(t, remaining, 4, "three newest terminal rows plus the in-flight row")
	remainingIDs := map[int64]bool{}
	for _, row := range remaining {
		require.NotEqual(t, oldestTerminalA, row.ID, "the oldest terminal row is pruned")
		remainingIDs[row.ID] = true
	}
	require.True(t, remainingIDs[inflight.ID], "non-terminal rows are never pruned")

	// Other hooks are unaffected by pruning.
	hookB2 := hookB
	remainingB, err := ts.ListWebhookDeliveries(ctx, &store.FindWebhookDeliveries{UserID: &userID, WebhookID: &hookB2})
	require.NoError(t, err)
	require.Len(t, remainingB, 2)

	// Pruning again is a no-op when nothing exceeds the cap.
	require.NoError(t, ts.PruneWebhookDeliveries(ctx, userID, hookA, 3))
	remaining, err = ts.ListWebhookDeliveries(ctx, &store.FindWebhookDeliveries{UserID: &userID, WebhookID: &hookA2})
	require.NoError(t, err)
	require.Len(t, remaining, 4)
}
