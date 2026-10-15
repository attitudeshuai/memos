package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/usememos/memos/store"
)

func TestIdempotencyKeyLeaderElection(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)
	defer ts.Close()

	record, leader, err := ts.AcquireIdempotencyKey(ctx, 101, "leader-key", "hash-a")
	require.NoError(t, err)
	require.True(t, leader)
	require.Equal(t, store.IdempotencyStatusInFlight, record.Status)
	require.Equal(t, "hash-a", record.RequestHash)
	require.Greater(t, record.ExpiresTs, record.CreatedTs)

	// A second claim while the leader is in flight follows the leader instead
	// of overwriting its record.
	follower, leader, err := ts.AcquireIdempotencyKey(ctx, 101, "leader-key", "hash-a")
	require.NoError(t, err)
	require.False(t, leader)
	require.Equal(t, record.ID, follower.ID)
	require.Equal(t, store.IdempotencyStatusInFlight, follower.Status)
}

func TestIdempotencyKeyIsScopedPerCreator(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)
	defer ts.Close()

	first, leader, err := ts.AcquireIdempotencyKey(ctx, 101, "shared-key", "hash-a")
	require.NoError(t, err)
	require.True(t, leader)
	second, leader, err := ts.AcquireIdempotencyKey(ctx, 202, "shared-key", "hash-a")
	require.NoError(t, err)
	require.True(t, leader, "another creator must own an independent claim")
	require.NotEqual(t, first.ID, second.ID)
}

func TestIdempotencyKeyCompleteReplaysResponse(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)
	defer ts.Close()

	record, leader, err := ts.AcquireIdempotencyKey(ctx, 101, "complete-key", "hash-a")
	require.NoError(t, err)
	require.True(t, leader)

	response := []byte{0x0a, 0x03, 'a', 'b', 'c'}
	require.NoError(t, ts.CompleteIdempotencyKey(ctx, record.ID, 101, "complete-key", response))

	stored, err := ts.GetIdempotencyKey(ctx, 101, "complete-key")
	require.NoError(t, err)
	require.NotNil(t, stored)
	require.Equal(t, store.IdempotencyStatusCompleted, stored.Status)
	require.Equal(t, response, stored.Response)

	// A completed claim is never completed twice.
	err = ts.CompleteIdempotencyKey(ctx, record.ID, 101, "complete-key", response)
	require.ErrorIs(t, err, store.ErrIdempotencyKeyNotInFlight)

	// Re-acquiring within the retention window follows the completed record.
	replayed, leader, err := ts.AcquireIdempotencyKey(ctx, 101, "complete-key", "hash-a")
	require.NoError(t, err)
	require.False(t, leader)
	require.Equal(t, store.IdempotencyStatusCompleted, replayed.Status)
	require.Equal(t, response, replayed.Response)

	got, err := ts.GetIdempotencyKey(ctx, 999, "complete-key")
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestIdempotencyKeyReleaseAllowsRetry(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)
	defer ts.Close()

	first, leader, err := ts.AcquireIdempotencyKey(ctx, 101, "release-key", "hash-a")
	require.NoError(t, err)
	require.True(t, leader)

	require.NoError(t, ts.ReleaseIdempotencyKey(ctx, first.ID, 101, "release-key"))
	got, err := ts.GetIdempotencyKey(ctx, 101, "release-key")
	require.NoError(t, err)
	require.Nil(t, got)

	again, leader, err := ts.AcquireIdempotencyKey(ctx, 101, "release-key", "hash-a")
	require.NoError(t, err)
	require.True(t, leader)
	require.Equal(t, store.IdempotencyStatusInFlight, again.Status)

	// Releasing an absent claim or a completed row is a no-op.
	require.NoError(t, ts.ReleaseIdempotencyKey(ctx, 999999, 101, "never-claimed"))
	require.NoError(t, ts.CompleteIdempotencyKey(ctx, again.ID, 101, "release-key", []byte("ok")))
	require.NoError(t, ts.ReleaseIdempotencyKey(ctx, again.ID, 101, "release-key"))
	completed, err := ts.GetIdempotencyKey(ctx, 101, "release-key")
	require.NoError(t, err)
	require.Equal(t, store.IdempotencyStatusCompleted, completed.Status)
}

func TestIdempotencyKeyExpiredRecordIsPurged(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)
	defer ts.Close()

	expired, leader, err := ts.AcquireIdempotencyKey(ctx, 101, "expired-key", "hash-a")
	require.NoError(t, err)
	require.True(t, leader)
	require.NoError(t, ts.CompleteIdempotencyKey(ctx, expired.ID, 101, "expired-key", []byte("old")))

	// Force the record beyond its retention window.
	expireIdempotencyRecord(ctx, t, ts, 101, "expired-key", "expires_ts = expires_ts - 100000")

	reacquired, leader, err := ts.AcquireIdempotencyKey(ctx, 101, "expired-key", "hash-a")
	require.NoError(t, err)
	require.True(t, leader, "an expired claim must be reclaimable")
	require.Empty(t, reacquired.Response)

	// The janitor delete path removes expired rows directly.
	_, leader, err = ts.AcquireIdempotencyKey(ctx, 101, "sweep-key", "hash-a")
	require.NoError(t, err)
	require.True(t, leader)
	expireIdempotencyRecord(ctx, t, ts, 101, "sweep-key", "expires_ts = expires_ts - 100000")
	removed, err := ts.DeleteExpiredIdempotencyKeys(ctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, removed, int64(1))
	got, err := ts.GetIdempotencyKey(ctx, 101, "sweep-key")
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestIdempotencyKeyStaleInFlightIsTakenOver(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)
	defer ts.Close()

	original, leader, err := ts.AcquireIdempotencyKey(ctx, 101, "stuck-key", "hash-a")
	require.NoError(t, err)
	require.True(t, leader)

	// Simulate a leader that stopped reporting well before the takeover cut.
	expireIdempotencyRecord(ctx, t, ts, 101, "stuck-key", "created_ts = 1")

	takeover, leader, err := ts.AcquireIdempotencyKey(ctx, 101, "stuck-key", "hash-a")
	require.NoError(t, err)
	require.True(t, leader, "a stale IN_FLIGHT claim must be takeable")
	require.NotEqual(t, original.ID, takeover.ID)

	// The old leader can no longer complete the claim it lost.
	err = ts.CompleteIdempotencyKey(ctx, original.ID, 101, "stuck-key", []byte("late"))
	require.ErrorIs(t, err, store.ErrIdempotencyKeyNotInFlight)

	// The new leader completes its own claim normally.
	require.NoError(t, ts.CompleteIdempotencyKey(ctx, takeover.ID, 101, "stuck-key", []byte("new")))
	stored, err := ts.GetIdempotencyKey(ctx, 101, "stuck-key")
	require.NoError(t, err)
	require.Equal(t, store.IdempotencyStatusCompleted, stored.Status)
	require.Equal(t, []byte("new"), stored.Response)
}

func TestIdempotencyKeyRejectsEmptyArguments(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)
	defer ts.Close()

	_, _, err := ts.AcquireIdempotencyKey(ctx, 101, "", "hash-a")
	require.Error(t, err)
	_, _, err = ts.AcquireIdempotencyKey(ctx, 0, "key", "hash-a")
	require.Error(t, err)
}

// expireIdempotencyRecord runs a dialect-neutral UPDATE against one record.
func expireIdempotencyRecord(ctx context.Context, t *testing.T, ts *store.Store, creatorID int32, key, setClause string) {
	t.Helper()
	query := "UPDATE idempotency_key SET " + setClause + " WHERE creator_id = ? AND idempotency_key = ?"
	if getDriverFromEnv() == "postgres" {
		query = "UPDATE idempotency_key SET " + setClause + " WHERE creator_id = $1 AND idempotency_key = $2"
	}
	_, err := ts.GetDriver().GetDB().ExecContext(ctx, query, creatorID, key)
	require.NoError(t, err)
}
