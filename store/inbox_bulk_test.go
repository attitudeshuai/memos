package store

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProcessInboxIDChunks(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("drains in ascending chunks", func(t *testing.T) {
		t.Parallel()
		pending := []int32{1, 2, 3, 4, 5}
		var appliedBatches [][]int32
		var affectedTotal int32
		listChunk := func(_ context.Context, limit int) ([]int32, error) {
			if len(pending) == 0 {
				return nil, nil
			}
			n := limit
			if n > len(pending) {
				n = len(pending)
			}
			ids := append([]int32(nil), pending[:n]...)
			return ids, nil
		}
		applyChunk := func(_ context.Context, ids []int32) (int32, error) {
			copied := append([]int32(nil), ids...)
			appliedBatches = append(appliedBatches, copied)
			affectedTotal += int32(len(ids))
			pending = pending[len(ids):]
			return int32(len(ids)), nil
		}

		affected, lastID, err := processInboxIDChunks(ctx, 2, listChunk, applyChunk)
		require.NoError(t, err)
		require.Equal(t, int32(5), affected)
		require.Equal(t, int32(5), lastID)
		require.Equal(t, [][]int32{{1, 2}, {3, 4}, {5}}, appliedBatches)
		require.Equal(t, int32(5), affectedTotal)
	})

	t.Run("empty match returns zero boundary", func(t *testing.T) {
		t.Parallel()
		listChunk := func(_ context.Context, _ int) ([]int32, error) { return nil, nil }
		applyChunk := func(_ context.Context, _ []int32) (int32, error) {
			t.Fatal("apply must not be called")
			return 0, nil
		}
		affected, lastID, err := processInboxIDChunks(ctx, 10, listChunk, applyChunk)
		require.NoError(t, err)
		require.Equal(t, int32(0), affected)
		require.Equal(t, int32(0), lastID)
	})

	t.Run("failure leaves committed prefix boundary", func(t *testing.T) {
		t.Parallel()
		injected := errors.New("boom")
		pending := []int32{10, 20, 30, 40, 50, 60}
		listChunk := func(_ context.Context, limit int) ([]int32, error) {
			if len(pending) == 0 {
				return nil, nil
			}
			n := limit
			if n > len(pending) {
				n = len(pending)
			}
			ids := append([]int32(nil), pending[:n]...)
			return ids, nil
		}
		var calls int
		applyChunk := func(_ context.Context, ids []int32) (int32, error) {
			calls++
			if calls == 2 {
				return 0, injected
			}
			pending = pending[len(ids):]
			return int32(len(ids)), nil
		}

		affected, lastID, err := processInboxIDChunks(ctx, 2, listChunk, applyChunk)
		require.ErrorIs(t, err, injected)
		// First chunk (10, 20) committed; the failing second chunk does not count.
		require.Equal(t, int32(2), affected)
		require.Equal(t, int32(20), lastID)
	})

	t.Run("retry after partial failure completes idempotently", func(t *testing.T) {
		t.Parallel()
		// Simulate a retry: the first batch's rows no longer match the filter,
		// so listing resumes at the remainder.
		remaining := []int32{30, 40, 50}
		listChunk := func(_ context.Context, limit int) ([]int32, error) {
			if len(remaining) == 0 {
				return nil, nil
			}
			n := limit
			if n > len(remaining) {
				n = len(remaining)
			}
			ids := append([]int32(nil), remaining[:n]...)
			return ids, nil
		}
		applyChunk := func(_ context.Context, ids []int32) (int32, error) {
			remaining = remaining[len(ids):]
			return int32(len(ids)), nil
		}

		affected, lastID, err := processInboxIDChunks(ctx, 5, listChunk, applyChunk)
		require.NoError(t, err)
		// 2 from the first attempt plus 3 on retry equals the full set of 5.
		require.Equal(t, int32(3), affected)
		require.Equal(t, int32(50), lastID)
	})

	t.Run("list error returns accumulated boundary", func(t *testing.T) {
		t.Parallel()
		injected := errors.New("list boom")
		var calls int
		listChunk := func(_ context.Context, _ int) ([]int32, error) {
			calls++
			if calls == 1 {
				// A full chunk forces a second listing call.
				return []int32{1}, nil
			}
			return nil, injected
		}
		applyChunk := func(_ context.Context, ids []int32) (int32, error) {
			return int32(len(ids)), nil
		}
		affected, lastID, err := processInboxIDChunks(ctx, 1, listChunk, applyChunk)
		require.ErrorIs(t, err, injected)
		require.Equal(t, int32(1), affected)
		require.Equal(t, int32(1), lastID)
	})
}
