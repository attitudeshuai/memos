package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/usememos/memos/store"
)

// memoKeysetTS returns the sort-key timestamp of a row under the ordering.
func memoKeysetTS(m *store.Memo, byUpdate bool) int64 {
	if byUpdate {
		return m.UpdatedTs
	}
	return m.CreatedTs
}

// walkMemoKeyset pages through every row matching base using keyset cursors,
// mirroring how the API layer builds cursors from each page's last row.
func walkMemoKeyset(ctx context.Context, t *testing.T, ts *store.Store, base store.FindMemo, pageSize int) []*store.Memo {
	t.Helper()

	var collected []*store.Memo
	var cursor *store.MemoKeyset
	for {
		find := base
		limit := pageSize + 1
		find.Limit = &limit
		find.Offset = nil
		find.Cursor = cursor
		page, err := ts.ListMemos(ctx, &find)
		require.NoError(t, err)

		hasMore := len(page) == pageSize+1
		if hasMore {
			page = page[:pageSize]
		}
		collected = append(collected, page...)
		if !hasMore {
			break
		}
		require.NotEmpty(t, page)
		last := page[len(page)-1]
		cursor = &store.MemoKeyset{
			LastPinned: last.Pinned,
			LastTs:     memoKeysetTS(last, base.OrderByUpdatedTs),
			LastID:     last.ID,
		}
	}
	return collected
}

// seedKeysetMemos creates memos with deliberate timestamp ties (triples on
// created_ts, pairs on updated_ts) and two pinned rows.
func seedKeysetMemos(ctx context.Context, t *testing.T, ts *store.Store, user *store.User, n int) []*store.Memo {
	t.Helper()
	created := make([]*store.Memo, 0, n)
	for i := range n {
		visibility := store.Private
		if i%2 == 0 {
			visibility = store.Public
		}
		memo, err := ts.CreateMemo(ctx, &store.Memo{
			UID:        fmt.Sprintf("keyset-memo-%d", i),
			CreatorID:  user.ID,
			Content:    fmt.Sprintf("keyset content %d", i),
			Visibility: visibility,
			CreatedTs:  1000 + int64(i/3)*10,
			UpdatedTs:  5000 + int64(i/2)*7,
		})
		require.NoError(t, err)
		created = append(created, memo)
	}
	for _, i := range []int{2, n - 3} {
		pinned := true
		require.NoError(t, ts.UpdateMemo(ctx, &store.UpdateMemo{ID: created[i].ID, Pinned: &pinned}))
	}
	return created
}

func TestMemoKeysetPagination(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)
	defer ts.Close()
	user, err := createTestingHostUser(ctx, ts)
	require.NoError(t, err)

	const n = 23
	seedKeysetMemos(ctx, t, ts, user, n)

	for _, pageSize := range []int{1, 5, 100} {
		for _, flags := range []struct {
			name           string
			orderByUpdated bool
			orderByTimeAsc bool
			orderByPinned  bool
			filterExpr     string
		}{
			{"create_time desc", false, false, false, ""},
			{"create_time asc", false, true, false, ""},
			{"update_time desc", true, false, false, ""},
			{"update_time asc", true, true, false, ""},
			{"pinned then create_time desc", false, false, true, ""},
			{"pinned then create_time asc", false, true, true, ""},
			{"create_time desc with filter", false, false, false, `visibility == "PUBLIC"`},
		} {
			t.Run(fmt.Sprintf("%s/pagesize_%d", flags.name, pageSize), func(t *testing.T) {
				base := store.FindMemo{
					CreatorID:        &user.ID,
					OrderByUpdatedTs: flags.orderByUpdated,
					OrderByTimeAsc:   flags.orderByTimeAsc,
					OrderByPinned:    flags.orderByPinned,
				}
				if flags.filterExpr != "" {
					base.Filters = []string{flags.filterExpr}
				}

				full, err := ts.ListMemos(ctx, &base)
				require.NoError(t, err)
				if flags.filterExpr != "" {
					// Half of the seeded rows are PUBLIC.
					require.NotZero(t, len(full))
					for _, m := range full {
						require.Equal(t, store.Public, m.Visibility)
					}
				} else {
					require.Len(t, full, n)
				}

				walked := walkMemoKeyset(ctx, t, ts, base, pageSize)
				require.Equal(t, memoIDs(full), memoIDs(walked))

				// Pages must never repeat a row.
				seen := make(map[int32]struct{}, len(walked))
				for _, m := range walked {
					_, dup := seen[m.ID]
					require.False(t, dup, "duplicate memo id %d across pages", m.ID)
					seen[m.ID] = struct{}{}
				}
			})
		}
	}
}

func TestMemoKeysetTieBreakerStability(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)
	defer ts.Close()
	user, err := createTestingHostUser(ctx, ts)
	require.NoError(t, err)

	// Six rows on the same created_ts; only the id tie-breaker orders them.
	const tiedTs = int64(3000)
	for i := range 6 {
		_, err := ts.CreateMemo(ctx, &store.Memo{
			UID:        fmt.Sprintf("tied-%d", i),
			CreatorID:  user.ID,
			Content:    "tied",
			Visibility: store.Public,
			CreatedTs:  tiedTs,
			UpdatedTs:  tiedTs,
		})
		require.NoError(t, err)
	}

	for _, asc := range []bool{false, true} {
		base := store.FindMemo{CreatorID: &user.ID, OrderByTimeAsc: asc}
		full, err := ts.ListMemos(ctx, &base)
		require.NoError(t, err)
		require.Len(t, full, 6)
		for _, m := range full {
			require.Equal(t, tiedTs, m.CreatedTs)
		}
		walked := walkMemoKeyset(ctx, t, ts, base, 2)
		require.Equal(t, memoIDs(full), memoIDs(walked))
	}
}

func TestMemoKeysetConcurrentArchiveAndInsert(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)
	defer ts.Close()
	user, err := createTestingHostUser(ctx, ts)
	require.NoError(t, err)

	normal := store.Normal
	const n = 12
	for i := range n {
		_, err := ts.CreateMemo(ctx, &store.Memo{
			UID:        fmt.Sprintf("concurrent-%d", i),
			CreatorID:  user.ID,
			Content:    "concurrent",
			Visibility: store.Public,
			CreatedTs:  2000 + int64(i)*10,
			UpdatedTs:  2000 + int64(i)*10,
		})
		require.NoError(t, err)
	}

	base := store.FindMemo{CreatorID: &user.ID, RowStatus: &normal}
	full, err := ts.ListMemos(ctx, &base)
	require.NoError(t, err)
	require.Len(t, full, n)

	// Fetch the first page, then mutate the list before continuing.
	firstLimit := 5
	firstProbe := firstLimit + 1
	firstPage, err := ts.ListMemos(ctx, &store.FindMemo{
		CreatorID: &user.ID,
		RowStatus: &normal,
		Limit:     &firstProbe,
	})
	require.NoError(t, err)
	require.Len(t, firstPage, firstProbe)
	firstPage = firstPage[:firstLimit]
	last := firstPage[firstLimit-1]
	cursor := &store.MemoKeyset{LastTs: last.CreatedTs, LastID: last.ID}

	// Archive a row that has not been visited yet.
	archivedRow := full[8]
	archived := store.Archived
	require.NoError(t, ts.UpdateMemo(ctx, &store.UpdateMemo{ID: archivedRow.ID, RowStatus: &archived}))

	t.Run("newer_insert_is_not_revisited", func(t *testing.T) {
		// A newer-than-cursor row belongs above the current position and must
		// not surface on later pages.
		newer, err := ts.CreateMemo(ctx, &store.Memo{
			UID:        "concurrent-newer",
			CreatorID:  user.ID,
			Content:    "newer",
			Visibility: store.Public,
			CreatedTs:  9000,
			UpdatedTs:  9000,
		})
		require.NoError(t, err)

		rest := walkMemoKeysetFrom(ctx, t, ts, base, cursor, firstLimit)
		expected := make([]int32, 0, n-firstLimit)
		for _, m := range full[firstLimit:] {
			if m.ID != archivedRow.ID {
				expected = append(expected, m.ID)
			}
		}
		require.Equal(t, expected, memoIDs(rest))
		for _, m := range rest {
			require.NotEqual(t, newer.ID, m.ID)
			require.NotEqual(t, archivedRow.ID, m.ID)
		}
	})

	t.Run("older_insert_appears_once_at_position", func(t *testing.T) {
		// An older-than-cursor row belongs below the current position and must
		// appear exactly once, at its correct keyset position.
		anchor := full[firstLimit]
		older, err := ts.CreateMemo(ctx, &store.Memo{
			UID:        "concurrent-older",
			CreatorID:  user.ID,
			Content:    "older",
			Visibility: store.Public,
			CreatedTs:  anchor.CreatedTs - 5,
			UpdatedTs:  anchor.CreatedTs - 5,
		})
		require.NoError(t, err)

		rest := walkMemoKeysetFrom(ctx, t, ts, base, cursor, firstLimit)

		expected := []int32{anchor.ID, older.ID}
		for _, m := range full[firstLimit+1:] {
			if m.ID != archivedRow.ID {
				expected = append(expected, m.ID)
			}
		}
		require.Equal(t, expected, memoIDs(rest))
	})
}

// walkMemoKeysetFrom continues paging from an existing cursor.
func walkMemoKeysetFrom(ctx context.Context, t *testing.T, ts *store.Store, base store.FindMemo, cursor *store.MemoKeyset, pageSize int) []*store.Memo {
	t.Helper()
	var collected []*store.Memo
	for {
		find := base
		limit := pageSize + 1
		find.Limit = &limit
		find.Cursor = cursor
		page, err := ts.ListMemos(ctx, &find)
		require.NoError(t, err)
		hasMore := len(page) == pageSize+1
		if hasMore {
			page = page[:pageSize]
		}
		collected = append(collected, page...)
		if !hasMore {
			break
		}
		last := page[len(page)-1]
		cursor = &store.MemoKeyset{LastTs: last.CreatedTs, LastID: last.ID}
	}
	return collected
}
