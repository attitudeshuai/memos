package test

import (
	"context"
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	apipb "github.com/usememos/memos/proto/gen/api"
	"github.com/usememos/memos/server/auth"
	"github.com/usememos/memos/store"
)

// walkListMemos follows next_page_token until the end and returns every memo.
func walkListMemos(t *testing.T, ctx context.Context, ts *TestService, first *apipb.ListMemosRequest) []*apipb.Memo {
	t.Helper()
	request := proto.Clone(first).(*apipb.ListMemosRequest)
	var collected []*apipb.Memo
	for {
		resp, err := ts.Service.ListMemos(ctx, request)
		require.NoError(t, err)
		collected = append(collected, resp.Memos...)
		if resp.NextPageToken == "" {
			break
		}
		request = &apipb.ListMemosRequest{PageToken: resp.NextPageToken}
	}
	return collected
}

func seedListMemoRows(t *testing.T, ctx context.Context, ts *TestService, user *store.User, n int, contentPrefix string) []*store.Memo {
	t.Helper()
	rows := make([]*store.Memo, 0, n)
	for i := range n {
		memo, err := ts.Store.CreateMemo(ctx, &store.Memo{
			UID:        fmt.Sprintf("%s-%d", contentPrefix, i),
			CreatorID:  user.ID,
			Content:    fmt.Sprintf("%s %d", contentPrefix, i),
			Visibility: store.Public,
			CreatedTs:  1000 + int64(i)*10,
			UpdatedTs:  8000 + int64(i)*10,
		})
		require.NoError(t, err)
		rows = append(rows, memo)
	}
	return rows
}

func memoNames(memos []*apipb.Memo) []string {
	names := make([]string, len(memos))
	for i, m := range memos {
		names[i] = m.Name
	}
	return names
}

func TestListMemosKeysetPagination(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestService(t)
	defer ts.Cleanup()
	host, err := ts.CreateHostUser(ctx, "keyset-host")
	require.NoError(t, err)
	hostCtx := context.WithValue(ctx, auth.UserIDContextKey, host.ID)

	const n = 12
	seedListMemoRows(t, ctx, ts, host, n, "keyset-walk")

	reference, err := ts.Service.ListMemos(hostCtx, &apipb.ListMemosRequest{PageSize: 1000})
	require.NoError(t, err)
	require.Len(t, reference.Memos, n)
	require.Empty(t, reference.NextPageToken)

	// Issued tokens are keyset tokens.
	first, err := ts.Service.ListMemos(hostCtx, &apipb.ListMemosRequest{PageSize: 5})
	require.NoError(t, err)
	require.Len(t, first.Memos, 5)
	require.NotEmpty(t, first.NextPageToken)
	cursor, err := decodeTestPageToken(first.NextPageToken)
	require.NoError(t, err)
	require.NotNil(t, cursor.GetMemoCursor())

	walked := walkListMemos(t, hostCtx, ts, &apipb.ListMemosRequest{PageSize: 5})
	require.Len(t, walked, n)
	require.Equal(t, memoNames(reference.Memos), memoNames(walked))

	// Pages must be disjoint.
	seen := map[string]struct{}{}
	for _, m := range walked {
		_, dup := seen[m.Name]
		require.False(t, dup, "duplicate memo %s across pages", m.Name)
		seen[m.Name] = struct{}{}
	}
}

func TestListMemosKeysetOrderings(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestService(t)
	defer ts.Cleanup()
	host, err := ts.CreateHostUser(ctx, "keyset-order-host")
	require.NoError(t, err)
	hostCtx := context.WithValue(ctx, auth.UserIDContextKey, host.ID)

	const n = 11
	seedListMemoRows(t, ctx, ts, host, n, "keyset-order")
	// Pin the third-newest row so pinned ordering has something to move.
	all, err := ts.Store.ListMemos(ctx, &store.FindMemo{CreatorID: &host.ID})
	require.NoError(t, err)
	pinnedID := all[2].ID
	pinned := true
	require.NoError(t, ts.Store.UpdateMemo(ctx, &store.UpdateMemo{ID: pinnedID, Pinned: &pinned}))

	for _, tc := range []struct {
		name    string
		orderBy string
	}{
		{"default desc", ""},
		{"create asc", "create_time asc"},
		{"update desc", "update_time desc"},
		{"pinned then desc", "pinned desc, create_time desc"},
		{"pinned then asc", "pinned, create_time asc"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := &apipb.ListMemosRequest{PageSize: 4, OrderBy: tc.orderBy}
			reference, err := ts.Service.ListMemos(hostCtx, &apipb.ListMemosRequest{PageSize: 1000, OrderBy: tc.orderBy})
			require.NoError(t, err)
			require.Len(t, reference.Memos, n)
			walked := walkListMemos(t, hostCtx, ts, req)
			require.Equal(t, memoNames(reference.Memos), memoNames(walked))

			if tc.orderBy != "" && tc.orderBy[:6] == "pinned" {
				require.True(t, walked[0].Pinned, "pinned memo must come first")
			}
		})
	}
}

func TestListMemosKeysetCombinesWithFilter(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestService(t)
	defer ts.Cleanup()
	host, err := ts.CreateHostUser(ctx, "keyset-filter-host")
	require.NoError(t, err)
	hostCtx := context.WithValue(ctx, auth.UserIDContextKey, host.ID)

	const n = 12
	for i := range n {
		content := fmt.Sprintf("keyset-skip %d", i)
		if i%2 == 0 {
			content = fmt.Sprintf("keyset-match %d", i)
		}
		_, err := ts.Store.CreateMemo(ctx, &store.Memo{
			UID:        fmt.Sprintf("keyset-filter-%d", i),
			CreatorID:  host.ID,
			Content:    content,
			Visibility: store.Public,
			CreatedTs:  1000 + int64(i),
			UpdatedTs:  1000 + int64(i),
		})
		require.NoError(t, err)
	}

	filter := `content.contains("keyset-match")`
	reference, err := ts.Service.ListMemos(hostCtx, &apipb.ListMemosRequest{PageSize: 1000, Filter: filter})
	require.NoError(t, err)
	require.Len(t, reference.Memos, n/2)

	walked := walkListMemos(t, hostCtx, ts, &apipb.ListMemosRequest{PageSize: 3, Filter: filter})
	require.Len(t, walked, n/2)
	require.Equal(t, memoNames(reference.Memos), memoNames(walked))
}

func TestListMemosKeysetEmptyAndLastPage(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestService(t)
	defer ts.Cleanup()
	host, err := ts.CreateHostUser(ctx, "keyset-empty-host")
	require.NoError(t, err)
	hostCtx := context.WithValue(ctx, auth.UserIDContextKey, host.ID)

	// Empty result: empty memo list and empty token.
	empty, err := ts.Service.ListMemos(hostCtx, &apipb.ListMemosRequest{PageSize: 10})
	require.NoError(t, err)
	require.Empty(t, empty.Memos)
	require.Empty(t, empty.NextPageToken)

	// Exact multiple of the page size: probe keeps the token until the true end.
	seedListMemoRows(t, ctx, ts, host, 10, "keyset-pages")
	page1, err := ts.Service.ListMemos(hostCtx, &apipb.ListMemosRequest{PageSize: 5})
	require.NoError(t, err)
	require.Len(t, page1.Memos, 5)
	require.NotEmpty(t, page1.NextPageToken)
	page2, err := ts.Service.ListMemos(hostCtx, &apipb.ListMemosRequest{PageToken: page1.NextPageToken})
	require.NoError(t, err)
	require.Len(t, page2.Memos, 5)
	require.Empty(t, page2.NextPageToken, "the last page must not return a token")
}

func TestListMemosLegacyOffsetTokenStillAccepted(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestService(t)
	defer ts.Cleanup()
	host, err := ts.CreateHostUser(ctx, "keyset-legacy-host")
	require.NoError(t, err)
	hostCtx := context.WithValue(ctx, auth.UserIDContextKey, host.ID)

	const n = 12
	seedListMemoRows(t, ctx, ts, host, n, "keyset-legacy")
	reference, err := ts.Service.ListMemos(hostCtx, &apipb.ListMemosRequest{PageSize: 1000})
	require.NoError(t, err)

	// Hand-craft a pre-upgrade offset token (limit=5, offset=5).
	raw, err := marshalTestPageToken(&apipb.PageToken{Limit: 5, Offset: 5})
	require.NoError(t, err)
	resp, err := ts.Service.ListMemos(hostCtx, &apipb.ListMemosRequest{PageToken: raw})
	require.NoError(t, err)
	require.Len(t, resp.Memos, 5)
	require.Equal(t, memoNames(reference.Memos[5:10]), memoNames(resp.Memos))

	// Offset tokens still chain to the final page.
	resp2, err := ts.Service.ListMemos(hostCtx, &apipb.ListMemosRequest{PageToken: resp.NextPageToken})
	require.NoError(t, err)
	require.Len(t, resp2.Memos, 2)
	require.Empty(t, resp2.NextPageToken)
}

func TestListMemosInvalidTokensRejected(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestService(t)
	defer ts.Cleanup()
	host, err := ts.CreateHostUser(ctx, "keyset-invalid-host")
	require.NoError(t, err)
	hostCtx := context.WithValue(ctx, auth.UserIDContextKey, host.ID)
	seedListMemoRows(t, ctx, ts, host, 8, "keyset-invalid")

	first, err := ts.Service.ListMemos(hostCtx, &apipb.ListMemosRequest{PageSize: 3})
	require.NoError(t, err)
	require.NotEmpty(t, first.NextPageToken)

	st := func(err error) codes.Code {
		s, ok := status.FromError(err)
		require.True(t, ok)
		return s.Code()
	}

	// Malformed garbage.
	_, err = ts.Service.ListMemos(hostCtx, &apipb.ListMemosRequest{PageToken: "!!!not-base64"})
	require.Equal(t, codes.InvalidArgument, st(err))
	require.Contains(t, err.Error(), "malformed")

	// Tampered token (signature mismatch).
	b, err := base64.StdEncoding.DecodeString(first.NextPageToken)
	require.NoError(t, err)
	b[len(b)-1] ^= 0xff
	forged := base64.StdEncoding.EncodeToString(b)
	_, err = ts.Service.ListMemos(hostCtx, &apipb.ListMemosRequest{PageToken: forged})
	require.Equal(t, codes.InvalidArgument, st(err))
	require.Contains(t, err.Error(), "signature")

	// Token replayed against a different order_by.
	_, err = ts.Service.ListMemos(hostCtx, &apipb.ListMemosRequest{
		PageToken: first.NextPageToken,
		OrderBy:   "create_time asc",
	})
	require.Equal(t, codes.InvalidArgument, st(err))
	require.Contains(t, err.Error(), "does not match")

	// Token replayed against a different filter.
	_, err = ts.Service.ListMemos(hostCtx, &apipb.ListMemosRequest{
		PageToken: first.NextPageToken,
		Filter:    `content.contains("nonexistent")`,
	})
	require.Equal(t, codes.InvalidArgument, st(err))
	require.Contains(t, err.Error(), "does not match")
}

func TestListMemosKeysetStableUnderConcurrentChanges(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestService(t)
	defer ts.Cleanup()
	host, err := ts.CreateHostUser(ctx, "keyset-concurrent-host")
	require.NoError(t, err)
	hostCtx := context.WithValue(ctx, auth.UserIDContextKey, host.ID)

	const n = 12
	rows := seedListMemoRows(t, ctx, ts, host, n, "keyset-concurrent")

	page1, err := ts.Service.ListMemos(hostCtx, &apipb.ListMemosRequest{PageSize: 5})
	require.NoError(t, err)
	require.Len(t, page1.Memos, 5)

	// Archive a not-yet-visited row and insert a newer row while paging.
	archived := store.Archived
	require.NoError(t, ts.Store.UpdateMemo(ctx, &store.UpdateMemo{ID: rows[1].ID, RowStatus: &archived}))
	_, err = ts.Store.CreateMemo(ctx, &store.Memo{
		UID:        "keyset-concurrent-new",
		CreatorID:  host.ID,
		Content:    "brand new",
		Visibility: store.Public,
		CreatedTs:  9000,
		UpdatedTs:  9000,
	})
	require.NoError(t, err)

	seen := map[string]struct{}{}
	for _, m := range page1.Memos {
		seen[m.Name] = struct{}{}
	}
	token := page1.NextPageToken
	for token != "" {
		page, err := ts.Service.ListMemos(hostCtx, &apipb.ListMemosRequest{PageToken: token})
		require.NoError(t, err)
		for _, m := range page.Memos {
			_, dup := seen[m.Name]
			require.False(t, dup, "duplicate memo %s after concurrent changes", m.Name)
			seen[m.Name] = struct{}{}
			require.NotEqual(t, "memos/"+rows[1].UID, m.Name, "archived memo must not be returned")
		}
		token = page.NextPageToken
	}
	// First page (5) plus the 6 surviving unvisited originals.
	require.Len(t, seen, 11)
}

func TestListMemosKeysetArchivedRestoresStateFromToken(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ts := NewTestService(t)
	defer ts.Cleanup()
	host, err := ts.CreateHostUser(ctx, "keyset-archived-host")
	require.NoError(t, err)
	hostCtx := context.WithValue(ctx, auth.UserIDContextKey, host.ID)
	other, err := ts.CreateRegularUser(ctx, "keyset-archived-other")
	require.NoError(t, err)
	otherCtx := context.WithValue(ctx, auth.UserIDContextKey, other.ID)

	rows := seedListMemoRows(t, ctx, ts, host, 6, "keyset-archived")
	for _, row := range rows[:3] {
		archived := store.Archived
		require.NoError(t, ts.Store.UpdateMemo(ctx, &store.UpdateMemo{ID: row.ID, RowStatus: &archived}))
	}
	// Another user also has an archived memo; it must never leak into host's
	// archived listing.
	otherRows := seedListMemoRows(t, ctx, ts, other, 2, "keyset-archived-other")
	archivedOther := store.Archived
	require.NoError(t, ts.Store.UpdateMemo(ctx, &store.UpdateMemo{ID: otherRows[0].ID, RowStatus: &archivedOther}))

	// The walk helper sends only PageToken on later pages; state and the
	// creator scope are restored from the signed token.
	walked := walkListMemos(t, hostCtx, ts, &apipb.ListMemosRequest{
		PageSize: 2,
		State:    apipb.State_ARCHIVED,
	})
	require.Len(t, walked, 3)
	for _, m := range walked {
		require.Contains(t, m.Name, "memos/keyset-archived-", "listing must stay scoped to the caller's archived memos")
		require.NotContains(t, m.Name, "keyset-archived-other")
	}

	// The other user's own archived listing is scoped to that user.
	otherWalked := walkListMemos(t, otherCtx, ts, &apipb.ListMemosRequest{
		PageSize: 2,
		State:    apipb.State_ARCHIVED,
	})
	require.Len(t, otherWalked, 1)
}

func decodeTestPageToken(raw string) (*apipb.PageToken, error) {
	b, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return nil, err
	}
	var token apipb.PageToken
	if err := proto.Unmarshal(b, &token); err != nil {
		return nil, err
	}
	return &token, nil
}

func marshalTestPageToken(token *apipb.PageToken) (string, error) {
	b, err := proto.Marshal(token)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}
