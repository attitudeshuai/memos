package api

import (
	"encoding/base64"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	apipb "github.com/usememos/memos/proto/gen/api"
	"github.com/usememos/memos/store"
)

func newPaginationTestService() *APIService {
	return &APIService{Secret: "page-token-test-secret"}
}

func TestMemoPageTokenRoundTrip(t *testing.T) {
	t.Parallel()
	svc := newPaginationTestService()
	now := time.Now()
	shape := memoPageShape{
		sortByUpdate: true,
		timeAsc:      true,
		pinnedFirst:  true,
		archived:     true,
		filter:       `visibility == "PUBLIC"`,
	}
	last := &store.Memo{
		ID:        42,
		CreatedTs: 1000,
		UpdatedTs: 2000,
		Pinned:    true,
	}

	raw, err := svc.encodeMemoPageToken(25, shape, last, now)
	require.NoError(t, err)
	require.NotEmpty(t, raw)

	parsed, err := svc.parseMemoPageToken(raw, now.Add(time.Minute))
	require.NoError(t, err)
	require.Equal(t, 25, parsed.limit)
	require.NotNil(t, parsed.cursor, "expected keyset cursor")
	require.Equal(t, true, parsed.cursor.SortByUpdate)
	require.Equal(t, true, parsed.cursor.TimeAsc)
	require.Equal(t, true, parsed.cursor.PinnedFirst)
	require.Equal(t, true, parsed.cursor.Archived)
	require.Equal(t, shape.filter, parsed.cursor.Filter)
	require.Equal(t, int64(2000), parsed.cursor.LastTs)
	require.Equal(t, int32(42), parsed.cursor.LastId)
	require.True(t, parsed.cursor.LastPinned)
}

func TestMemoPageTokenDefaultsToCreatedTs(t *testing.T) {
	t.Parallel()
	svc := newPaginationTestService()
	shape := memoPageShape{}
	last := &store.Memo{ID: 7, CreatedTs: 111, UpdatedTs: 222}

	raw, err := svc.encodeMemoPageToken(10, shape, last, time.Now())
	require.NoError(t, err)

	parsed, err := svc.parseMemoPageToken(raw, time.Now())
	require.NoError(t, err)
	require.Equal(t, int64(111), parsed.cursor.LastTs)
	require.Empty(t, parsed.cursor.Filter)
}

func TestMalformedMemoPageTokenRejected(t *testing.T) {
	t.Parallel()
	svc := newPaginationTestService()

	for _, raw := range []string{"not-base64!!", base64.StdEncoding.EncodeToString([]byte("not protobuf"))} {
		_, err := svc.parseMemoPageToken(raw, time.Now())
		require.ErrorIs(t, err, errMalformedMemoPageToken)
	}
}

func TestForgedMemoPageTokenRejected(t *testing.T) {
	t.Parallel()
	svc := newPaginationTestService()
	shape := memoPageShape{archived: true}
	last := &store.Memo{ID: 9, CreatedTs: 1234}

	raw, err := svc.encodeMemoPageToken(10, shape, last, time.Now())
	require.NoError(t, err)

	// Flip one byte of the encoded protobuf so the signature no longer matches.
	b, err := base64.StdEncoding.DecodeString(raw)
	require.NoError(t, err)
	b[len(b)-1] ^= 0xff
	forged := base64.StdEncoding.EncodeToString(b)

	_, err = svc.parseMemoPageToken(forged, time.Now())
	require.ErrorIs(t, err, errMemoPageTokenSignature)

	// A token signed with another secret is also a forgery.
	other := &APIService{Secret: "a-different-secret"}
	_, err = other.parseMemoPageToken(raw, time.Now())
	require.ErrorIs(t, err, errMemoPageTokenSignature)
}

func TestExpiredMemoPageTokenRejected(t *testing.T) {
	t.Parallel()
	svc := newPaginationTestService()
	shape := memoPageShape{}
	last := &store.Memo{ID: 9, CreatedTs: 1234}
	now := time.Now()

	expired, err := svc.encodeMemoPageToken(10, shape, last, now.Add(-memoPageTokenTTL-time.Second))
	require.NoError(t, err)
	_, err = svc.parseMemoPageToken(expired, now)
	require.ErrorIs(t, err, errMemoPageTokenExpired)

	future, err := svc.encodeMemoPageToken(10, shape, last, now.Add(memoPageTokenMaxClockSkew+time.Minute))
	require.NoError(t, err)
	_, err = svc.parseMemoPageToken(future, now)
	require.ErrorIs(t, err, errMemoPageTokenExpired)
}

func TestLegacyOffsetPageTokenAccepted(t *testing.T) {
	t.Parallel()
	svc := newPaginationTestService()

	legacy, err := marshalPageToken(&apipb.PageToken{Limit: 30, Offset: 90})
	require.NoError(t, err)

	parsed, err := svc.parseMemoPageToken(legacy, time.Now())
	require.NoError(t, err)
	require.Nil(t, parsed.cursor)
	require.Equal(t, 30, parsed.limit)
	require.Equal(t, 90, parsed.offset)
}
