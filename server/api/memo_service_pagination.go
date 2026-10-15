package api

import (
	"time"

	"github.com/pkg/errors"

	apipb "github.com/usememos/memos/proto/gen/api"
	"github.com/usememos/memos/store"
)

const (
	// memoPageTokenTTL bounds how long a keyset page token stays valid.
	memoPageTokenTTL = 24 * time.Hour
	// memoPageTokenMaxClockSkew tolerates a token issued slightly in the
	// future by another replica.
	memoPageTokenMaxClockSkew = 5 * time.Minute
)

// Distinguishable page-token failures. All map to InvalidArgument, but their
// messages tell malformed/forged, expired, and shape-mismatch tokens apart.
var (
	errMalformedMemoPageToken = errors.New("invalid page token: malformed or unsupported encoding")
	errMemoPageTokenSignature = errors.New("invalid page token: signature mismatch")
	errMemoPageTokenExpired   = errors.New("invalid page token: token has expired, request the first page again")
	errMemoPageTokenMismatch  = errors.New("invalid page token: token does not match the request filter or order_by")
)

// memoPageShape is the effective query shape a keyset token is bound to. The
// signed token is authoritative: fields omitted on later-page requests are
// restored from it, while request-supplied fields must agree with it.
type memoPageShape struct {
	sortByUpdate bool
	timeAsc      bool
	pinnedFirst  bool
	archived     bool
	filter       string
}

// newMemoPageShape derives the shape from the parsed find options (the
// validated order_by) and the raw request (state and filter text).
func newMemoPageShape(request *apipb.ListMemosRequest, memoFind *store.FindMemo) memoPageShape {
	return memoPageShape{
		sortByUpdate: memoFind.OrderByUpdatedTs,
		timeAsc:      memoFind.OrderByTimeAsc,
		pinnedFirst:  memoFind.OrderByPinned,
		archived:     request.State == apipb.State_ARCHIVED,
		filter:       request.Filter,
	}
}

func (s *APIService) encodeMemoPageToken(limit int, shape memoPageShape, last *store.Memo, now time.Time) (string, error) {
	lastTs := last.CreatedTs
	if shape.sortByUpdate {
		lastTs = last.UpdatedTs
	}
	pageToken := &apipb.PageToken{
		Limit:        int32(limit),
		IssuedAtUnix: now.Unix(),
		MemoCursor: &apipb.MemoPageCursor{
			SortByUpdate: shape.sortByUpdate,
			TimeAsc:      shape.timeAsc,
			PinnedFirst:  shape.pinnedFirst,
			Archived:     shape.archived,
			Filter:       shape.filter,
			LastTs:       lastTs,
			LastId:       last.ID,
			LastPinned:   last.Pinned,
		},
	}
	if err := signPageToken(pageToken, s.Secret); err != nil {
		return "", errors.Wrap(err, "failed to sign page token")
	}
	return marshalPageToken(pageToken)
}

// parsedMemoPageToken is the decoded token. Legacy tokens set offset; keyset
// tokens set cursor.
type parsedMemoPageToken struct {
	limit  int
	offset int
	cursor *apipb.MemoPageCursor
}

// parseMemoPageToken decodes a ListMemos page token and verifies its
// authenticity and freshness. Query-shape agreement is enforced separately by
// the caller, because omitted request fields are restored from the token.
func (s *APIService) parseMemoPageToken(raw string, now time.Time) (*parsedMemoPageToken, error) {
	var pageToken apipb.PageToken
	if err := unmarshalPageToken(raw, &pageToken); err != nil {
		return nil, errMalformedMemoPageToken
	}

	// Legacy offset tokens carry no cursor or signature. They stay valid
	// during the transition and behave exactly as before.
	if pageToken.MemoCursor == nil {
		return &parsedMemoPageToken{
			limit:  normalizePageSize(pageToken.Limit),
			offset: max(int(pageToken.Offset), 0),
		}, nil
	}

	if !verifyPageTokenSignature(&pageToken, s.Secret) {
		return nil, errMemoPageTokenSignature
	}
	issuedAt := time.Unix(pageToken.IssuedAtUnix, 0)
	if now.Sub(issuedAt) > memoPageTokenTTL || issuedAt.Sub(now) > memoPageTokenMaxClockSkew {
		return nil, errMemoPageTokenExpired
	}

	return &parsedMemoPageToken{
		limit:  normalizePageSize(pageToken.Limit),
		cursor: pageToken.MemoCursor,
	}, nil
}
