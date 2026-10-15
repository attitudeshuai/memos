package test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	apipb "github.com/usememos/memos/proto/gen/api"
)

// idempotencyTestService prepares an isolated service with one writer.
func idempotencyTestService(ctx context.Context, t *testing.T, username string) (*TestService, context.Context) {
	t.Helper()
	ts := NewTestService(t)
	t.Cleanup(ts.Cleanup)
	user, err := ts.CreateRegularUser(ctx, username)
	require.NoError(t, err)
	return ts, ts.CreateUserContext(ctx, user.ID)
}

func TestCreateMemoIdempotencyKeyReplaysFirstResult(t *testing.T) {
	ctx := context.Background()
	ts, userCtx := idempotencyTestService(ctx, t, "idem-memo-owner")

	request := &apipb.CreateMemoRequest{
		Memo:           &apipb.Memo{Content: "idempotent memo body", Visibility: apipb.Visibility_PRIVATE},
		IdempotencyKey: "memo-key-1",
	}
	first, err := ts.Service.CreateMemo(userCtx, request)
	require.NoError(t, err)

	// The client retries verbatim after a timeout.
	second, err := ts.Service.CreateMemo(userCtx, proto.Clone(request).(*apipb.CreateMemoRequest))
	require.NoError(t, err)
	require.True(t, proto.Equal(first, second), "the retry must return the first response")
	require.Equal(t, first.Name, second.Name)

	listed, err := ts.Service.ListMemos(userCtx, &apipb.ListMemosRequest{PageSize: 50})
	require.NoError(t, err)
	require.Len(t, listed.Memos, 1, "retry must not create a second memo")

	// Same key, different payload: a distinguishable rejection, not the old memo.
	conflict, err := ts.Service.CreateMemo(userCtx, &apipb.CreateMemoRequest{
		Memo:           &apipb.Memo{Content: "different body", Visibility: apipb.Visibility_PRIVATE},
		IdempotencyKey: "memo-key-1",
	})
	require.Nil(t, conflict)
	require.Equal(t, codes.Aborted, status.Code(err))

	listed, err = ts.Service.ListMemos(userCtx, &apipb.ListMemosRequest{PageSize: 50})
	require.NoError(t, err)
	require.Len(t, listed.Memos, 1, "a conflicting request must not write anything")
}

func TestCreateMemoIdempotencyKeyConcurrentSingleWriter(t *testing.T) {
	ctx := context.Background()
	ts, userCtx := idempotencyTestService(ctx, t, "idem-memo-concurrent")

	const callers = 8
	start := make(chan struct{})
	var wg sync.WaitGroup
	results := make([]*apipb.Memo, callers)
	errs := make([]error, callers)
	wg.Add(callers)
	for i := range callers {
		go func(i int) {
			defer wg.Done()
			<-start
			request := &apipb.CreateMemoRequest{
				Memo:           &apipb.Memo{Content: "concurrent idempotent memo", Visibility: apipb.Visibility_PRIVATE},
				IdempotencyKey: "concurrent-key",
			}
			results[i], errs[i] = ts.Service.CreateMemo(userCtx, request)
		}(i)
	}
	close(start)
	wg.Wait()

	for i := range callers {
		require.NoError(t, errs[i], "caller %d must receive a result", i)
		require.Equal(t, results[0].Name, results[i].Name, "caller %d must see the same memo", i)
	}
	listed, err := ts.Service.ListMemos(userCtx, &apipb.ListMemosRequest{PageSize: 50})
	require.NoError(t, err)
	require.Len(t, listed.Memos, 1, "exactly one concurrent request may write")
}

func TestCreateMemoWithoutKeyStaysNonIdempotent(t *testing.T) {
	ctx := context.Background()
	ts, userCtx := idempotencyTestService(ctx, t, "idem-memo-keyless")

	request := &apipb.CreateMemoRequest{
		Memo: &apipb.Memo{Content: "keyless memo", Visibility: apipb.Visibility_PRIVATE},
	}
	first, err := ts.Service.CreateMemo(userCtx, request)
	require.NoError(t, err)
	second, err := ts.Service.CreateMemo(userCtx, proto.Clone(request).(*apipb.CreateMemoRequest))
	require.NoError(t, err)
	require.NotEqual(t, first.Name, second.Name, "keyless requests keep creating independent memos")

	listed, err := ts.Service.ListMemos(userCtx, &apipb.ListMemosRequest{PageSize: 50})
	require.NoError(t, err)
	require.Len(t, listed.Memos, 2)

	// Validation failures keep their existing response shape for both paths.
	_, err = ts.Service.CreateMemo(userCtx, &apipb.CreateMemoRequest{IdempotencyKey: "bad-key"})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = ts.Service.CreateMemo(userCtx, &apipb.CreateMemoRequest{
		Memo:           &apipb.Memo{Content: "x"},
		IdempotencyKey: string(make([]byte, 256)),
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err), "an overlong key is an invalid argument")
}

func TestCreateMemoIdempotencyKeyReleasedAfterFailure(t *testing.T) {
	ctx := context.Background()
	ts, userCtx := idempotencyTestService(ctx, t, "idem-memo-retry-fail")

	// The first attempt fails validation; the claim must be released so the
	// client can retry the same key with a valid request.
	_, err := ts.Service.CreateMemo(userCtx, &apipb.CreateMemoRequest{
		Memo:           nil,
		IdempotencyKey: "fail-then-succeed",
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	memo, err := ts.Service.CreateMemo(userCtx, &apipb.CreateMemoRequest{
		Memo:           &apipb.Memo{Content: "recovered after failure", Visibility: apipb.Visibility_PRIVATE},
		IdempotencyKey: "fail-then-succeed",
	})
	require.NoError(t, err)
	require.NotEmpty(t, memo.Name)

	replay, err := ts.Service.CreateMemo(userCtx, &apipb.CreateMemoRequest{
		Memo:           &apipb.Memo{Content: "recovered after failure", Visibility: apipb.Visibility_PRIVATE},
		IdempotencyKey: "fail-then-succeed",
	})
	require.NoError(t, err)
	require.Equal(t, memo.Name, replay.Name)
}

func TestCreateMemoCommentIdempotencyKey(t *testing.T) {
	ctx := context.Background()
	ts, userCtx := idempotencyTestService(ctx, t, "idem-comment-owner")
	parent, err := ts.Service.CreateMemo(userCtx, &apipb.CreateMemoRequest{
		Memo: &apipb.Memo{Content: "parent for comments", Visibility: apipb.Visibility_PUBLIC},
	})
	require.NoError(t, err)

	request := &apipb.CreateMemoCommentRequest{
		Parent:         parent.Name,
		Comment:        &apipb.Memo{Content: "idempotent comment", Visibility: apipb.Visibility_PRIVATE},
		IdempotencyKey: "comment-key-1",
	}
	first, err := ts.Service.CreateMemoComment(userCtx, request)
	require.NoError(t, err)
	second, err := ts.Service.CreateMemoComment(userCtx, proto.Clone(request).(*apipb.CreateMemoCommentRequest))
	require.NoError(t, err)
	require.True(t, proto.Equal(first, second))

	listed, err := ts.Service.ListMemoComments(userCtx, &apipb.ListMemoCommentsRequest{Parent: parent.Name})
	require.NoError(t, err)
	require.Len(t, listed.Comments, 1)

	_, err = ts.Service.CreateMemoComment(userCtx, &apipb.CreateMemoCommentRequest{
		Parent:         parent.Name,
		Comment:        &apipb.Memo{Content: "different comment", Visibility: apipb.Visibility_PRIVATE},
		IdempotencyKey: "comment-key-1",
	})
	require.Equal(t, codes.Aborted, status.Code(err))

	listed, err = ts.Service.ListMemoComments(userCtx, &apipb.ListMemoCommentsRequest{Parent: parent.Name})
	require.NoError(t, err)
	require.Len(t, listed.Comments, 1)

	// No key: duplicate comments remain possible, preserving current behavior.
	keyless := &apipb.CreateMemoCommentRequest{
		Parent:  parent.Name,
		Comment: &apipb.Memo{Content: "keyless comment", Visibility: apipb.Visibility_PRIVATE},
	}
	_, err = ts.Service.CreateMemoComment(userCtx, keyless)
	require.NoError(t, err)
	_, err = ts.Service.CreateMemoComment(userCtx, proto.Clone(keyless).(*apipb.CreateMemoCommentRequest))
	require.NoError(t, err)
	listed, err = ts.Service.ListMemoComments(userCtx, &apipb.ListMemoCommentsRequest{Parent: parent.Name})
	require.NoError(t, err)
	require.Len(t, listed.Comments, 3)
}

func TestCreateMemoShareIdempotencyKey(t *testing.T) {
	ctx := context.Background()
	ts, userCtx := idempotencyTestService(ctx, t, "idem-share-owner")
	memo, err := ts.Service.CreateMemo(userCtx, &apipb.CreateMemoRequest{
		Memo: &apipb.Memo{Content: "sharable memo", Visibility: apipb.Visibility_PUBLIC},
	})
	require.NoError(t, err)

	request := &apipb.CreateMemoShareRequest{
		Parent:         memo.Name,
		MemoShare:      &apipb.MemoShare{},
		IdempotencyKey: "share-key-1",
	}
	first, err := ts.Service.CreateMemoShare(userCtx, request)
	require.NoError(t, err)
	second, err := ts.Service.CreateMemoShare(userCtx, proto.Clone(request).(*apipb.CreateMemoShareRequest))
	require.NoError(t, err)
	require.True(t, proto.Equal(first, second), "retry must return the same share token")

	shares, err := ts.Service.ListMemoShares(userCtx, &apipb.ListMemoSharesRequest{Parent: memo.Name})
	require.NoError(t, err)
	require.Len(t, shares.MemoShares, 1)

	_, err = ts.Service.CreateMemoShare(userCtx, &apipb.CreateMemoShareRequest{
		Parent: memo.Name,
		MemoShare: &apipb.MemoShare{
			ExpireTime: timestamppb.New(time.Now().Add(time.Hour)),
		},
		IdempotencyKey: "share-key-1",
	})
	require.Equal(t, codes.Aborted, status.Code(err))

	shares, err = ts.Service.ListMemoShares(userCtx, &apipb.ListMemoSharesRequest{Parent: memo.Name})
	require.NoError(t, err)
	require.Len(t, shares.MemoShares, 1)
}

func TestCreateAttachmentIdempotencyKey(t *testing.T) {
	ctx := context.Background()
	ts, userCtx := idempotencyTestService(ctx, t, "idem-attachment-owner")

	content := []byte("idempotent file contents")
	request := &apipb.CreateAttachmentRequest{
		Attachment:     &apipb.Attachment{Filename: "idem-upload.bin", Type: "application/octet-stream", Content: content},
		IdempotencyKey: "attachment-key-1",
	}
	first, err := ts.Service.CreateAttachment(userCtx, request)
	require.NoError(t, err)
	second, err := ts.Service.CreateAttachment(userCtx, proto.Clone(request).(*apipb.CreateAttachmentRequest))
	require.NoError(t, err)
	require.True(t, proto.Equal(first, second), "retry must return the stored attachment")
	require.Equal(t, int64(len(content)), second.Size)

	attachments, err := ts.Service.ListAttachments(userCtx, &apipb.ListAttachmentsRequest{PageSize: 50})
	require.NoError(t, err)
	require.Len(t, attachments.Attachments, 1, "the bytes must be stored only once")

	// Different bytes under the same key are rejected rather than overwritten.
	_, err = ts.Service.CreateAttachment(userCtx, &apipb.CreateAttachmentRequest{
		Attachment:     &apipb.Attachment{Filename: "idem-upload.bin", Type: "application/octet-stream", Content: []byte("different bytes")},
		IdempotencyKey: "attachment-key-1",
	})
	require.Equal(t, codes.Aborted, status.Code(err))
	attachments, err = ts.Service.ListAttachments(userCtx, &apipb.ListAttachmentsRequest{PageSize: 50})
	require.NoError(t, err)
	require.Len(t, attachments.Attachments, 1)

	// Keyless uploads keep the current behavior: each call lands a new row.
	keyless := &apipb.CreateAttachmentRequest{
		Attachment: &apipb.Attachment{Filename: "idem-keyless.bin", Type: "application/octet-stream", Content: content},
	}
	_, err = ts.Service.CreateAttachment(userCtx, keyless)
	require.NoError(t, err)
	_, err = ts.Service.CreateAttachment(userCtx, proto.Clone(keyless).(*apipb.CreateAttachmentRequest))
	require.NoError(t, err)
	attachments, err = ts.Service.ListAttachments(userCtx, &apipb.ListAttachmentsRequest{PageSize: 50})
	require.NoError(t, err)
	require.Len(t, attachments.Attachments, 3)
}

func TestIdempotencyKeyIsScopedPerCreator(t *testing.T) {
	ctx := context.Background()
	ts := NewTestService(t)
	t.Cleanup(ts.Cleanup)
	first, err := ts.CreateRegularUser(ctx, "idem-scope-a")
	require.NoError(t, err)
	second, err := ts.CreateRegularUser(ctx, "idem-scope-b")
	require.NoError(t, err)
	firstCtx := ts.CreateUserContext(ctx, first.ID)
	secondCtx := ts.CreateUserContext(ctx, second.ID)

	request := &apipb.CreateMemoRequest{
		Memo:           &apipb.Memo{Content: "same key, different users", Visibility: apipb.Visibility_PRIVATE},
		IdempotencyKey: "cross-user-key",
	}
	one, err := ts.Service.CreateMemo(firstCtx, request)
	require.NoError(t, err)
	two, err := ts.Service.CreateMemo(secondCtx, proto.Clone(request).(*apipb.CreateMemoRequest))
	require.NoError(t, err)
	require.NotEqual(t, one.Name, two.Name, "each creator owns an independent key space")
}
