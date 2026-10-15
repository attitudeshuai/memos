package api

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/usememos/memos/internal/protohash"
	"github.com/usememos/memos/store"
)

const (
	// maxIdempotencyKeyBytes bounds the accepted key length.
	maxIdempotencyKeyBytes = 255
	// idempotencyPollInterval is how long a duplicate request waits between
	// checks for the in-flight leader to finish.
	idempotencyPollInterval = 50 * time.Millisecond
	// idempotencyMaxWait bounds how long a duplicate waits for a leader before
	// reporting that the operation is still running.
	idempotencyMaxWait = 60 * time.Second
)

// withIdempotencyKey makes execute run at most once per (creatorID, key).
//
// Empty key: execute runs directly, with behavior identical to an unkeyed
// request. Otherwise the request is fingerprinted, and one of three outcomes
// follows:
//
//   - The caller wins the (creator, key) claim: execute runs. Its response is
//     stored for replays; a failure removes the claim so a retry can run.
//   - The claim exists with the same fingerprint: a COMPLETED claim replays
//     the stored response; an IN_FLIGHT claim makes the caller wait until the
//     leader finishes, then returns the same response.
//   - The claim exists with a different fingerprint: the request is rejected
//     with ABORTED instead of silently replaying the other result.
//
// The store rows coordinate leaders across processes.
func withIdempotency[T proto.Message](
	ctx context.Context,
	s *APIService,
	creatorID int32,
	key string,
	request proto.Message,
	newResponse func() T,
	execute func() (T, error),
) (T, error) {
	if key == "" {
		return execute()
	}
	if len(key) > maxIdempotencyKeyBytes {
		var zero T
		return zero, status.Errorf(codes.InvalidArgument, "idempotency_key must be at most %d bytes", maxIdempotencyKeyBytes)
	}
	requestHash, err := protohash.Digest(request, "idempotency_key")
	if err != nil {
		var zero T
		return zero, status.Errorf(codes.Internal, "failed to fingerprint request: %v", err)
	}

	waitDeadline := time.Now().Add(idempotencyMaxWait)
	if deadline, ok := ctx.Deadline(); ok {
		if maxWait := deadline.Add(-time.Second); maxWait.Before(waitDeadline) {
			waitDeadline = maxWait
		}
	}

	var record *store.IdempotencyRecord
	for {
		if record == nil {
			acquired, leader, err := s.Store.AcquireIdempotencyKey(ctx, creatorID, key, requestHash)
			if err != nil {
				var zero T
				return zero, status.Errorf(codes.Internal, "failed to acquire idempotency key: %v", err)
			}
			if leader {
				return executeIdempotentLeader(ctx, s, acquired.ID, creatorID, key, execute)
			}
			record = acquired
		}

		if record.RequestHash != requestHash {
			var zero T
			if record.Status == store.IdempotencyStatusInFlight {
				return zero, status.Errorf(codes.Aborted, "a request with idempotency key %q from this user is already in progress with a different payload", key)
			}
			return zero, status.Errorf(codes.Aborted, "idempotency key %q was already used with a different request payload", key)
		}
		if record.Status == store.IdempotencyStatusCompleted {
			response := newResponse()
			if err := proto.Unmarshal(record.Response, response); err != nil {
				var zero T
				return zero, status.Errorf(codes.Internal, "failed to decode stored response for idempotency key: %v", err)
			}
			return response, nil
		}

		if err := waitForIdempotencyLeader(ctx, waitDeadline, key); err != nil {
			var zero T
			return zero, err
		}
		reloaded, err := s.Store.GetIdempotencyKey(ctx, creatorID, key)
		if err != nil {
			var zero T
			return zero, status.Errorf(codes.Internal, "failed to reload idempotency key: %v", err)
		}
		// nil means the leader failed and released the claim; loop back and
		// try to become the leader with the same fingerprint.
		record = reloaded
	}
}

// executeIdempotentLeader runs the creation and records its outcome.
func executeIdempotentLeader[T proto.Message](
	ctx context.Context,
	s *APIService,
	recordID int32,
	creatorID int32,
	key string,
	execute func() (T, error),
) (T, error) {
	response, err := execute()
	if err != nil {
		if releaseErr := s.Store.ReleaseIdempotencyKey(ctx, recordID, creatorID, key); releaseErr != nil {
			slog.Warn("Failed to release idempotency key after failed creation",
				slog.String("idempotency_key", key),
				slog.Any("err", releaseErr))
		}
		var zero T
		return zero, err
	}
	wire, err := proto.Marshal(response)
	if err != nil {
		if releaseErr := s.Store.ReleaseIdempotencyKey(ctx, recordID, creatorID, key); releaseErr != nil {
			slog.Warn("Failed to release idempotency key after response marshal failure",
				slog.String("idempotency_key", key),
				slog.Any("err", releaseErr))
		}
		var zero T
		return zero, status.Errorf(codes.Internal, "failed to encode idempotent response: %v", err)
	}
	if err := s.Store.CompleteIdempotencyKey(ctx, recordID, creatorID, key, wire); err != nil {
		// The claim expired or was taken over while the creation ran. The
		// creation itself succeeded, so still return its response to the
		// caller; only replay availability is affected.
		slog.Warn("Failed to store idempotent creation response",
			slog.String("idempotency_key", key),
			slog.Any("err", err))
	}
	return response, nil
}

// waitForIdempotencyLeader blocks briefly for the leader to finish or until
// the wait budget / request context is exhausted.
func waitForIdempotencyLeader(ctx context.Context, waitDeadline time.Time, key string) error {
	waitFor := time.Until(waitDeadline)
	if waitFor <= 0 {
		return status.Errorf(codes.Unavailable, "request with idempotency key %q is still being processed; retry later", key)
	}
	if waitFor > idempotencyPollInterval {
		waitFor = idempotencyPollInterval
	}
	timer := time.NewTimer(waitFor)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	case <-timer.C:
	}
	if time.Now().After(waitDeadline) {
		return status.Errorf(codes.Unavailable, "request with idempotency key %q is still being processed; retry later", key)
	}
	return nil
}
