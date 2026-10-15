package store

import (
	"context"
	"errors"
	"time"
)

const (
	// IdempotencyKeyTTL is how long a completed idempotency record keeps
	// serving replayed responses. After it elapses the record may be deleted
	// and the key may be reused.
	IdempotencyKeyTTL = 24 * time.Hour
	// idempotencyKeySweepInterval bounds how long expired records may linger
	// before the background janitor removes them.
	idempotencyKeySweepInterval = time.Hour
	// idempotencyInFlightTimeout bounds how long an IN_FLIGHT record makes
	// duplicate requests wait. A record older than this is presumed abandoned
	// (its leader crashed) and may be taken over by another request.
	idempotencyInFlightTimeout = 15 * time.Minute
)

// IdempotencyStatus is the lifecycle state of a stored idempotency key.
type IdempotencyStatus string

const (
	// IdempotencyStatusInFlight means the leader request is still executing.
	IdempotencyStatusInFlight IdempotencyStatus = "IN_FLIGHT"
	// IdempotencyStatusCompleted means the leader request finished and the
	// stored response can be replayed.
	IdempotencyStatusCompleted IdempotencyStatus = "COMPLETED"
)

// ErrIdempotencyKeyNotInFlight is returned when an IN_FLIGHT record can no
// longer be completed, typically because it was taken over after the leader
// stopped updating it.
var ErrIdempotencyKeyNotInFlight = errors.New("idempotency key is not in flight")

// IdempotencyRecord is the stored state of one (creator, key) pair.
type IdempotencyRecord struct {
	ID          int32
	CreatorID   int32
	Key         string
	RequestHash string
	Status      IdempotencyStatus
	// Response holds the protobuf-marshaled response of the leader request.
	// It is empty while the record is IN_FLIGHT.
	Response  []byte
	CreatedTs int64
	ExpiresTs int64
}

// IdempotencyKeyInsert are the fields set when a leader claims a key.
type IdempotencyKeyInsert struct {
	CreatorID   int32
	Key         string
	RequestHash string
	ExpiresTs   int64
}

// AcquireIdempotencyKey claims the (creatorID, key) pair on behalf of a
// leader request. An expired record, or an IN_FLIGHT record older than the
// in-flight timeout, is removed first. When the pair is free it inserts an
// IN_FLIGHT record and returns leader=true. Otherwise it returns the existing
// record with leader=false so the caller can reject mismatched payloads or
// wait for the result.
func (s *Store) AcquireIdempotencyKey(ctx context.Context, creatorID int32, key, requestHash string) (*IdempotencyRecord, bool, error) {
	if key == "" {
		return nil, false, errors.New("idempotency key is required")
	}
	if creatorID <= 0 {
		return nil, false, errors.New("idempotency key requires a creator")
	}
	now := time.Now().Unix()
	return s.driver.AcquireIdempotencyKey(ctx, &IdempotencyKeyInsert{
		CreatorID:   creatorID,
		Key:         key,
		RequestHash: requestHash,
		ExpiresTs:   now + int64(IdempotencyKeyTTL/time.Second),
	}, now, now-int64(idempotencyInFlightTimeout/time.Second))
}

// GetIdempotencyKey returns the record for (creatorID, key), or nil when no
// live record exists.
func (s *Store) GetIdempotencyKey(ctx context.Context, creatorID int32, key string) (*IdempotencyRecord, error) {
	return s.driver.GetIdempotencyKey(ctx, creatorID, key)
}

// CompleteIdempotencyKey stores the leader response and marks the record
// COMPLETED. recordID identifies the claim returned by AcquireIdempotencyKey;
// a stale leader whose claim was taken over fails with
// ErrIdempotencyKeyNotInFlight instead of overwriting the successor.
func (s *Store) CompleteIdempotencyKey(ctx context.Context, recordID int32, creatorID int32, key string, response []byte) error {
	return s.driver.CompleteIdempotencyKey(ctx, recordID, creatorID, key, response)
}

// ReleaseIdempotencyKey removes the IN_FLIGHT claim after the leader request
// fails, allowing an immediate retry with the same key. It does nothing when
// the record is COMPLETED, belongs to a taken-over claim, or no longer exists.
func (s *Store) ReleaseIdempotencyKey(ctx context.Context, recordID int32, creatorID int32, key string) error {
	return s.driver.ReleaseIdempotencyKey(ctx, recordID, creatorID, key)
}

// DeleteExpiredIdempotencyKeys removes every record whose retention period has
// elapsed. It returns the number of deleted rows.
func (s *Store) DeleteExpiredIdempotencyKeys(ctx context.Context) (int64, error) {
	return s.driver.DeleteExpiredIdempotencyKeys(ctx, time.Now().Unix())
}
