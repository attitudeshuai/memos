package store

import "context"

// inboxBatchChunkSize bounds how many rows one bulk statement touches. It keeps
// a large batch operation online: each chunk is a single statement that the
// database commits independently instead of one long transaction locking the
// whole inbox table.
const inboxBatchChunkSize = 500

// listInboxIDsChunk returns up to limit matching inbox IDs in ascending order.
// Rows consumed by a successful applyChunk must no longer match the filter,
// which is what advances the scan.
type listInboxIDsChunk func(ctx context.Context, limit int) ([]int32, error)

// applyInboxIDsChunk applies the bulk operation to one ID batch and reports the
// rows actually affected.
type applyInboxIDsChunk func(ctx context.Context, ids []int32) (int32, error)

// processInboxIDChunks drains matching inboxes in ascending ID order. Every
// batch is applied independently, so a failure leaves the already-processed ID
// prefix durably committed: the boundary is observable as "all matching IDs up
// to lastID are done". It returns the affected count accumulated from committed
// batches and the last ID of the last successful batch. Retrying with a filter
// under which processed rows no longer match resumes from the remainder and
// yields the same total as an uninterrupted run.
func processInboxIDChunks(ctx context.Context, chunkSize int, listChunk listInboxIDsChunk, applyChunk applyInboxIDsChunk) (totalAffected int32, lastID int32, err error) {
	for {
		ids, err := listChunk(ctx, chunkSize)
		if err != nil {
			return totalAffected, lastID, err
		}
		if len(ids) == 0 {
			return totalAffected, lastID, nil
		}
		affected, err := applyChunk(ctx, ids)
		if err != nil {
			return totalAffected, lastID, err
		}
		totalAffected += affected
		lastID = ids[len(ids)-1]
		if len(ids) < chunkSize {
			return totalAffected, lastID, nil
		}
	}
}
