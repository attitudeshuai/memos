package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	storepb "github.com/usememos/memos/proto/gen/store"
	"github.com/usememos/memos/store"
)

func TestAuditLogCreateListAndPrune(t *testing.T) {
	ctx := context.Background()
	ts := NewTestingStore(ctx, t)
	defer ts.Close()

	write := func(actor int32) {
		require.NoError(t, ts.CreateAuditLog(ctx, &store.AuditLog{
			ActorID:      actor,
			ActorName:    "root",
			ResourceType: storepb.AuditLog_INSTANCE_SETTING,
			ResourceID:   "ACCESS",
			Action:       storepb.AuditLog_UPDATE,
			Changes:      `[{"field":"accessMode","before":"PRIVATE","after":"PUBLIC"}]`,
		}))
	}
	for range 3 {
		write(1)
	}

	// Newest first.
	logs, err := ts.ListAuditLogs(ctx, &store.FindAuditLog{})
	require.NoError(t, err)
	require.Len(t, logs, 3)
	require.Equal(t, int64(3), logs[0].ID)
	require.Equal(t, int64(1), logs[2].ID)

	// Filter by object and actor.
	resourceType := storepb.AuditLog_INSTANCE_SETTING
	actorID := int32(1)
	logs, err = ts.ListAuditLogs(ctx, &store.FindAuditLog{
		ResourceType: &resourceType,
		ResourceID:   "ACCESS",
		ActorID:      &actorID,
	})
	require.NoError(t, err)
	require.Len(t, logs, 3)

	// Retention by count prunes oldest.
	require.NoError(t, ts.UpdateAuditRetention(ctx, store.AuditRetention{MaxRecords: 1, RetainDays: 0}))
	logs, err = ts.ListAuditLogs(ctx, &store.FindAuditLog{})
	require.NoError(t, err)
	require.Len(t, logs, 1)
	require.Equal(t, int64(3), logs[0].ID, "the newest entry survives pruning")

	// Subsequent writes honor the bound.
	write(1)
	logs, err = ts.ListAuditLogs(ctx, &store.FindAuditLog{})
	require.NoError(t, err)
	require.Len(t, logs, 1)
	require.Equal(t, int64(4), logs[0].ID)

	// Validation.
	require.Error(t, ts.UpdateAuditRetention(ctx, store.AuditRetention{MaxRecords: -1}))
	require.Error(t, ts.UpdateAuditRetention(ctx, store.AuditRetention{RetainDays: -1}))
}
