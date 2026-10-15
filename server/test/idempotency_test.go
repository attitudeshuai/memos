package test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCreateMemoIdempotencyKeyOverREST exercises the full HTTP path: the query
// parameter must survive grpc-gateway binding and make a retried POST return
// the first memo instead of creating another one.
func TestCreateMemoIdempotencyKeyOverREST(t *testing.T) {
	ctx := t.Context()
	inst := bootInstance(ctx, t, instanceOptions{instanceURL: "http://localhost:5230"})
	inst.createAdmin(t)
	token := inst.signIn(t)

	body := map[string]any{
		"content":    "rest idempotent memo",
		"visibility": "PRIVATE",
	}

	firstStatus, firstBody := inst.do(t, http.MethodPost, "/api/memos?idempotencyKey=rest-key-1", token, body)
	require.Equal(t, http.StatusOK, firstStatus, string(firstBody))
	secondStatus, secondBody := inst.do(t, http.MethodPost, "/api/memos?idempotencyKey=rest-key-1", token, body)
	require.Equal(t, http.StatusOK, secondStatus, string(secondBody))

	var first, second struct {
		Name string `json:"name"`
	}
	require.NoError(t, json.Unmarshal(firstBody, &first))
	require.NoError(t, json.Unmarshal(secondBody, &second))
	require.Equal(t, first.Name, second.Name)

	// Same key with a different payload is a conflict, not a silent replay.
	conflictStatus, conflictBody := inst.do(t, http.MethodPost, "/api/memos?idempotencyKey=rest-key-1", token, map[string]any{
		"content":    "a different body",
		"visibility": "PRIVATE",
	})
	require.Equal(t, http.StatusConflict, conflictStatus, string(conflictBody))

	listStatus, listBody := inst.do(t, http.MethodGet, "/api/memos?pageSize=50", token, nil)
	require.Equal(t, http.StatusOK, listStatus, string(listBody))
	var listed struct {
		Memos []struct {
			Content string `json:"content"`
		} `json:"memos"`
	}
	require.NoError(t, json.Unmarshal(listBody, &listed))
	count := 0
	for _, memo := range listed.Memos {
		if memo.Content == "rest idempotent memo" {
			count++
		}
	}
	require.Equal(t, 1, count, "only the first request may create the memo")
}
