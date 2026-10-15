package api

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/usememos/memos/core/webhookdelivery"
	"github.com/usememos/memos/internal/webhook"
	apipb "github.com/usememos/memos/proto/gen/api"
	storepb "github.com/usememos/memos/proto/gen/store"
	"github.com/usememos/memos/store"
)

// switchableReceiver answers with a recorded status code and counts requests.
type switchableReceiver struct {
	server  *httptest.Server
	mu      sync.Mutex
	failing bool
	count   int
}

func newSwitchableReceiver(t *testing.T, failing bool) *switchableReceiver {
	t.Helper()
	r := &switchableReceiver{failing: failing}
	r.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		_, _ = io.Copy(io.Discard, req.Body)
		r.mu.Lock()
		r.count++
		failing := r.failing
		r.mu.Unlock()
		if failing {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte(`{"code":0}`))
	}))
	t.Cleanup(r.server.Close)
	return r
}

func (r *switchableReceiver) setFailing(failing bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failing = failing
}

func (r *switchableReceiver) requests() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.count
}

func allowLocalWebhooks(t *testing.T) {
	t.Helper()
	require.NoError(t, webhook.ConfigurePrivateDestinationAllowlist([]string{"127.0.0.1", "::1"}))
	t.Cleanup(func() { require.NoError(t, webhook.ConfigurePrivateDestinationAllowlist(nil)) })
}

// startFastDeliveryManager replaces the service manager with one whose workers
// poll quickly so lifecycle transitions settle in test time.
func startFastDeliveryManager(t *testing.T, svc *APIService) {
	t.Helper()
	svc.WebhookDeliveries.Close()
	svc.WebhookDeliveries = webhookdelivery.New(svc.Store, webhookdelivery.WithPollInterval(20*time.Millisecond))
	require.NoError(t, svc.WebhookDeliveries.Start(context.Background()))
	t.Cleanup(svc.WebhookDeliveries.Close)
}

func configureWebhookPolicy(ctx context.Context, t *testing.T, svc *APIService, maxAttempts int32, baseSeconds, maxSeconds int64, retention int32) {
	t.Helper()
	_, err := svc.Store.UpsertInstanceSetting(ctx, &storepb.InstanceSetting{
		Key: storepb.InstanceSettingKey_WEBHOOK,
		Value: &storepb.InstanceSetting_WebhookSetting{WebhookSetting: &storepb.InstanceWebhookSetting{
			RetryPolicy: &storepb.WebhookRetryPolicy{
				MaxAttempts:        maxAttempts,
				BackoffBaseSeconds: baseSeconds,
				BackoffMaxSeconds:  maxSeconds,
			},
			LedgerRetentionCount: retention,
		}},
	})
	require.NoError(t, err)
	require.NoError(t, svc.WebhookDeliveries.ReloadPolicy(ctx))
}

func deliveryTestUsers(ctx context.Context, t *testing.T, svc *APIService) (host, owner, other *store.User) {
	t.Helper()
	var err error
	host, err = svc.Store.CreateUser(ctx, &store.User{Username: "del-host", Role: store.RoleAdmin, Email: "del-host@example.com"})
	require.NoError(t, err)
	owner, err = svc.Store.CreateUser(ctx, &store.User{Username: "del-owner", Role: store.RoleUser, Email: "del-owner@example.com"})
	require.NoError(t, err)
	other, err = svc.Store.CreateUser(ctx, &store.User{Username: "del-other", Role: store.RoleUser, Email: "del-other@example.com"})
	require.NoError(t, err)
	return host, owner, other
}

func waitForDeliveryState(ctx context.Context, t *testing.T, svc *APIService, userID int32, webhookID string, state store.WebhookDeliveryStatus) *store.WebhookDelivery {
	t.Helper()
	var found *store.WebhookDelivery
	require.Eventually(t, func() bool {
		statusFilter := state
		rows, err := svc.Store.ListWebhookDeliveries(ctx, &store.FindWebhookDeliveries{
			UserID: &userID, WebhookID: &webhookID, Status: &statusFilter,
		})
		require.NoError(t, err)
		if len(rows) > 0 {
			found = rows[0]
			return true
		}
		return false
	}, 5*time.Second, 20*time.Millisecond, "delivery never reached %s", state)
	return found
}

func buildDeliveryName(username, webhookID string, id int64) string {
	return "users/" + username + "/webhooks/" + webhookID + "/deliveries/" + strconv.FormatInt(id, 10)
}

func webhookIDFromName(t *testing.T, name string) string {
	t.Helper()
	tokens, err := GetNameParentTokens(name, UserNamePrefix, WebhookNamePrefix)
	require.NoError(t, err)
	return tokens[1]
}

func submitTestDelivery(ctx context.Context, userID int32, webhookID, label, url string, svc *APIService) {
	svc.WebhookDeliveries.Submit(ctx, &webhook.Request{
		UserID: userID, WebhookID: webhookID, Label: label,
		URL: url, Payload: map[string]any{"hello": "world"},
	})
}

func TestWebhookDeliveryLifecycleQueryAndAuthz(t *testing.T) {
	allowLocalWebhooks(t)
	svc := newIntegrationService(t)
	ctx := context.Background()
	host, owner, other := deliveryTestUsers(ctx, t, svc)
	startFastDeliveryManager(t, svc)

	receiver := newSwitchableReceiver(t, false)
	configureWebhookPolicy(ctx, t, svc, 3, 1, 4, 100)

	hook, err := svc.CreateUserWebhook(userCtx(ctx, owner.ID), &apipb.CreateUserWebhookRequest{
		Parent:  "users/del-owner",
		Webhook: &apipb.UserWebhook{DisplayName: "lifecycle", Url: receiver.server.URL},
	})
	require.NoError(t, err)
	webhookID := webhookIDFromName(t, hook.Name)

	submitTestDelivery(ctx, owner.ID, webhookID, "memos.memo.created", receiver.server.URL, svc)
	row := waitForDeliveryState(ctx, t, svc, owner.ID, webhookID, store.WebhookDeliverySucceeded)
	require.Equal(t, int32(1), row.Attempts)
	require.Empty(t, row.LastError)
	require.NotZero(t, row.SettledTs)
	require.Equal(t, 1, receiver.requests())

	// The owner lists deliveries.
	listResp, err := svc.ListUserWebhookDeliveries(userCtx(ctx, owner.ID), &apipb.ListUserWebhookDeliveriesRequest{
		Parent: hook.Name,
	})
	require.NoError(t, err)
	require.Len(t, listResp.Deliveries, 1)
	delivery := listResp.Deliveries[0]
	require.Equal(t, apipb.WebhookDeliveryState_SUCCEEDED, delivery.State)
	require.Equal(t, "memos.memo.created", delivery.ActivityType)
	require.Equal(t, receiver.server.URL, delivery.Url)
	require.Equal(t, int32(1), delivery.Attempts)
	require.Equal(t, int32(3), delivery.MaxAttempts)
	require.NotNil(t, delivery.CreateTime)
	require.NotNil(t, delivery.SettleTime)
	require.Nil(t, delivery.NextAttemptTime, "settled rows have no next attempt")
	// The resource name round-trips through Get.
	got, err := svc.GetUserWebhookDelivery(userCtx(ctx, owner.ID), &apipb.GetUserWebhookDeliveryRequest{Name: delivery.Name})
	require.NoError(t, err)
	require.Equal(t, delivery.Name, got.Name)

	// Other users are denied; the instance admin may read delivery records.
	_, err = svc.ListUserWebhookDeliveries(userCtx(ctx, other.ID), &apipb.ListUserWebhookDeliveriesRequest{Parent: hook.Name})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	_, err = svc.GetUserWebhookDelivery(userCtx(ctx, other.ID), &apipb.GetUserWebhookDeliveryRequest{Name: delivery.Name})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	adminResp, err := svc.ListUserWebhookDeliveries(userCtx(ctx, host.ID), &apipb.ListUserWebhookDeliveriesRequest{Parent: hook.Name})
	require.NoError(t, err)
	require.Len(t, adminResp.Deliveries, 1)
	_, err = svc.GetUserWebhookDelivery(userCtx(ctx, host.ID), &apipb.GetUserWebhookDeliveryRequest{Name: delivery.Name})
	require.NoError(t, err)

	// Unauthenticated calls are rejected.
	_, err = svc.ListUserWebhookDeliveries(ctx, &apipb.ListUserWebhookDeliveriesRequest{Parent: hook.Name})
	require.Error(t, err)

	// Malformed names and missing records produce the expected status codes.
	_, err = svc.GetUserWebhookDelivery(userCtx(ctx, owner.ID), &apipb.GetUserWebhookDeliveryRequest{Name: buildDeliveryName("del-owner", webhookID, 0) + "-bad/notanint"})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = svc.GetUserWebhookDelivery(userCtx(ctx, owner.ID), &apipb.GetUserWebhookDeliveryRequest{Name: buildDeliveryName("del-owner", webhookID, 99999999)})
	require.Equal(t, codes.NotFound, status.Code(err))
	_, err = svc.ListUserWebhookDeliveries(userCtx(ctx, owner.ID), &apipb.ListUserWebhookDeliveriesRequest{
		Parent: "users/del-owner/webhooks/missing-hook",
	})
	require.Equal(t, codes.NotFound, status.Code(err), "owners see NotFound for a hook they do not own")
}

func TestWebhookDeliveryAbandonAndReplay(t *testing.T) {
	allowLocalWebhooks(t)
	svc := newIntegrationService(t)
	ctx := context.Background()
	host, owner, _ := deliveryTestUsers(ctx, t, svc)
	startFastDeliveryManager(t, svc)

	receiver := newSwitchableReceiver(t, true)
	// Two attempts total with a 1 second gap.
	configureWebhookPolicy(ctx, t, svc, 2, 1, 1, 100)

	hook, err := svc.CreateUserWebhook(userCtx(ctx, owner.ID), &apipb.CreateUserWebhookRequest{
		Parent:  "users/del-owner",
		Webhook: &apipb.UserWebhook{DisplayName: "abandon", Url: receiver.server.URL},
	})
	require.NoError(t, err)
	webhookID := webhookIDFromName(t, hook.Name)

	submitTestDelivery(ctx, owner.ID, webhookID, "memos.memo.updated", receiver.server.URL, svc)
	abandoned := waitForDeliveryState(ctx, t, svc, owner.ID, webhookID, store.WebhookDeliveryAbandoned)
	require.Equal(t, int32(2), abandoned.Attempts)
	require.Contains(t, abandoned.LastError, "500")
	require.Equal(t, 2, receiver.requests())

	replayed, err := svc.ReplayUserWebhookDelivery(userCtx(ctx, owner.ID), &apipb.ReplayUserWebhookDeliveryRequest{
		Name: buildDeliveryName("del-owner", webhookID, abandoned.ID),
	})
	require.NoError(t, err)
	require.Equal(t, apipb.WebhookDeliveryState_PENDING, replayed.State)
	require.Equal(t, int32(0), replayed.Attempts)
	require.Empty(t, replayed.LastError)
	require.Nil(t, replayed.SettleTime)
	require.Equal(t, abandoned.CreatedTs, replayed.CreateTime.AsTime().UnixMilli())

	receiver.setFailing(false)
	row := waitForDeliveryState(ctx, t, svc, owner.ID, webhookID, store.WebhookDeliverySucceeded)
	require.Equal(t, int32(1), row.Attempts, "replay starts a fresh attempt sequence")
	require.Equal(t, 3, receiver.requests())

	// A succeeded delivery cannot be replayed (no duplicate delivery).
	_, err = svc.ReplayUserWebhookDelivery(userCtx(ctx, owner.ID), &apipb.ReplayUserWebhookDeliveryRequest{
		Name: buildDeliveryName("del-owner", webhookID, abandoned.ID),
	})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Equal(t, 3, receiver.requests(), "replaying success never sends another request")

	// The admin sees the same FailedPrecondition for the already-succeeded row.
	_, err = svc.ReplayUserWebhookDelivery(userCtx(ctx, host.ID), &apipb.ReplayUserWebhookDeliveryRequest{
		Name: buildDeliveryName("del-owner", webhookID, abandoned.ID),
	})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestWebhookDeliveryReplayRejectsInFlightStates(t *testing.T) {
	allowLocalWebhooks(t)
	svc := newIntegrationService(t)
	ctx := context.Background()
	_, owner, _ := deliveryTestUsers(ctx, t, svc)
	startFastDeliveryManager(t, svc)
	configureWebhookPolicy(ctx, t, svc, 3, 1, 4, 100)

	hook, err := svc.CreateUserWebhook(userCtx(ctx, owner.ID), &apipb.CreateUserWebhookRequest{
		Parent:  "users/del-owner",
		Webhook: &apipb.UserWebhook{DisplayName: "inflight", Url: "http://127.0.0.1:1/hook"},
	})
	require.NoError(t, err)
	webhookID := webhookIDFromName(t, hook.Name)

	seed := func(state store.WebhookDeliveryStatus) int64 {
		row, createErr := svc.Store.CreateWebhookDelivery(ctx, &store.WebhookDelivery{
			UserID: owner.ID, WebhookID: webhookID, Label: "memos.memo.created",
			URL: "http://127.0.0.1:1/hook", Status: state, MaxAttempts: 3,
			NextDueTs: time.Now().UnixMilli() + 60000, RequestBody: `{}`,
		})
		require.NoError(t, createErr)
		return row.ID
	}
	for _, inFlightState := range []store.WebhookDeliveryStatus{
		store.WebhookDeliveryPending, store.WebhookDeliveryRetrying, store.WebhookDeliveryDelivering,
	} {
		id := seed(inFlightState)
		_, err = svc.ReplayUserWebhookDelivery(userCtx(ctx, owner.ID), &apipb.ReplayUserWebhookDeliveryRequest{
			Name: buildDeliveryName("del-owner", webhookID, id),
		})
		require.Equal(t, codes.FailedPrecondition, status.Code(err), string(inFlightState))
	}
}

func TestWebhookDeliveryListPagination(t *testing.T) {
	allowLocalWebhooks(t)
	svc := newIntegrationService(t)
	ctx := context.Background()
	_, owner, _ := deliveryTestUsers(ctx, t, svc)
	hook, err := svc.CreateUserWebhook(userCtx(ctx, owner.ID), &apipb.CreateUserWebhookRequest{
		Parent:  "users/del-owner",
		Webhook: &apipb.UserWebhook{DisplayName: "paged", Url: "http://127.0.0.1:65000/hook"},
	})
	require.NoError(t, err)
	webhookID := webhookIDFromName(t, hook.Name)
	for _, state := range []store.WebhookDeliveryStatus{
		store.WebhookDeliveryPending, store.WebhookDeliverySucceeded, store.WebhookDeliveryAbandoned,
	} {
		_, err = svc.Store.CreateWebhookDelivery(ctx, &store.WebhookDelivery{
			UserID: owner.ID, WebhookID: webhookID, Label: "memos.memo.created",
			URL: "http://127.0.0.1:65000/hook", Status: state, MaxAttempts: 3,
			NextDueTs: time.Now().UnixMilli(), RequestBody: `{}`,
		})
		require.NoError(t, err)
	}
	parent := hook.Name
	page1, err := svc.ListUserWebhookDeliveries(userCtx(ctx, owner.ID), &apipb.ListUserWebhookDeliveriesRequest{
		Parent: parent, PageSize: 2,
	})
	require.NoError(t, err)
	require.Len(t, page1.Deliveries, 2)
	require.NotEmpty(t, page1.NextPageToken)

	page2, err := svc.ListUserWebhookDeliveries(userCtx(ctx, owner.ID), &apipb.ListUserWebhookDeliveriesRequest{
		Parent: parent, PageSize: 2, PageToken: page1.NextPageToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.Deliveries, 1)
	require.Empty(t, page2.NextPageToken)
	require.NotEqual(t, page1.Deliveries[0].Name, page2.Deliveries[0].Name)

	// Default size applies and oversize pages are rejected.
	defaultPage, err := svc.ListUserWebhookDeliveries(userCtx(ctx, owner.ID), &apipb.ListUserWebhookDeliveriesRequest{Parent: parent})
	require.NoError(t, err)
	require.Len(t, defaultPage.Deliveries, 3)
	_, err = svc.ListUserWebhookDeliveries(userCtx(ctx, owner.ID), &apipb.ListUserWebhookDeliveriesRequest{
		Parent: parent, PageSize: 201,
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = svc.ListUserWebhookDeliveries(userCtx(ctx, owner.ID), &apipb.ListUserWebhookDeliveriesRequest{
		Parent: parent, PageToken: "%%%not-base64%%%",
	})
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	// State filtering through the API.
	abandonedPage, err := svc.ListUserWebhookDeliveries(userCtx(ctx, owner.ID), &apipb.ListUserWebhookDeliveriesRequest{
		Parent: parent, State: apipb.WebhookDeliveryState_ABANDONED,
	})
	require.NoError(t, err)
	require.Len(t, abandonedPage.Deliveries, 1)
	require.Equal(t, apipb.WebhookDeliveryState_ABANDONED, abandonedPage.Deliveries[0].State)
}

func TestUpdateWebhookSettingValidationAndHotReload(t *testing.T) {
	allowLocalWebhooks(t)
	svc := newIntegrationService(t)
	ctx := context.Background()
	host, _, _ := deliveryTestUsers(ctx, t, svc)
	startFastDeliveryManager(t, svc)
	hostCtx := userCtx(ctx, host.ID)

	update := func(policy *apipb.InstanceSetting_WebhookSetting_RetryPolicy, retention int32) error {
		_, err := svc.UpdateInstanceSetting(hostCtx, &apipb.UpdateInstanceSettingRequest{
			InstanceSetting: &apipb.InstanceSetting{
				Name:  "instance/settings/WEBHOOK",
				Value: &apipb.InstanceSetting_WebhookSetting_{WebhookSetting: &apipb.InstanceSetting_WebhookSetting{RetryPolicy: policy, LedgerRetentionCount: retention}},
			},
		})
		return err
	}

	for _, bad := range []struct {
		policy    *apipb.InstanceSetting_WebhookSetting_RetryPolicy
		retention int32
	}{
		{policy: &apipb.InstanceSetting_WebhookSetting_RetryPolicy{MaxAttempts: 51}, retention: 0},
		{policy: &apipb.InstanceSetting_WebhookSetting_RetryPolicy{MaxAttempts: -1}, retention: 0},
		{policy: &apipb.InstanceSetting_WebhookSetting_RetryPolicy{MaxAttempts: 3, BackoffBaseSeconds: 3601}, retention: 0},
		{policy: &apipb.InstanceSetting_WebhookSetting_RetryPolicy{MaxAttempts: 3, BackoffMaxSeconds: 86401}, retention: 0},
		{policy: &apipb.InstanceSetting_WebhookSetting_RetryPolicy{MaxAttempts: 3, BackoffBaseSeconds: 60, BackoffMaxSeconds: 30}, retention: 0},
		{policy: nil, retention: 10001},
		{policy: nil, retention: -1},
	} {
		err := update(bad.policy, bad.retention)
		require.Equal(t, codes.InvalidArgument, status.Code(err), "bad policy should be rejected: %+v", bad)
	}

	// The previous (unset) value survives the rejected updates.
	got, err := svc.GetInstanceSetting(hostCtx, &apipb.GetInstanceSettingRequest{Name: "instance/settings/WEBHOOK"})
	require.NoError(t, err)
	require.Nil(t, got.GetWebhookSetting().GetRetryPolicy())

	// A valid update is accepted, readable, and hot-reloads the manager: a new
	// submission lands in the ledger immediately.
	require.NoError(t, update(&apipb.InstanceSetting_WebhookSetting_RetryPolicy{
		MaxAttempts: 1, BackoffBaseSeconds: 1, BackoffMaxSeconds: 1,
	}, 5))
	got, err = svc.GetInstanceSetting(hostCtx, &apipb.GetInstanceSettingRequest{Name: "instance/settings/WEBHOOK"})
	require.NoError(t, err)
	require.Equal(t, int32(1), got.GetWebhookSetting().GetRetryPolicy().GetMaxAttempts())
	require.Equal(t, int32(5), got.GetWebhookSetting().GetLedgerRetentionCount())

	receiver := newSwitchableReceiver(t, false)
	submitTestDelivery(ctx, host.ID, "hot-reload", "memos.memo.created", receiver.server.URL, svc)
	waitForDeliveryState(ctx, t, svc, host.ID, "hot-reload", store.WebhookDeliverySucceeded)
}
