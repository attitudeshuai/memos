// Package webhookdelivery owns the lifecycle of outbound webhook deliveries:
// persistent registration in the store ledger, claiming due deliveries,
// retrying failures with exponential backoff, abandoning deliveries that
// exhaust their attempt budget, recovering deliveries interrupted by a
// restart, and pruning terminal records to the configured retention.
//
// The package sits between the API layer and the store. It performs HTTP
// delivery through internal/webhook, which stays unaware of Memos types.
// When no retry policy is configured every submission keeps the legacy
// fire-and-forget behavior of internal/webhook.PostAsync.
package webhookdelivery
