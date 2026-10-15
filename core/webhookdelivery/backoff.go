package webhookdelivery

import "time"

// backoffShiftCap bounds the exponent so a large attempt count cannot shift
// the duration into an overflow. Larger attempts simply wait the capped delay.
const backoffShiftCap = 20

// backoffDelay returns the wait after attempt number attempt (1-based, the
// attempt that just failed) under exponential backoff:
// min(base * 2^(attempt-1), max).
func backoffDelay(attempt int32, base, max time.Duration) time.Duration {
	if base <= 0 {
		return max
	}
	shift := attempt - 1
	if shift < 0 {
		shift = 0
	}
	if shift > backoffShiftCap {
		shift = backoffShiftCap
	}
	delay := base << shift
	if delay <= 0 || delay > max || max < base {
		return max
	}
	return delay
}
