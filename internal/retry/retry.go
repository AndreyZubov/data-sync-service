// Package retry provides exponential backoff with jitter and Retry-After
// parsing for callers that resend failed requests.
package retry

import (
	"context"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Policy bounds how often and how long a caller retries.
type Policy struct {
	// MaxAttempts is the total number of attempts, including the first one.
	MaxAttempts int
	// BaseDelay is the backoff ceiling before the first retry; it doubles with
	// every further retry.
	BaseDelay time.Duration
	// MaxDelay caps the backoff ceiling and the longest Retry-After a caller
	// should be willing to honour.
	MaxDelay time.Duration
}

// DefaultPolicy returns the policy used for Salesforce API calls: five attempts
// with delays capped at 30 seconds.
func DefaultPolicy() Policy {
	return Policy{MaxAttempts: 5, BaseDelay: 500 * time.Millisecond, MaxDelay: 30 * time.Second}
}

// Backoff returns the delay before retry number n (n >= 1), using "full
// jitter": a uniformly random duration in [0, min(MaxDelay, BaseDelay*2^(n-1))].
// Randomising the whole interval keeps clients that failed together from
// retrying together.
func (p Policy) Backoff(n int) time.Duration {
	ceiling := p.BaseDelay
	// Doubling stops at MaxDelay, so the ceiling cannot overflow.
	for i := 1; i < n && ceiling < p.MaxDelay; i++ {
		ceiling *= 2
	}
	ceiling = min(ceiling, p.MaxDelay)
	if ceiling <= 0 {
		return 0
	}
	return rand.N(ceiling + 1) //nolint:gosec // Jitter needs no cryptographic randomness.
}

// ParseRetryAfter interprets a Retry-After header value, which is either a
// number of seconds or an HTTP date. ok is false when the value is absent or
// malformed; a date in the past yields zero.
func ParseRetryAfter(value string, now time.Time) (time.Duration, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	if seconds, err := strconv.Atoi(value); err == nil {
		if seconds < 0 {
			return 0, false
		}
		return time.Duration(seconds) * time.Second, true
	}
	at, err := http.ParseTime(value)
	if err != nil {
		return 0, false
	}
	return max(at.Sub(now), 0), true
}

// Sleep waits for d, returning early with ctx's error if ctx is done first.
func Sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
