package retry

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackoffStaysWithinGrowingCeiling(t *testing.T) {
	p := Policy{BaseDelay: 100 * time.Millisecond, MaxDelay: time.Second}

	tests := []struct {
		retry   int
		ceiling time.Duration
	}{
		{retry: 1, ceiling: 100 * time.Millisecond},
		{retry: 2, ceiling: 200 * time.Millisecond},
		{retry: 3, ceiling: 400 * time.Millisecond},
		{retry: 4, ceiling: 800 * time.Millisecond},
		{retry: 5, ceiling: time.Second},
		{retry: 1000, ceiling: time.Second},
	}

	for _, tt := range tests {
		for range 200 {
			d := p.Backoff(tt.retry)
			require.GreaterOrEqual(t, d, time.Duration(0), "retry %d", tt.retry)
			require.LessOrEqual(t, d, tt.ceiling, "retry %d", tt.retry)
		}
	}
}

func TestBackoffIsJittered(t *testing.T) {
	p := Policy{BaseDelay: time.Second, MaxDelay: time.Minute}

	seen := make(map[time.Duration]bool)
	for range 50 {
		seen[p.Backoff(3)] = true
	}

	assert.Greater(t, len(seen), 1, "delays must vary between calls")
}

func TestBackoffWithZeroPolicyIsZero(t *testing.T) {
	assert.Zero(t, Policy{}.Backoff(3))
}

func TestDefaultPolicyIsUsable(t *testing.T) {
	p := DefaultPolicy()

	assert.Greater(t, p.MaxAttempts, 1)
	assert.Positive(t, p.BaseDelay)
	assert.Greater(t, p.MaxDelay, p.BaseDelay)
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name   string
		value  string
		want   time.Duration
		wantOK bool
	}{
		{name: "empty", value: "", wantOK: false},
		{name: "seconds", value: "120", want: 2 * time.Minute, wantOK: true},
		{name: "zero seconds", value: "0", want: 0, wantOK: true},
		{name: "padded", value: " 5 ", want: 5 * time.Second, wantOK: true},
		{name: "negative seconds", value: "-1", wantOK: false},
		{name: "HTTP date", value: now.Add(90 * time.Second).Format(http.TimeFormat), want: 90 * time.Second, wantOK: true},
		{name: "HTTP date in the past", value: now.Add(-time.Hour).Format(http.TimeFormat), want: 0, wantOK: true},
		{name: "garbage", value: "soon", wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := ParseRetryAfter(tt.value, now)

			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestSleep(t *testing.T) {
	t.Run("waits for the duration", func(t *testing.T) {
		require.NoError(t, Sleep(context.Background(), time.Millisecond))
	})

	t.Run("returns early when the context ends", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		start := time.Now()
		err := Sleep(ctx, time.Hour)

		require.ErrorIs(t, err, context.Canceled)
		assert.Less(t, time.Since(start), time.Second)
	})
}
