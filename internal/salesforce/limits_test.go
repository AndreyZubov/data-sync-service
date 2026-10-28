package salesforce

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/AndreyZubov/data-sync-service/internal/salesforce/sftest"
)

func TestParseLimitInfo(t *testing.T) {
	tests := []struct {
		header      string
		used, limit int64
		ok          bool
	}{
		{"api-usage=25/15000", 25, 15000, true},
		{"api-usage=25/15000; per-app-api-usage=17/250(appName=sample)", 25, 15000, true},
		{"per-app-api-usage=17/250(appName=sample), api-usage=0/5000", 0, 5000, true},
		{"", 0, 0, false},
		{"per-app-api-usage=17/250(appName=sample)", 0, 0, false},
		{"api-usage=25", 0, 0, false},
		{"api-usage=x/15000", 0, 0, false},
		{"api-usage=25/0", 0, 0, false},
		{"api-usage=-1/15000", 0, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.header, func(t *testing.T) {
			used, limit, ok := parseLimitInfo(tt.header)
			assert.Equal(t, tt.ok, ok)
			assert.Equal(t, tt.used, used)
			assert.Equal(t, tt.limit, limit)
		})
	}
}

func TestAPIUsageFraction(t *testing.T) {
	assert.InDelta(t, 0.25, APIUsage{Used: 25, Max: 100}.Fraction(), 1e-9)
	assert.Zero(t, APIUsage{Used: 25}.Fraction(), "an unknown allocation reports no usage")
}

func TestClientTracksAPIUsageFromResponses(t *testing.T) {
	org := sftest.NewServer(t)
	org.SetDescribe(sftest.Describe{Name: "Account"})
	reg := prometheus.NewRegistry()
	c := newMeteredClient(t, org, reg, 0)
	ctx := context.Background()

	_, ok := c.APIUsage()
	assert.False(t, ok, "nothing is known before the first response")

	org.SetAPIUsage(1200, 15000)
	before := time.Now()
	_, err := c.Describe(ctx, "Account")
	require.NoError(t, err)

	u, ok := c.APIUsage()
	require.True(t, ok)
	assert.Equal(t, int64(1200), u.Used)
	assert.Equal(t, int64(15000), u.Max)
	assert.False(t, u.ObservedAt.Before(before))
	assert.InDelta(t, 1200, testutil.ToFloat64(c.metrics.apiUsed), 0)
	assert.InDelta(t, 15000, testutil.ToFloat64(c.metrics.apiMax), 0)

	_, err = c.Describe(ctx, "Contact")
	require.Error(t, err)
	assert.InDelta(t, 1, testutil.ToFloat64(c.metrics.requests.WithLabelValues("200")), 0)
	assert.InDelta(t, 1, testutil.ToFloat64(c.metrics.requests.WithLabelValues("404")), 0)
}

func TestUsageTrackerKeepsTheNewestReport(t *testing.T) {
	var tr usageTracker
	now := time.Now()
	tr.observe(APIUsage{Used: 20, Max: 100, ObservedAt: now})
	tr.observe(APIUsage{Used: 10, Max: 100, ObservedAt: now.Add(-time.Second)})

	u, ok := tr.get()
	require.True(t, ok)
	assert.Equal(t, int64(20), u.Used, "a late response carries older figures")
}

func TestRefreshAPIUsage(t *testing.T) {
	org := sftest.NewServer(t)
	c := newTestClient(t, org)
	org.SetAPIUsage(14000, 15000)

	u, err := c.RefreshAPIUsage(context.Background())

	require.NoError(t, err)
	assert.Equal(t, int64(14000), u.Used)
	assert.Equal(t, int64(15000), u.Max)
	got, ok := c.APIUsage()
	require.True(t, ok)
	assert.Equal(t, u.Used, got.Used)
}

func TestRefreshAPIUsageRejectsAMissingLimit(t *testing.T) {
	org := sftest.NewServer(t)
	org.SetAPIUsage(0, 0)
	c := newTestClient(t, org)

	_, err := c.RefreshAPIUsage(context.Background())

	assert.ErrorContains(t, err, "no DailyApiRequests")
}

func TestRateLimitSpacesRequests(t *testing.T) {
	org := sftest.NewServer(t)
	org.SetDescribe(sftest.Describe{Name: "Account"})
	const perSecond = 20
	c := newMeteredClient(t, org, nil, perSecond)
	ctx := context.Background()

	// The first second's worth goes out at once; the rest wait their turn.
	const requests = perSecond + 5
	start := time.Now()
	var wg sync.WaitGroup
	for range requests {
		wg.Go(func() {
			_, err := c.Describe(ctx, "Account")
			assert.NoError(t, err)
		})
	}
	wg.Wait()

	assert.GreaterOrEqual(t, time.Since(start), 5*time.Second/perSecond-10*time.Millisecond)
}

func TestRateLimitWaitHonoursCancellation(t *testing.T) {
	org := sftest.NewServer(t)
	org.SetDescribe(sftest.Describe{Name: "Account"})
	c := newMeteredClient(t, org, nil, 1)
	_, err := c.Describe(context.Background(), "Account")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = c.Describe(ctx, "Account")

	assert.ErrorContains(t, err, "rate limit")
}

func TestRetriesAreCounted(t *testing.T) {
	org := sftest.NewServer(t)
	h, _ := sequence(http.StatusServiceUnavailable, http.StatusServiceUnavailable)
	org.Handle("GET /flaky", h)
	c := newMeteredClient(t, org, prometheus.NewRegistry(), 0)

	require.NoError(t, c.DoJSON(context.Background(), http.MethodGet, "/flaky", nil, nil))

	assert.InDelta(t, 2, testutil.ToFloat64(c.metrics.retries.WithLabelValues("503")), 0)
}

func newMeteredClient(t *testing.T, org *sftest.Server, reg prometheus.Registerer, rateLimit float64) *Client {
	t.Helper()
	tokens, _ := newProvider(org, time.Hour)
	return NewClient(ClientConfig{
		Tokens:     tokens,
		APIVersion: sftest.APIVersion,
		HTTPClient: org.HTTPClient(),
		Retry:      fastRetry,
		Logger:     zaptest.NewLogger(t),
		RateLimit:  rateLimit,
		Registerer: reg,
	})
}
