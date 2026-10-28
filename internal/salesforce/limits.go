package salesforce

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// APIUsage is the org's consumption of its rolling 24-hour API request
// allocation, which every integration of the org shares.
type APIUsage struct {
	Used int64
	Max  int64
	// ObservedAt is when Salesforce reported the figures.
	ObservedAt time.Time
}

// Fraction returns Used as a fraction of Max, or 0 when Max is unknown.
func (u APIUsage) Fraction() float64 {
	if u.Max <= 0 {
		return 0
	}
	return float64(u.Used) / float64(u.Max)
}

// usageTracker keeps the latest API usage Salesforce reported.
type usageTracker struct {
	mu    sync.Mutex
	usage APIUsage
	known bool
	// onObserve publishes every observation, e.g. to metrics.
	onObserve func(APIUsage)
}

func (t *usageTracker) observe(u APIUsage) {
	t.mu.Lock()
	// Concurrent responses can arrive out of order; an older report must not
	// replace a newer one.
	if t.known && u.ObservedAt.Before(t.usage.ObservedAt) {
		t.mu.Unlock()
		return
	}
	t.usage, t.known = u, true
	t.mu.Unlock()
	if t.onObserve != nil {
		t.onObserve(u)
	}
}

func (t *usageTracker) get() (APIUsage, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.usage, t.known
}

// parseLimitInfo reads the org-wide figures from a Sforce-Limit-Info header,
// e.g. "api-usage=25/15000" or "api-usage=25/15000; per-app-api-usage=17/250(appName=x)".
// ok is false when the header carries no well-formed api-usage entry.
func parseLimitInfo(header string) (used, limit int64, ok bool) {
	for _, item := range strings.FieldsFunc(header, func(r rune) bool { return r == ';' || r == ',' }) {
		key, value, found := strings.Cut(strings.TrimSpace(item), "=")
		if !found || key != "api-usage" {
			continue
		}
		u, m, found := strings.Cut(value, "/")
		if !found {
			return 0, 0, false
		}
		used, err1 := strconv.ParseInt(u, 10, 64)
		limit, err2 := strconv.ParseInt(m, 10, 64)
		if err1 != nil || err2 != nil || used < 0 || limit <= 0 {
			return 0, 0, false
		}
		return used, limit, true
	}
	return 0, 0, false
}

// APIUsage returns the org's API usage as last reported by Salesforce, which
// attaches it to every data API response. ok is false until the first
// response arrives.
func (c *Client) APIUsage() (APIUsage, bool) {
	return c.usage.get()
}

// RefreshAPIUsage asks Salesforce for the org's current API usage, for when
// the last report is too old to act on.
func (c *Client) RefreshAPIUsage(ctx context.Context) (APIUsage, error) {
	var out struct {
		DailyAPIRequests struct {
			Max       int64 `json:"Max"`
			Remaining int64 `json:"Remaining"`
		} `json:"DailyApiRequests"`
	}
	if err := c.DoJSON(ctx, http.MethodGet, "/limits", nil, &out); err != nil {
		return APIUsage{}, fmt.Errorf("get org limits: %w", err)
	}
	d := out.DailyAPIRequests
	if d.Max <= 0 {
		return APIUsage{}, fmt.Errorf("get org limits: response has no DailyApiRequests")
	}
	u := APIUsage{Used: d.Max - d.Remaining, Max: d.Max, ObservedAt: time.Now()}
	c.usage.observe(u)
	return u, nil
}
