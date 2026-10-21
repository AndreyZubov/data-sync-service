package salesforce

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/AndreyZubov/data-sync-service/internal/salesforce/sftest"
)

// fakeClock is a manually advanced clock for token expiry tests.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// newProvider returns a provider for org with a controllable clock.
func newProvider(org *sftest.Server, ttl time.Duration) (*ClientCredentials, *fakeClock) {
	p := NewClientCredentials(ClientCredentialsConfig{
		LoginURL:     org.URL(),
		ClientID:     sftest.ClientID,
		ClientSecret: sftest.ClientSecret,
		TokenTTL:     ttl,
		HTTPClient:   org.HTTPClient(),
	})
	clock := &fakeClock{now: time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)}
	p.now = clock.Now
	return p, clock
}

func TestTokenIsFetchedOnceAndCached(t *testing.T) {
	org := sftest.NewServer(t)
	p, _ := newProvider(org, 30*time.Minute)

	first, err := p.Token(context.Background())
	require.NoError(t, err)
	second, err := p.Token(context.Background())
	require.NoError(t, err)

	assert.NotEmpty(t, first.AccessToken)
	assert.Equal(t, org.URL(), first.InstanceURL)
	assert.Equal(t, first, second)
	assert.Equal(t, 1, org.TokenRequests())
}

func TestTokenIsRefreshedBeforeExpiry(t *testing.T) {
	org := sftest.NewServer(t)
	p, clock := newProvider(org, 30*time.Minute)

	first, err := p.Token(context.Background())
	require.NoError(t, err)

	clock.Advance(30*time.Minute - tokenRefreshMargin - time.Second)
	_, err = p.Token(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, org.TokenRequests(), "still outside the refresh margin")

	clock.Advance(time.Second)
	second, err := p.Token(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 2, org.TokenRequests(), "inside the refresh margin")
	assert.NotEqual(t, first.AccessToken, second.AccessToken)
}

func TestTokenLifetimeFromExpiresIn(t *testing.T) {
	org := sftest.NewServer(t)
	org.OverrideTokenResponse(http.StatusOK, fmt.Sprintf(
		`{"access_token":"tok","instance_url":%q,"expires_in":600}`, org.URL()))
	p, clock := newProvider(org, 2*time.Hour)

	token, err := p.Token(context.Background())

	require.NoError(t, err)
	assert.Equal(t, clock.Now().Add(10*time.Minute), token.ExpiresAt, "expires_in wins over the configured TTL")
}

func TestTokenRequestFailures(t *testing.T) {
	tests := []struct {
		name     string
		status   int
		body     func(org *sftest.Server) string
		wantCode string
		wantMsg  string
	}{
		{
			name:     "OAuth error",
			status:   http.StatusBadRequest,
			body:     func(*sftest.Server) string { return `{"error":"invalid_grant","error_description":"no run-as user"}` },
			wantCode: "invalid_grant",
		},
		{
			name:     "non-JSON error page",
			status:   http.StatusBadGateway,
			body:     func(*sftest.Server) string { return "<html>upstream down</html>" },
			wantCode: "Bad Gateway",
		},
		{
			name:    "plain-HTTP instance URL",
			status:  http.StatusOK,
			body:    func(*sftest.Server) string { return `{"access_token":"tok","instance_url":"http://evil.example"}` },
			wantMsg: "invalid instance_url",
		},
		{
			name:    "missing access token",
			status:  http.StatusOK,
			body:    func(org *sftest.Server) string { return fmt.Sprintf(`{"instance_url":%q}`, org.URL()) },
			wantMsg: "no access_token",
		},
		{
			name:    "malformed success body",
			status:  http.StatusOK,
			body:    func(*sftest.Server) string { return "not json" },
			wantMsg: "decode token response",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			org := sftest.NewServer(t)
			org.OverrideTokenResponse(tt.status, tt.body(org))
			p, _ := newProvider(org, time.Hour)

			_, err := p.Token(context.Background())

			require.Error(t, err)
			if tt.wantCode != "" {
				var authErr *AuthError
				require.ErrorAs(t, err, &authErr)
				assert.Equal(t, tt.status, authErr.StatusCode)
				assert.Equal(t, tt.wantCode, authErr.Code)
			}
			if tt.wantMsg != "" {
				assert.Contains(t, err.Error(), tt.wantMsg)
			}
		})
	}
}

func TestWrongCredentialsAreReportedWithoutTheSecret(t *testing.T) {
	org := sftest.NewServer(t)
	p := NewClientCredentials(ClientCredentialsConfig{
		LoginURL:     org.URL(),
		ClientID:     sftest.ClientID,
		ClientSecret: "wrong-secret",
		TokenTTL:     time.Hour,
		HTTPClient:   org.HTTPClient(),
	})

	_, err := p.Token(context.Background())

	var authErr *AuthError
	require.ErrorAs(t, err, &authErr)
	assert.Equal(t, "invalid_client", authErr.Code)
	assert.NotContains(t, err.Error(), "wrong-secret")
}

func TestConcurrentCallersShareOneTokenRequest(t *testing.T) {
	org := sftest.NewServer(t)
	p, _ := newProvider(org, time.Hour)

	var wg sync.WaitGroup
	tokens := make([]Token, 20)
	errs := make([]error, len(tokens))
	for i := range tokens {
		wg.Go(func() { tokens[i], errs[i] = p.Token(context.Background()) })
	}
	wg.Wait()

	for i := range tokens {
		require.NoError(t, errs[i])
		assert.Equal(t, tokens[0], tokens[i])
	}
	assert.Equal(t, 1, org.TokenRequests())
}

func TestWaitingForATokenRespectsTheContext(t *testing.T) {
	org := sftest.NewServer(t)
	p, _ := newProvider(org, time.Hour)
	p.fetching <- struct{}{} // another caller is mid-fetch
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := p.Token(ctx)

	require.ErrorIs(t, err, context.Canceled)
	assert.Zero(t, org.TokenRequests())
}

func TestInvalidate(t *testing.T) {
	org := sftest.NewServer(t)
	p, _ := newProvider(org, time.Hour)
	ctx := context.Background()

	stale, err := p.Token(ctx)
	require.NoError(t, err)
	p.Invalidate(stale)

	fresh, err := p.Token(ctx)
	require.NoError(t, err)
	assert.NotEqual(t, stale.AccessToken, fresh.AccessToken)
	assert.Equal(t, 2, org.TokenRequests())

	// A late caller reporting the stale token must not discard the fresh one.
	p.Invalidate(stale)
	again, err := p.Token(ctx)
	require.NoError(t, err)
	assert.Equal(t, fresh, again)
	assert.Equal(t, 2, org.TokenRequests())
}

func TestSecretIsRedacted(t *testing.T) {
	token := Token{AccessToken: "s3cr3t", InstanceURL: "https://example.my.salesforce.com"}

	for _, format := range []string{"%v", "%+v", "%#v", "%s"} {
		assert.NotContains(t, fmt.Sprintf(format, token), "s3cr3t", format)
	}
	raw, err := json.Marshal(token)
	require.NoError(t, err)
	assert.NotContains(t, string(raw), "s3cr3t")
	assert.Equal(t, "s3cr3t", string(token.AccessToken), "explicit conversion still reveals it")
}
