package salesforce

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// tokenRefreshMargin is how long before expiry a cached token is replaced, so
// a request never starts with a token that expires while it is in flight.
const tokenRefreshMargin = time.Minute

// tokenRequestTimeout bounds a single call to the OAuth token endpoint.
const tokenRequestTimeout = 30 * time.Second

// maxErrorBody caps how much of an error response is read into memory.
const maxErrorBody = 64 << 10

// Secret is a credential that redacts itself when formatted or marshalled, so
// it cannot leak into logs or error messages by accident. Convert it to string
// explicitly where the raw value is needed.
type Secret string

// String implements fmt.Stringer.
func (Secret) String() string { return "[REDACTED]" }

// GoString implements fmt.GoStringer, which covers the %#v verb.
func (Secret) GoString() string { return `"[REDACTED]"` }

// MarshalJSON implements json.Marshaler, which covers reflection-based loggers.
func (Secret) MarshalJSON() ([]byte, error) { return []byte(`"[REDACTED]"`), nil }

// Token is an OAuth access token and the org instance it is valid for.
type Token struct {
	AccessToken Secret
	// InstanceURL is the org's base URL, e.g. https://example.my.salesforce.com.
	InstanceURL string
	// ExpiresAt is when the token should no longer be used.
	ExpiresAt time.Time
}

// TokenProvider supplies access tokens for Salesforce API calls.
type TokenProvider interface {
	// Token returns a token valid for at least tokenRefreshMargin, fetching a
	// new one when needed.
	Token(ctx context.Context) (Token, error)
	// Invalidate discards t after Salesforce rejected it, so the next Token call
	// fetches a fresh one. A token other than the cached one is ignored.
	Invalidate(t Token)
}

// AuthError is returned when the OAuth token endpoint rejects a request.
type AuthError struct {
	StatusCode int
	// Code is the OAuth error code, e.g. "invalid_client".
	Code string
	// Description is Salesforce's human-readable explanation.
	Description string
}

func (e *AuthError) Error() string {
	return fmt.Sprintf("salesforce token endpoint: %d %s: %s", e.StatusCode, e.Code, e.Description)
}

// ClientCredentialsConfig configures a ClientCredentials token provider.
type ClientCredentialsConfig struct {
	// LoginURL is the org's My Domain URL. The client credentials flow is not
	// available on login.salesforce.com or test.salesforce.com.
	LoginURL     string
	ClientID     string
	ClientSecret Secret
	// TokenTTL is how long a token is used when the response does not state its
	// lifetime. Keep it below the connected app's session timeout.
	TokenTTL time.Duration
	// HTTPClient sends token requests; NewHTTPClient() is used when nil.
	HTTPClient *http.Client
}

// ClientCredentials is a TokenProvider for the OAuth 2.0 client credentials
// flow. It caches the token and is safe for concurrent use: concurrent callers
// that find no valid token share a single token request.
type ClientCredentials struct {
	cfg  ClientCredentialsConfig
	http *http.Client
	now  func() time.Time

	// fetching serialises token requests; a channel rather than a mutex lets a
	// waiting caller give up when its context ends.
	fetching chan struct{}

	mu    sync.Mutex
	token Token
}

// NewClientCredentials returns a provider for the given connected app.
func NewClientCredentials(cfg ClientCredentialsConfig) *ClientCredentials {
	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = NewHTTPClient()
	}
	return &ClientCredentials{
		cfg:      cfg,
		http:     httpClient,
		now:      time.Now,
		fetching: make(chan struct{}, 1),
	}
}

// Token implements TokenProvider.
func (p *ClientCredentials) Token(ctx context.Context) (Token, error) {
	if t, ok := p.cached(); ok {
		return t, nil
	}

	select {
	case p.fetching <- struct{}{}:
	case <-ctx.Done():
		return Token{}, ctx.Err()
	}
	defer func() { <-p.fetching }()

	// Another caller may have fetched a token while this one waited.
	if t, ok := p.cached(); ok {
		return t, nil
	}

	t, err := p.fetch(ctx)
	if err != nil {
		return Token{}, err
	}
	p.mu.Lock()
	p.token = t
	p.mu.Unlock()
	return t, nil
}

// Invalidate implements TokenProvider. Comparing tokens keeps a caller holding
// an old rejected token from discarding a fresh one another caller fetched.
func (p *ClientCredentials) Invalidate(t Token) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.token.AccessToken == t.AccessToken {
		p.token = Token{}
	}
}

func (p *ClientCredentials) cached() (Token, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.token.AccessToken == "" || !p.now().Before(p.token.ExpiresAt.Add(-tokenRefreshMargin)) {
		return Token{}, false
	}
	return p.token, true
}

// tokenResponse is the token endpoint's success body. expires_in is optional:
// Salesforce ties token lifetime to the session timeout and may omit it.
type tokenResponse struct {
	AccessToken string `json:"access_token"`
	InstanceURL string `json:"instance_url"`
	ExpiresIn   int    `json:"expires_in"`
}

type tokenErrorResponse struct {
	Error            string `json:"error"`
	ErrorDescription string `json:"error_description"`
}

func (p *ClientCredentials) fetch(ctx context.Context) (Token, error) {
	ctx, cancel := context.WithTimeout(ctx, tokenRequestTimeout)
	defer cancel()

	endpoint, err := url.JoinPath(p.cfg.LoginURL, "services", "oauth2", "token")
	if err != nil {
		return Token{}, fmt.Errorf("build token URL: %w", err)
	}
	form := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {p.cfg.ClientID},
		"client_secret": {string(p.cfg.ClientSecret)},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return Token{}, fmt.Errorf("build token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	// Measured before the request so network latency shortens, never extends,
	// the token's assumed lifetime. The local clock is used instead of the
	// response's issued_at to stay immune to clock skew.
	requestedAt := p.now()
	resp, err := p.http.Do(req)
	if err != nil {
		return Token{}, fmt.Errorf("salesforce token request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return Token{}, readAuthError(resp)
	}

	var body tokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return Token{}, fmt.Errorf("decode token response: %w", err)
	}
	if body.AccessToken == "" {
		return Token{}, errors.New("salesforce token response has no access_token")
	}
	if err := validateInstanceURL(body.InstanceURL); err != nil {
		return Token{}, err
	}

	ttl := p.cfg.TokenTTL
	if body.ExpiresIn > 0 {
		ttl = time.Duration(body.ExpiresIn) * time.Second
	}
	return Token{
		AccessToken: Secret(body.AccessToken),
		InstanceURL: strings.TrimRight(body.InstanceURL, "/"),
		ExpiresAt:   requestedAt.Add(ttl),
	}, nil
}

// validateInstanceURL refuses anything but HTTPS: the access token is sent to
// this URL with every API call.
func validateInstanceURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Host == "" {
		return fmt.Errorf("salesforce token response has invalid instance_url %q", raw)
	}
	return nil
}

func readAuthError(resp *http.Response) error {
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
	authErr := &AuthError{StatusCode: resp.StatusCode}
	var body tokenErrorResponse
	if json.Unmarshal(raw, &body) == nil && body.Error != "" {
		authErr.Code = body.Error
		authErr.Description = body.ErrorDescription
	} else {
		authErr.Code = http.StatusText(resp.StatusCode)
		authErr.Description = truncate(strings.TrimSpace(string(raw)), 200)
	}
	return authErr
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	// Cutting at a byte offset can split a multi-byte rune; drop the remnant.
	return strings.ToValidUTF8(s[:n], "") + "…"
}
