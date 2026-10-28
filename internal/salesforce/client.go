// Package salesforce is a client for the Salesforce REST and Bulk API 2.0:
// OAuth token management, authenticated requests, retries, and error decoding.
package salesforce

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/time/rate"

	"github.com/AndreyZubov/data-sync-service/internal/retry"
)

// userAgent identifies the service in the org's API usage and login history.
const userAgent = "data-sync-service"

// NewHTTPClient returns an HTTP client tuned for Salesforce. It sets no overall
// timeout, because Bulk API result downloads can legitimately take minutes;
// callers bound each call with a context instead.
func NewHTTPClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = time.Minute
	// Every call goes to the same instance host; the default of 2 idle
	// connections per host would force concurrent jobs to reconnect constantly.
	transport.MaxIdleConnsPerHost = 16
	return &http.Client{Transport: transport}
}

// APIError is a non-2xx response from the Salesforce API.
type APIError struct {
	StatusCode int
	// Code is Salesforce's errorCode, e.g. "INVALID_SESSION_ID" or
	// "REQUEST_LIMIT_EXCEEDED"; empty when the body carried none.
	Code    string
	Message string
}

func (e *APIError) Error() string {
	if e.Code == "" {
		return fmt.Sprintf("salesforce API: %d: %s", e.StatusCode, e.Message)
	}
	return fmt.Sprintf("salesforce API: %d %s: %s", e.StatusCode, e.Code, e.Message)
}

// Request describes a call to the org's data API.
type Request struct {
	Method string
	// Path is relative to /services/data/vXX.X, e.g. "/jobs/query".
	Path   string
	Query  url.Values
	Header http.Header
	// Body returns a fresh reader for every attempt, so a request can be
	// resent; nil means no body.
	Body func() (io.Reader, error)
}

// ClientConfig configures a Client.
type ClientConfig struct {
	Tokens TokenProvider
	// APIVersion is the API version without the "v" prefix, e.g. "60.0".
	APIVersion string
	// HTTPClient sends API requests; NewHTTPClient() is used when nil.
	HTTPClient *http.Client
	// Retry governs resending failed requests; retry.DefaultPolicy() is used
	// when MaxAttempts is zero.
	Retry retry.Policy
	// Logger records retries; a no-op logger is used when nil.
	Logger *zap.Logger
	// JobPollInterval is the first wait between Bulk API job status checks;
	// it doubles after every check up to JobPollMaxInterval. Zero values
	// default to one and thirty seconds.
	JobPollInterval    time.Duration
	JobPollMaxInterval time.Duration
	// RateLimit caps the requests sent per second, retries included, with
	// bursts of up to one second's worth; zero means no limit.
	RateLimit float64
	// Registerer receives the client's metrics; nil leaves them unregistered.
	Registerer prometheus.Registerer
}

// Client sends authenticated requests to the Salesforce data API. It is safe
// for concurrent use.
type Client struct {
	tokens     TokenProvider
	apiVersion string
	http       *http.Client
	retry      retry.Policy
	log        *zap.Logger
	limiter    *rate.Limiter // nil when unlimited
	usage      *usageTracker
	metrics    *clientMetrics

	pollInterval    time.Duration
	pollMaxInterval time.Duration
}

// NewClient returns a client for the given configuration.
func NewClient(cfg ClientConfig) *Client {
	c := &Client{
		tokens:     cfg.Tokens,
		apiVersion: cfg.APIVersion,
		http:       cfg.HTTPClient,
		retry:      cfg.Retry,
		log:        cfg.Logger,
		metrics:    newClientMetrics(cfg.Registerer),

		pollInterval:    cfg.JobPollInterval,
		pollMaxInterval: cfg.JobPollMaxInterval,
	}
	c.usage = &usageTracker{onObserve: c.metrics.usage}
	if cfg.RateLimit > 0 {
		c.limiter = rate.NewLimiter(rate.Limit(cfg.RateLimit), max(1, int(cfg.RateLimit)))
	}
	if c.http == nil {
		c.http = NewHTTPClient()
	}
	if c.retry.MaxAttempts == 0 {
		c.retry = retry.DefaultPolicy()
	}
	if c.log == nil {
		c.log = zap.NewNop()
	}
	if c.pollInterval <= 0 {
		c.pollInterval = defaultJobPollInterval
	}
	if c.pollMaxInterval <= 0 {
		c.pollMaxInterval = defaultJobPollMaxInterval
	}
	return c
}

// Do sends req and returns the response if its status is 2xx; the caller must
// close its body. Any other status is returned as an *APIError.
//
// Rate limiting (429) and unavailability (503) are retried for every method,
// because Salesforce did not act on the request; other 5xx responses and
// network errors are retried only for idempotent methods, where resending
// cannot apply the request twice. A 401 is answered once by fetching a new
// token, without using up a retry.
func (c *Client) Do(ctx context.Context, req Request) (*http.Response, error) {
	reauthenticated := false
	for attempt := 1; ; attempt++ {
		token, err := c.tokens.Token(ctx)
		if err != nil {
			return nil, fmt.Errorf("salesforce access token: %w", err)
		}

		resp, err := c.send(ctx, token, req)
		if err != nil {
			if ctx.Err() != nil || !idempotent(req.Method) || attempt >= c.retry.MaxAttempts {
				return nil, fmt.Errorf("%s %s: %w", req.Method, req.Path, err)
			}
			if err := c.wait(ctx, req, attempt, c.retry.Backoff(attempt), "error", zap.Error(err)); err != nil {
				return nil, err
			}
			continue
		}

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return resp, nil
		}
		apiErr := readAPIError(resp)

		// A session can end before the token's assumed lifetime, for example when
		// an admin revokes it, so one fresh token is worth trying.
		if resp.StatusCode == http.StatusUnauthorized && !reauthenticated {
			c.tokens.Invalidate(token)
			reauthenticated = true
			attempt--
			continue
		}

		delay, ok := c.retryDelay(req.Method, resp, attempt)
		if !ok {
			return nil, apiErr
		}
		if err := c.wait(ctx, req, attempt, delay, strconv.Itoa(resp.StatusCode), zap.Int("status", resp.StatusCode)); err != nil {
			return nil, err
		}
	}
}

// DoJSON sends in as a JSON body (none when nil) and decodes a JSON response
// into out (discarded when nil).
func (c *Client) DoJSON(ctx context.Context, method, path string, in, out any) error {
	req := Request{Method: method, Path: path, Header: http.Header{}}
	req.Header.Set("Accept", "application/json")
	if in != nil {
		payload, err := json.Marshal(in)
		if err != nil {
			return fmt.Errorf("encode %s %s request: %w", method, path, err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Body = func() (io.Reader, error) { return bytes.NewReader(payload), nil }
	}

	resp, err := c.Do(ctx, req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if out == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode %s %s response: %w", method, path, err)
	}
	return nil
}

// send makes one attempt at req, once the rate limit allows it, and records
// the org's API usage reported by the response.
func (c *Client) send(ctx context.Context, token Token, req Request) (*http.Response, error) {
	if c.limiter != nil {
		if err := c.limiter.Wait(ctx); err != nil {
			return nil, fmt.Errorf("wait for rate limit: %w", err)
		}
	}
	base, err := url.Parse(token.InstanceURL)
	if err != nil {
		return nil, fmt.Errorf("parse instance URL: %w", err)
	}
	target := base.JoinPath("services", "data", "v"+c.apiVersion, req.Path)
	target.RawQuery = req.Query.Encode()

	var body io.Reader
	if req.Body != nil {
		if body, err = req.Body(); err != nil {
			return nil, fmt.Errorf("open request body: %w", err)
		}
	}
	httpReq, err := http.NewRequestWithContext(ctx, req.Method, target.String(), body)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	if req.Header != nil {
		httpReq.Header = req.Header.Clone()
	}
	httpReq.Header.Set("Authorization", "Bearer "+string(token.AccessToken))
	httpReq.Header.Set("User-Agent", userAgent)

	resp, err := c.http.Do(httpReq)
	if err != nil {
		c.metrics.requests.WithLabelValues("error").Inc()
		return nil, err
	}
	c.metrics.response(resp.StatusCode)
	if used, limit, ok := parseLimitInfo(resp.Header.Get("Sforce-Limit-Info")); ok {
		c.usage.observe(APIUsage{Used: used, Max: limit, ObservedAt: time.Now()})
	}
	return resp, nil
}

// retryDelay decides whether a failed response is worth resending and after
// how long. REQUEST_LIMIT_EXCEEDED (403) is deliberately final: the org's daily
// API quota is spent, and retrying would only burn what other integrations have left.
func (c *Client) retryDelay(method string, resp *http.Response, attempt int) (time.Duration, bool) {
	if attempt >= c.retry.MaxAttempts {
		return 0, false
	}
	switch resp.StatusCode {
	case http.StatusTooManyRequests, http.StatusServiceUnavailable:
		if d, ok := retry.ParseRetryAfter(resp.Header.Get("Retry-After"), time.Now()); ok {
			// Waiting longer than the policy allows would stall the caller; give
			// up instead of retrying early against the server's explicit request.
			return d, d <= c.retry.MaxDelay
		}
		return c.retry.Backoff(attempt), true
	case http.StatusInternalServerError, http.StatusBadGateway, http.StatusGatewayTimeout:
		return c.retry.Backoff(attempt), idempotent(method)
	default:
		return 0, false
	}
}

// wait logs a retry and sleeps for delay. reason labels the retry metric.
func (c *Client) wait(ctx context.Context, req Request, attempt int, delay time.Duration, reason string, cause zap.Field) error {
	c.metrics.retries.WithLabelValues(reason).Inc()
	c.log.Warn("retrying salesforce request",
		zap.String("method", req.Method),
		zap.String("path", req.Path),
		zap.Int("attempt", attempt),
		zap.Duration("delay", delay),
		cause,
	)
	if err := retry.Sleep(ctx, delay); err != nil {
		return fmt.Errorf("%s %s: %w", req.Method, req.Path, err)
	}
	return nil
}

// idempotent reports whether sending a request with method twice has the same
// effect as sending it once (RFC 9110, section 9.2.2).
func idempotent(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodPut, http.MethodDelete:
		return true
	default:
		return false
	}
}

// apiErrorBody is one entry of Salesforce's error response, which is usually
// a JSON array of these and occasionally a single object.
type apiErrorBody struct {
	ErrorCode string `json:"errorCode"`
	Message   string `json:"message"`
}

func readAPIError(resp *http.Response) *APIError {
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
	apiErr := &APIError{StatusCode: resp.StatusCode}

	var list []apiErrorBody
	var single apiErrorBody
	switch {
	case json.Unmarshal(raw, &list) == nil && len(list) > 0:
		apiErr.Code, apiErr.Message = list[0].ErrorCode, list[0].Message
	case json.Unmarshal(raw, &single) == nil && single.ErrorCode != "":
		apiErr.Code, apiErr.Message = single.ErrorCode, single.Message
	default:
		apiErr.Message = truncate(strings.TrimSpace(string(raw)), 200)
	}
	if apiErr.Message == "" {
		apiErr.Message = http.StatusText(resp.StatusCode)
	}
	return apiErr
}
