package salesforce

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

// clientMetrics holds the client's collectors. Label values are bounded:
// HTTP status codes and a fixed set of retry reasons.
type clientMetrics struct {
	requests *prometheus.CounterVec
	retries  *prometheus.CounterVec
	apiUsed  prometheus.Gauge
	apiMax   prometheus.Gauge
}

func newClientMetrics(reg prometheus.Registerer) *clientMetrics {
	m := &clientMetrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "salesforce_requests_total",
			Help: "Salesforce data API responses by HTTP status code; \"error\" counts requests that got no response.",
		}, []string{"code"}),
		retries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "salesforce_request_retries_total",
			Help: "Salesforce data API requests resent, by the HTTP status code or network error that caused it.",
		}, []string{"reason"}),
		apiUsed: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "salesforce_api_usage_requests",
			Help: "API requests the org has used of its rolling 24-hour allocation, as last reported by Salesforce.",
		}),
		apiMax: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "salesforce_api_usage_limit",
			Help: "The org's rolling 24-hour API request allocation, as last reported by Salesforce.",
		}),
	}
	if reg != nil {
		reg.MustRegister(m.requests, m.retries, m.apiUsed, m.apiMax)
	}
	return m
}

func (m *clientMetrics) response(status int) {
	m.requests.WithLabelValues(strconv.Itoa(status)).Inc()
}

func (m *clientMetrics) usage(u APIUsage) {
	m.apiUsed.Set(float64(u.Used))
	m.apiMax.Set(float64(u.Max))
}
