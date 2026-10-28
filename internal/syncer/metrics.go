package syncer

import "github.com/prometheus/client_golang/prometheus"

// metrics holds the sync collectors. Object labels are bounded by the
// configured object list.
type metrics struct {
	runs        *prometheus.CounterVec
	records     *prometheus.CounterVec
	failures    *prometheus.CounterVec
	deadLetters *prometheus.CounterVec
	duration    *prometheus.HistogramVec
	active      prometheus.Gauge
	resumed     *prometheus.CounterVec
}

func newMetrics(reg prometheus.Registerer) *metrics {
	m := &metrics{
		runs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "sync_runs_total",
			Help: "Finished sync runs by object, direction, and final status.",
		}, []string{"object", "direction", "status"}),
		records: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "sync_records_total",
			Help: "Records written by sync runs, by object and direction.",
		}, []string{"object", "direction"}),
		failures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "sync_records_failed_total",
			Help: "Records a sync run could not write, by object and direction.",
		}, []string{"object", "direction"}),
		deadLetters: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "sync_dead_letters_total",
			Help: "Outbox entries dead-lettered after failing for good, by object.",
		}, []string{"object"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "sync_run_duration_seconds",
			Help: "Duration of executed sync runs, excluding time queued, by object and direction.",
			// One second to about two hours.
			Buckets: prometheus.ExponentialBuckets(1, 2, 14),
		}, []string{"object", "direction"}),
		active: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "sync_active_runs",
			Help: "Sync runs currently executing.",
		}),
		resumed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "sync_runs_resumed_total",
			Help: "Runs resumed after a service restart, by object and direction.",
		}, []string{"object", "direction"}),
	}
	reg.MustRegister(m.runs, m.records, m.failures, m.deadLetters, m.duration, m.active, m.resumed)
	return m
}
