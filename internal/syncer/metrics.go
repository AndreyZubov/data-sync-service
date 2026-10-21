package syncer

import "github.com/prometheus/client_golang/prometheus"

// metrics holds the sync collectors. Object labels are bounded by the
// configured object list.
type metrics struct {
	runs     *prometheus.CounterVec
	records  *prometheus.CounterVec
	duration *prometheus.HistogramVec
	active   prometheus.Gauge
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
	}
	reg.MustRegister(m.runs, m.records, m.duration, m.active)
	return m
}
