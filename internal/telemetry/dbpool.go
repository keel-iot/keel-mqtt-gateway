package telemetry

import (
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

var dbPoolMetricsOnce sync.Once

// RegisterDBPoolMetrics exposes live pgxpool statistics for the process-local
// PostgreSQL pool. Snapshot counters are intentionally gauges because pgxpool
// owns their cumulative values and resets them when the process restarts.
func RegisterDBPoolMetrics(pool *pgxpool.Pool) {
	if pool == nil {
		return
	}
	dbPoolMetricsOnce.Do(func() {
		register := func(name, help string, value func(*pgxpool.Stat) float64) {
			prometheus.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
				Namespace: "keel_gateway",
				Subsystem: "db_pool",
				Name:      name,
				Help:      help,
			}, func() float64 { return value(pool.Stat()) }))
		}

		register("max_connections", "Configured maximum PostgreSQL pool connections.", func(s *pgxpool.Stat) float64 {
			return float64(s.MaxConns())
		})
		register("total_connections", "Current total PostgreSQL pool connections.", func(s *pgxpool.Stat) float64 {
			return float64(s.TotalConns())
		})
		register("idle_connections", "Current idle PostgreSQL pool connections.", func(s *pgxpool.Stat) float64 {
			return float64(s.IdleConns())
		})
		register("acquired_connections", "Current PostgreSQL pool connections acquired by callers.", func(s *pgxpool.Stat) float64 {
			return float64(s.AcquiredConns())
		})
		register("constructing_connections", "Current PostgreSQL pool connections being constructed.", func(s *pgxpool.Stat) float64 {
			return float64(s.ConstructingConns())
		})
		register("acquire_count", "Cumulative PostgreSQL pool acquire operations since process start.", func(s *pgxpool.Stat) float64 {
			return float64(s.AcquireCount())
		})
		register("canceled_acquire_count", "Cumulative PostgreSQL pool acquire operations canceled by context since process start.", func(s *pgxpool.Stat) float64 {
			return float64(s.CanceledAcquireCount())
		})
		register("empty_acquire_count", "Cumulative PostgreSQL pool acquire operations that waited for an empty pool since process start.", func(s *pgxpool.Stat) float64 {
			return float64(s.EmptyAcquireCount())
		})
		register("acquire_duration_seconds", "Cumulative time spent acquiring PostgreSQL pool connections.", func(s *pgxpool.Stat) float64 {
			return s.AcquireDuration().Seconds()
		})
		register("empty_acquire_wait_seconds", "Cumulative time spent waiting while the PostgreSQL pool had no immediately available connection.", func(s *pgxpool.Stat) float64 {
			return s.EmptyAcquireWaitTime().Seconds()
		})
	})
}
