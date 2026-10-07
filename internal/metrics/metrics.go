// Package metrics provides tiny, dependency-free Prometheus-style metrics.
package metrics

import (
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Upper bounds (in microseconds) of the latency histogram buckets.
var latencyBoundsMicros = []int64{50, 100, 250, 500, 1000, 2500, 5000, 10000, 50000}

// Registry holds all counters exposed by the service.
type Registry struct {
	allowed  atomic.Int64
	denied   atomic.Int64
	errors   atomic.Int64
	fallback atomic.Int64
	failures atomic.Int64

	buckets []atomic.Int64 // len(latencyBoundsMicros)+1, last is +Inf
	sumUs   atomic.Int64
	count   atomic.Int64

	mu       sync.Mutex
	backends map[string]*atomic.Int64
}

// New creates an empty registry.
func New() *Registry {
	return &Registry{
		buckets:  make([]atomic.Int64, len(latencyBoundsMicros)+1),
		backends: make(map[string]*atomic.Int64),
	}
}

// Decision records one rate-limit decision and how long it took.
func (r *Registry) Decision(allowed bool, backend string, d time.Duration) {
	if allowed {
		r.allowed.Add(1)
	} else {
		r.denied.Add(1)
	}
	r.backendCounter(backend).Add(1)

	us := d.Microseconds()
	r.sumUs.Add(us)
	r.count.Add(1)
	i := sort.Search(len(latencyBoundsMicros), func(i int) bool { return us <= latencyBoundsMicros[i] })
	r.buckets[i].Add(1)
}

// Error records a request that could not be decided at all.
func (r *Registry) Error() { r.errors.Add(1) }

// BackendEvent records a backend failure or a fallback activation.
func (r *Registry) BackendEvent(kind string) {
	switch kind {
	case "failure":
		r.failures.Add(1)
	case "fallback":
		r.fallback.Add(1)
	}
}

// Snapshot returns basic totals, useful in tests.
func (r *Registry) Snapshot() (allowed, denied, errs, fallback int64) {
	return r.allowed.Load(), r.denied.Load(), r.errors.Load(), r.fallback.Load()
}

func (r *Registry) backendCounter(name string) *atomic.Int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok := r.backends[name]
	if !ok {
		c = new(atomic.Int64)
		r.backends[name] = c
	}
	return c
}

// WritePrometheus renders the registry in Prometheus text exposition format.
func (r *Registry) WritePrometheus(w io.Writer, breakerStates map[string]string) {
	fmt.Fprintf(w, "# TYPE ratelimiter_decisions_total counter\n")
	fmt.Fprintf(w, "ratelimiter_decisions_total{result=\"allowed\"} %d\n", r.allowed.Load())
	fmt.Fprintf(w, "ratelimiter_decisions_total{result=\"denied\"} %d\n", r.denied.Load())
	fmt.Fprintf(w, "# TYPE ratelimiter_errors_total counter\nratelimiter_errors_total %d\n", r.errors.Load())
	fmt.Fprintf(w, "# TYPE ratelimiter_backend_failures_total counter\nratelimiter_backend_failures_total %d\n", r.failures.Load())
	fmt.Fprintf(w, "# TYPE ratelimiter_fallback_total counter\nratelimiter_fallback_total %d\n", r.fallback.Load())

	r.mu.Lock()
	names := make([]string, 0, len(r.backends))
	for n := range r.backends {
		names = append(names, n)
	}
	sort.Strings(names)
	fmt.Fprintf(w, "# TYPE ratelimiter_backend_decisions_total counter\n")
	for _, n := range names {
		fmt.Fprintf(w, "ratelimiter_backend_decisions_total{backend=%q} %d\n", n, r.backends[n].Load())
	}
	r.mu.Unlock()

	fmt.Fprintf(w, "# TYPE ratelimiter_decision_latency_seconds histogram\n")
	var cum int64
	for i, b := range latencyBoundsMicros {
		cum += r.buckets[i].Load()
		fmt.Fprintf(w, "ratelimiter_decision_latency_seconds_bucket{le=\"%g\"} %d\n", float64(b)/1e6, cum)
	}
	cum += r.buckets[len(latencyBoundsMicros)].Load()
	fmt.Fprintf(w, "ratelimiter_decision_latency_seconds_bucket{le=\"+Inf\"} %d\n", cum)
	fmt.Fprintf(w, "ratelimiter_decision_latency_seconds_sum %g\n", float64(r.sumUs.Load())/1e6)
	fmt.Fprintf(w, "ratelimiter_decision_latency_seconds_count %d\n", r.count.Load())

	keys := make([]string, 0, len(breakerStates))
	for k := range breakerStates {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	fmt.Fprintf(w, "# TYPE ratelimiter_breaker_open gauge\n")
	for _, k := range keys {
		v := 0
		if breakerStates[k] != "closed" {
			v = 1
		}
		fmt.Fprintf(w, "ratelimiter_breaker_open{backend=%q} %d\n", k, v)
	}
}
