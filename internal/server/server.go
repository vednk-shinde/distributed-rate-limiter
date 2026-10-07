// Package server exposes the limiter over HTTP.
package server

import (
	"encoding/json"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/vednk-shinde/distributed-rate-limiter/internal/limiter"
	"github.com/vednk-shinde/distributed-rate-limiter/internal/metrics"
)

// StateReporter exposes per-backend health (implemented by *limiter.Failover).
type StateReporter interface{ States() map[string]string }

// Server wires a Limiter to HTTP handlers.
type Server struct {
	lim     limiter.Limiter
	metrics *metrics.Registry
	states  StateReporter
	// FailOpen controls behaviour when no backend can decide: allow the request
	// (true) or reject it with 503 (false).
	FailOpen bool
}

// New creates a Server. states may be nil.
func New(lim limiter.Limiter, m *metrics.Registry, states StateReporter) *Server {
	return &Server{lim: lim, metrics: m, states: states, FailOpen: true}
}

// Handler returns the HTTP routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/check", s.check)
	mux.HandleFunc("/healthz", s.health)
	mux.HandleFunc("/metrics", s.metricsHandler)
	return mux
}

type checkResponse struct {
	Key        string `json:"key"`
	Allowed    bool   `json:"allowed"`
	Remaining  int64  `json:"remaining"`
	RetryAfter int64  `json:"retry_after_ms"`
	Backend    string `json:"backend"`
	Degraded   bool   `json:"degraded"`
}

func (s *Server) check(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, `{"error":"missing key"}`, http.StatusBadRequest)
		return
	}
	cost := 1
	if c := r.URL.Query().Get("cost"); c != "" {
		n, err := strconv.Atoi(c)
		if err != nil || n < 1 {
			http.Error(w, `{"error":"invalid cost"}`, http.StatusBadRequest)
			return
		}
		cost = n
	}

	start := time.Now()
	res, err := s.lim.Allow(r.Context(), key, cost)
	if err != nil {
		s.metrics.Error()
		if !s.FailOpen {
			http.Error(w, `{"error":"rate limiter unavailable"}`, http.StatusServiceUnavailable)
			return
		}
		res = limiter.Result{Allowed: true, Backend: "fail-open", Degraded: true}
	}
	s.metrics.Decision(res.Allowed, res.Backend, time.Since(start))

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-RateLimit-Remaining", strconv.FormatInt(res.Remaining, 10))
	w.Header().Set("X-RateLimit-Backend", res.Backend)
	status := http.StatusOK
	if !res.Allowed {
		status = http.StatusTooManyRequests
		w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(res.RetryAfter.Seconds()))))
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(checkResponse{
		Key: key, Allowed: res.Allowed, Remaining: res.Remaining,
		RetryAfter: res.RetryAfter.Milliseconds(), Backend: res.Backend, Degraded: res.Degraded,
	})
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	body := map[string]any{"status": "ok"}
	if s.states != nil {
		body["backends"] = s.states.States()
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(body)
}

func (s *Server) metricsHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	var st map[string]string
	if s.states != nil {
		st = s.states.States()
	}
	s.metrics.WritePrometheus(w, st)
}
