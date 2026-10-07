// Package limiter implements token-bucket rate limiting with pluggable,
// failure-tolerant backends (Redis, in-process memory) and a failover chain.
package limiter

import (
	"context"
	"time"
)

// Config describes a token bucket: Rate tokens are added per second up to Burst.
type Config struct {
	Rate  float64
	Burst int
}

// Result is the outcome of a single rate-limit decision.
type Result struct {
	Allowed    bool
	Remaining  int64
	RetryAfter time.Duration
	// Backend is the name of the backend that made the decision.
	Backend string
	// Degraded is true when the decision came from a fallback rather than the
	// primary backend.
	Degraded bool
}

// Limiter decides whether a request identified by key may proceed.
type Limiter interface {
	Allow(ctx context.Context, key string, cost int) (Result, error)
}

// Clock allows tests to control time.
type Clock func() time.Time
