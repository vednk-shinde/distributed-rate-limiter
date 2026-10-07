package limiter

import (
	"context"
	"errors"
	"time"
)

// Node is a named backend guarded by a circuit breaker.
type Node struct {
	Name    string
	Limiter Limiter
	Breaker *Breaker
}

// Failover tries each node in priority order, skipping nodes whose breaker is
// open, and finally falls back to a local limiter so the service keeps
// protecting itself (and keeps serving) even when every Redis node is down.
type Failover struct {
	nodes    []*Node
	fallback Limiter
	timeout  time.Duration
	// OnEvent, if set, is called with ("failure"|"fallback", nodeName).
	OnEvent func(kind, node string)
}

// NewFailover builds a failover chain. timeout bounds each backend call.
func NewFailover(nodes []*Node, fallback Limiter, timeout time.Duration) *Failover {
	return &Failover{nodes: nodes, fallback: fallback, timeout: timeout}
}

// Allow implements Limiter.
func (f *Failover) Allow(ctx context.Context, key string, cost int) (Result, error) {
	for i, n := range f.nodes {
		if !n.Breaker.Allow() {
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, f.timeout)
		res, err := n.Limiter.Allow(cctx, key, cost)
		cancel()
		if err != nil {
			n.Breaker.Failure()
			f.emit("failure", n.Name)
			if ctx.Err() != nil {
				return Result{}, ctx.Err()
			}
			continue
		}
		n.Breaker.Success()
		res.Degraded = i > 0
		return res, nil
	}

	if f.fallback == nil {
		return Result{}, errors.New("all rate-limit backends unavailable")
	}
	f.emit("fallback", "memory")
	res, err := f.fallback.Allow(ctx, key, cost)
	res.Degraded = true
	return res, err
}

// States returns the breaker state of each node, keyed by node name.
func (f *Failover) States() map[string]string {
	out := make(map[string]string, len(f.nodes))
	for _, n := range f.nodes {
		out[n.Name] = n.Breaker.State()
	}
	return out
}

func (f *Failover) emit(kind, node string) {
	if f.OnEvent != nil {
		f.OnEvent(kind, node)
	}
}
