package limiter

import (
	"context"
	"hash/fnv"
	"math"
	"sync"
	"time"
)

const shardCount = 64

type bucket struct {
	tokens float64
	last   time.Time
}

type shard struct {
	mu      sync.Mutex
	buckets map[string]*bucket
}

// Memory is a sharded, in-process token-bucket limiter. It is used as the last
// line of defence when every distributed backend is unavailable.
type Memory struct {
	cfg    Config
	now    Clock
	shards [shardCount]shard
	name   string
}

// NewMemory creates an in-memory limiter. now may be nil (time.Now is used).
func NewMemory(cfg Config, now Clock) *Memory {
	if now == nil {
		now = time.Now
	}
	m := &Memory{cfg: cfg, now: now, name: "memory"}
	for i := range m.shards {
		m.shards[i].buckets = make(map[string]*bucket)
	}
	return m
}

func (m *Memory) shardFor(key string) *shard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return &m.shards[h.Sum32()%shardCount]
}

// Allow implements Limiter.
func (m *Memory) Allow(_ context.Context, key string, cost int) (Result, error) {
	now := m.now()
	s := m.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	b, ok := s.buckets[key]
	if !ok {
		b = &bucket{tokens: float64(m.cfg.Burst), last: now}
		s.buckets[key] = b
	}
	if elapsed := now.Sub(b.last).Seconds(); elapsed > 0 {
		b.tokens = math.Min(float64(m.cfg.Burst), b.tokens+elapsed*m.cfg.Rate)
		b.last = now
	}

	res := Result{Backend: m.name}
	if b.tokens >= float64(cost) {
		b.tokens -= float64(cost)
		res.Allowed = true
	} else if m.cfg.Rate > 0 {
		res.RetryAfter = time.Duration((float64(cost) - b.tokens) / m.cfg.Rate * float64(time.Second))
	}
	res.Remaining = int64(b.tokens)
	return res, nil
}

// Sweep removes buckets that have been idle long enough to be full again.
func (m *Memory) Sweep() int {
	now := m.now()
	idle := time.Duration(float64(m.cfg.Burst) / m.cfg.Rate * float64(time.Second))
	removed := 0
	for i := range m.shards {
		s := &m.shards[i]
		s.mu.Lock()
		for k, b := range s.buckets {
			if now.Sub(b.last) > idle {
				delete(s.buckets, k)
				removed++
			}
		}
		s.mu.Unlock()
	}
	return removed
}

// RunJanitor periodically sweeps idle buckets until ctx is cancelled.
func (m *Memory) RunJanitor(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.Sweep()
		}
	}
}
