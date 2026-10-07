package limiter

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *fakeClock { return &fakeClock{t: time.Unix(1_700_000_000, 0)} }
func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func TestMemoryBurstThenRefill(t *testing.T) {
	clk := newClock()
	m := NewMemory(Config{Rate: 10, Burst: 5}, clk.Now)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if r, _ := m.Allow(ctx, "k", 1); !r.Allowed {
			t.Fatalf("request %d should be allowed within burst", i)
		}
	}
	r, _ := m.Allow(ctx, "k", 1)
	if r.Allowed {
		t.Fatal("6th request should be denied")
	}
	if r.RetryAfter <= 0 || r.RetryAfter > 150*time.Millisecond {
		t.Fatalf("unexpected RetryAfter %v", r.RetryAfter)
	}

	clk.Advance(200 * time.Millisecond) // +2 tokens
	for i := 0; i < 2; i++ {
		if r, _ := m.Allow(ctx, "k", 1); !r.Allowed {
			t.Fatalf("refilled request %d should be allowed", i)
		}
	}
	if r, _ := m.Allow(ctx, "k", 1); r.Allowed {
		t.Fatal("bucket should be empty again")
	}
}

func TestMemoryKeysAreIndependent(t *testing.T) {
	m := NewMemory(Config{Rate: 1, Burst: 1}, newClock().Now)
	ctx := context.Background()
	if r, _ := m.Allow(ctx, "a", 1); !r.Allowed {
		t.Fatal("a first")
	}
	if r, _ := m.Allow(ctx, "b", 1); !r.Allowed {
		t.Fatal("b should not be affected by a")
	}
}

func TestMemorySweepRemovesIdleBuckets(t *testing.T) {
	clk := newClock()
	m := NewMemory(Config{Rate: 10, Burst: 10}, clk.Now)
	_, _ = m.Allow(context.Background(), "k", 1)
	clk.Advance(5 * time.Second)
	if n := m.Sweep(); n != 1 {
		t.Fatalf("expected 1 bucket swept, got %d", n)
	}
}

func TestMemoryConcurrentNeverExceedsBurst(t *testing.T) {
	m := NewMemory(Config{Rate: 0.0001, Burst: 100}, newClock().Now)
	var allowed atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				if r, _ := m.Allow(context.Background(), "hot", 1); r.Allowed {
					allowed.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if allowed.Load() != 100 {
		t.Fatalf("expected exactly 100 allowed, got %d", allowed.Load())
	}
}

func newRedisLimiter(t *testing.T, name string, mr *miniredis.Miniredis, clk *fakeClock, cfg Config) *Redis {
	t.Helper()
	c := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = c.Close() })
	return NewRedis(name, c, cfg, clk.Now)
}

func TestRedisBurstThenRefill(t *testing.T) {
	mr := miniredis.RunT(t)
	clk := newClock()
	r := newRedisLimiter(t, "redis-test", mr, clk, Config{Rate: 10, Burst: 5})
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		res, err := r.Allow(ctx, "k", 1)
		if err != nil || !res.Allowed {
			t.Fatalf("request %d: allowed=%v err=%v", i, res.Allowed, err)
		}
	}
	res, err := r.Allow(ctx, "k", 1)
	if err != nil || res.Allowed {
		t.Fatalf("6th request should be denied, got %+v err=%v", res, err)
	}
	if res.RetryAfter <= 0 {
		t.Fatalf("expected positive RetryAfter, got %v", res.RetryAfter)
	}
	clk.Advance(300 * time.Millisecond)
	for i := 0; i < 3; i++ {
		if res, _ := r.Allow(ctx, "k", 1); !res.Allowed {
			t.Fatalf("refilled request %d should be allowed", i)
		}
	}
	if res, _ := r.Allow(ctx, "k", 1); res.Allowed {
		t.Fatal("should be empty after consuming refill")
	}
}

// Two API nodes sharing one Redis must share one bucket.
func TestRedisStateSharedAcrossNodes(t *testing.T) {
	mr := miniredis.RunT(t)
	clk := newClock()
	cfg := Config{Rate: 0.001, Burst: 10}
	a := newRedisLimiter(t, "a", mr, clk, cfg)
	b := newRedisLimiter(t, "b", mr, clk, cfg)

	allowed := 0
	for i := 0; i < 20; i++ {
		n := a
		if i%2 == 1 {
			n = b
		}
		if res, err := n.Allow(context.Background(), "shared", 1); err == nil && res.Allowed {
			allowed++
		}
	}
	if allowed != 10 {
		t.Fatalf("two nodes should share one bucket of 10, allowed=%d", allowed)
	}
}

func TestBreakerLifecycle(t *testing.T) {
	clk := newClock()
	b := NewBreaker(2, time.Second, clk.Now)
	if !b.Allow() {
		t.Fatal("closed breaker allows")
	}
	b.Failure()
	if b.State() != "closed" {
		t.Fatal("one failure must not open")
	}
	b.Failure()
	if b.State() != "open" || b.Allow() {
		t.Fatal("two failures must open and reject")
	}
	clk.Advance(1100 * time.Millisecond)
	if !b.Allow() || b.State() != "half-open" {
		t.Fatal("after cooldown a probe is allowed")
	}
	if b.Allow() {
		t.Fatal("only one probe at a time")
	}
	b.Failure()
	if b.State() != "open" {
		t.Fatal("failed probe re-opens")
	}
	clk.Advance(1100 * time.Millisecond)
	_ = b.Allow()
	b.Success()
	if b.State() != "closed" || !b.Allow() {
		t.Fatal("successful probe closes")
	}
}

type errLimiter struct{ calls atomic.Int64 }

func (e *errLimiter) Allow(context.Context, string, int) (Result, error) {
	e.calls.Add(1)
	return Result{}, errors.New("boom")
}

func TestFailoverToSecondaryThenMemory(t *testing.T) {
	primary, secondary := miniredis.RunT(t), miniredis.RunT(t)
	clk := newClock()
	cfg := Config{Rate: 1000, Burst: 1000}
	mk := func(name string, mr *miniredis.Miniredis) *Node {
		return &Node{Name: name, Limiter: newRedisLimiter(t, name, mr, clk, cfg), Breaker: NewBreaker(2, time.Second, clk.Now)}
	}
	var failures, fallbacks atomic.Int64
	fo := NewFailover([]*Node{mk("primary", primary), mk("secondary", secondary)},
		NewMemory(cfg, clk.Now), 200*time.Millisecond)
	fo.OnEvent = func(kind, _ string) {
		if kind == "failure" {
			failures.Add(1)
		} else {
			fallbacks.Add(1)
		}
	}
	ctx := context.Background()

	res, err := fo.Allow(ctx, "k", 1)
	if err != nil || res.Backend != "primary" || res.Degraded {
		t.Fatalf("healthy path should use primary, got %+v err=%v", res, err)
	}

	primary.Close() // regional node failure #1
	res, err = fo.Allow(ctx, "k", 1)
	if err != nil || res.Backend != "secondary" || !res.Degraded {
		t.Fatalf("expected failover to secondary, got %+v err=%v", res, err)
	}
	if failures.Load() == 0 {
		t.Fatal("expected a recorded failure event")
	}

	secondary.Close() // regional node failure #2
	res, err = fo.Allow(ctx, "k", 1)
	if err != nil || res.Backend != "memory" || !res.Degraded || !res.Allowed {
		t.Fatalf("expected local fallback, got %+v err=%v", res, err)
	}
	if fallbacks.Load() == 0 {
		t.Fatal("expected fallback event")
	}
	if fo.States()["primary"] == "closed" && fo.States()["secondary"] == "closed" {
		// breakers need 2 failures; drive one more call each
		_, _ = fo.Allow(ctx, "k", 1)
	}
}

func TestFailoverSkipsOpenBreakerWithoutCallingBackend(t *testing.T) {
	clk := newClock()
	bad := &errLimiter{}
	fo := NewFailover([]*Node{{Name: "bad", Limiter: bad, Breaker: NewBreaker(1, time.Minute, clk.Now)}},
		NewMemory(Config{Rate: 10, Burst: 10}, clk.Now), time.Second)

	for i := 0; i < 10; i++ {
		if _, err := fo.Allow(context.Background(), "k", 1); err != nil {
			t.Fatal(err)
		}
	}
	if bad.calls.Load() != 1 {
		t.Fatalf("open breaker must short-circuit; backend called %d times", bad.calls.Load())
	}
}

func TestFailoverRecoversWhenPrimaryReturns(t *testing.T) {
	mr := miniredis.RunT(t)
	clk := newClock()
	cfg := Config{Rate: 1000, Burst: 1000}
	node := &Node{Name: "primary", Limiter: newRedisLimiter(t, "primary", mr, clk, cfg), Breaker: NewBreaker(1, time.Second, clk.Now)}
	fo := NewFailover([]*Node{node}, NewMemory(cfg, clk.Now), 200*time.Millisecond)
	ctx := context.Background()

	addr := mr.Addr()
	mr.Close()
	if res, _ := fo.Allow(ctx, "k", 1); res.Backend != "memory" {
		t.Fatalf("expected memory fallback, got %s", res.Backend)
	}
	if err := mr.StartAddr(addr); err != nil {
		t.Fatal(err)
	}
	clk.Advance(2 * time.Second) // cooldown elapsed -> half-open probe
	res, err := fo.Allow(ctx, "k", 1)
	if err != nil || res.Backend != "primary" || res.Degraded {
		t.Fatalf("expected recovery to primary, got %+v err=%v", res, err)
	}
}

func TestFailoverWithoutFallbackReturnsError(t *testing.T) {
	clk := newClock()
	fo := NewFailover([]*Node{{Name: "bad", Limiter: &errLimiter{}, Breaker: NewBreaker(1, time.Second, clk.Now)}}, nil, time.Second)
	if _, err := fo.Allow(context.Background(), "k", 1); err == nil {
		t.Fatal("expected error when every backend is down and no fallback exists")
	}
}

func BenchmarkMemoryAllow(b *testing.B) {
	m := NewMemory(Config{Rate: 1e9, Burst: 1 << 30}, nil)
	ctx := context.Background()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			i++
			_, _ = m.Allow(ctx, "key-"+string(rune('a'+i%26)), 1)
		}
	})
}
