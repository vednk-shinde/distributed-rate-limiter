package limiter

import (
	"sync"
	"time"
)

type breakerState int

const (
	closed breakerState = iota
	open
	halfOpen
)

// Breaker is a small circuit breaker. After Threshold consecutive failures it
// opens and rejects calls for Cooldown, then lets a single probe through
// (half-open). A successful probe closes it again.
type Breaker struct {
	Threshold int
	Cooldown  time.Duration
	now       Clock

	mu       sync.Mutex
	state    breakerState
	failures int
	openedAt time.Time
	probing  bool
}

// NewBreaker creates a breaker. now may be nil.
func NewBreaker(threshold int, cooldown time.Duration, now Clock) *Breaker {
	if now == nil {
		now = time.Now
	}
	return &Breaker{Threshold: threshold, Cooldown: cooldown, now: now}
}

// Allow reports whether a call may be attempted.
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case closed:
		return true
	case open:
		if b.now().Sub(b.openedAt) >= b.Cooldown {
			b.state = halfOpen
			b.probing = true
			return true
		}
		return false
	default: // halfOpen: only one probe in flight
		if b.probing {
			return false
		}
		b.probing = true
		return true
	}
}

// Success records a successful call.
func (b *Breaker) Success() {
	b.mu.Lock()
	b.state, b.failures, b.probing = closed, 0, false
	b.mu.Unlock()
}

// Failure records a failed call.
func (b *Breaker) Failure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.probing = false
	b.failures++
	if b.state == halfOpen || b.failures >= b.Threshold {
		b.state = open
		b.openedAt = b.now()
	}
}

// State returns "closed", "open" or "half-open".
func (b *Breaker) State() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case open:
		return "open"
	case halfOpen:
		return "half-open"
	}
	return "closed"
}
