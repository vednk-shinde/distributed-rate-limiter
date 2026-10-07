package limiter

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// tokenBucketScript runs atomically inside Redis so that concurrent API nodes
// sharing a key can never double-spend tokens.
//
//	KEYS[1] bucket key
//	ARGV    rate (tokens/s), burst, now (ms), cost
//
// Returns {allowed, remainingTokens, retryAfterMs}.
var tokenBucketScript = redis.NewScript(`
local rate  = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local now   = tonumber(ARGV[3])
local cost  = tonumber(ARGV[4])

local data   = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts     = tonumber(data[2])
if tokens == nil then
  tokens = burst
  ts = now
end

if now < ts then now = ts end -- tolerate clock skew between API nodes
tokens = math.min(burst, tokens + (now - ts) * rate / 1000)

local allowed = 0
local retry = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
else
  retry = math.ceil((cost - tokens) * 1000 / rate)
end

redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', now)
redis.call('PEXPIRE', KEYS[1], math.ceil(burst / rate * 1000) + 1000)
return {allowed, math.floor(tokens), retry}
`)

// Redis is a distributed token-bucket limiter backed by a single Redis node.
type Redis struct {
	client redis.UniversalClient
	cfg    Config
	now    Clock
	name   string
	prefix string
}

// NewRedis creates a Redis-backed limiter. now may be nil.
func NewRedis(name string, client redis.UniversalClient, cfg Config, now Clock) *Redis {
	if now == nil {
		now = time.Now
	}
	return &Redis{client: client, cfg: cfg, now: now, name: name, prefix: "rl:"}
}

// Allow implements Limiter.
func (r *Redis) Allow(ctx context.Context, key string, cost int) (Result, error) {
	vals, err := tokenBucketScript.Run(ctx, r.client, []string{r.prefix + key},
		r.cfg.Rate, r.cfg.Burst, r.now().UnixMilli(), cost).Int64Slice()
	if err != nil {
		return Result{}, fmt.Errorf("redis %s: %w", r.name, err)
	}
	if len(vals) != 3 {
		return Result{}, fmt.Errorf("redis %s: unexpected script reply %v", r.name, vals)
	}
	return Result{
		Allowed:    vals[0] == 1,
		Remaining:  vals[1],
		RetryAfter: time.Duration(vals[2]) * time.Millisecond,
		Backend:    r.name,
	}, nil
}

// Ping checks connectivity.
func (r *Redis) Ping(ctx context.Context) error { return r.client.Ping(ctx).Err() }
