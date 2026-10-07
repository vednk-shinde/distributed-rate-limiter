# Distributed Rate Limiter (Go + Redis)

A fault-tolerant, distributed token-bucket rate limiter. API nodes share limiter state in Redis
(atomically, via a Lua script), and keep working when Redis nodes fail: requests fail over down a
priority list of Redis nodes guarded by circuit breakers, and finally to a local in-memory limiter.

```
client ─► nginx ─► limiter-1 ─┐                 ┌─► redis-a  (priority 1)
                   limiter-2 ─┴─ Failover chain ┼─► redis-b  (priority 2)
                                  (breakers)    └─► in-memory token bucket (last resort)
```

## Why it is built this way

| Concern | Design |
|---|---|
| Correctness across nodes | Token bucket implemented as a **single Lua script** → refill + consume is atomic, no double-spend between API nodes. |
| No single point of failure | Ordered list of Redis nodes; each has a **circuit breaker** (closed → open → half-open probe) so a dead node costs one timeout, not one per request. |
| Survive total Redis loss | Falls back to a **sharded in-memory limiter** with `rate/instances` so the fleet-wide limit stays approximately right. |
| Policy when nothing works | `-fail-open` (default) allows traffic; `-fail-open=false` returns 503. |
| Observability | `/metrics` (Prometheus text): decisions, latency histogram, backend failures, fallbacks, breaker state. `/healthz` shows breaker states. |
| Idle memory | Redis keys expire (`PEXPIRE`); the memory limiter sweeps idle buckets. |

## API

`GET /v1/check?key=<id>&cost=<n>` → `200` allowed / `429` limited (with `Retry-After`), body:

```json
{"key":"user-1","allowed":true,"remaining":199,"retry_after_ms":0,"backend":"redis-primary","degraded":false}
```

## Run it

```bash
# no Docker needed: in-memory only
go run ./cmd/server -rate 100 -burst 200

# full stack: 2 Redis nodes + 2 limiters + nginx
docker compose up -d --build
curl "localhost:8080/v1/check?key=alice"

# load test (Go tool, or Locust)
go run ./cmd/loadtest -url http://localhost:8080 -c 64 -d 15s
pip install locust && locust -f loadtest/locustfile.py --host http://localhost:8080 --headless -u 500 -r 100 -t 60s

# failure drill: kills Redis nodes under load and reports availability
./scripts/chaos.sh
```

Config (flags or env): `-redis/REDIS_ADDRS`, `-rate/RATE`, `-burst/BURST`, `-instances/INSTANCES`,
`-redis-timeout`, `-breaker-threshold`, `-breaker-cooldown`, `-fail-open`.

## Tests

`go test ./...` — unit tests use [miniredis](https://github.com/alicebob/miniredis) (real Lua execution) and
include failure injection: primary dies → secondary serves; both die → memory serves; Redis returns →
breaker half-opens and traffic returns to the primary; open breakers short-circuit without calling the backend;
concurrent requests never exceed the burst; two nodes share one bucket.

## Measured results (honest numbers)

Measured on an Intel i9-14900HX (Windows), Go 1.26:

* In-process decision cost (`go test -bench Memory`): **~48 ns/op** under parallel load (sharded locks).
* End-to-end HTTP and Redis throughput depend heavily on your network/host. **Run `make load` and
  `./scripts/chaos.sh` on your own stack and record the numbers here** — I deliberately don't claim figures
  such as "10,000 req/s" or "99.99% uptime" that haven't been measured on your infrastructure.

## Trade-offs / next steps

* Clients supply the timestamp to the Lua script; node clock skew is tolerated (time never goes backwards in a bucket) but
  large skew shifts refill timing. Using Redis `TIME` is an alternative.
* Fallback limits are per-instance, so during a full Redis outage the fleet-wide limit is approximate.
* Next: Redis Cluster / Sentinel client, per-route limit configs, sliding-window variant, gRPC API.

MIT licensed.
