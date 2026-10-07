// Command server runs the distributed rate-limiter HTTP service.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/vednk-shinde/distributed-rate-limiter/internal/limiter"
	"github.com/vednk-shinde/distributed-rate-limiter/internal/metrics"
	"github.com/vednk-shinde/distributed-rate-limiter/internal/server"
)

func main() {
	var (
		addr      = flag.String("addr", env("ADDR", ":8080"), "HTTP listen address")
		redisList = flag.String("redis", env("REDIS_ADDRS", ""), "comma-separated Redis addresses in priority order (empty = memory only)")
		rate      = flag.Float64("rate", envFloat("RATE", 100), "tokens added per second per key")
		burst     = flag.Int("burst", int(envFloat("BURST", 200)), "bucket capacity per key")
		timeout   = flag.Duration("redis-timeout", 50*time.Millisecond, "per-call Redis timeout")
		threshold = flag.Int("breaker-threshold", 3, "consecutive failures before a backend is skipped")
		cooldown  = flag.Duration("breaker-cooldown", 2*time.Second, "how long a tripped backend is skipped before probing")
		instances = flag.Int("instances", int(envFloat("INSTANCES", 1)), "API instances sharing the load; local fallback rate = rate/instances")
		failOpen  = flag.Bool("fail-open", true, "allow requests when no backend can decide")
	)
	flag.Parse()

	cfg := limiter.Config{Rate: *rate, Burst: *burst}
	localCfg := limiter.Config{Rate: *rate / float64(max(*instances, 1)), Burst: max(*burst/max(*instances, 1), 1)}
	local := limiter.NewMemory(localCfg, nil)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go local.RunJanitor(ctx, time.Minute)

	reg := metrics.New()
	var lim limiter.Limiter = local
	var states server.StateReporter

	if *redisList != "" {
		var nodes []*limiter.Node
		for i, a := range strings.Split(*redisList, ",") {
			a = strings.TrimSpace(a)
			client := redis.NewClient(&redis.Options{
				Addr: a, DialTimeout: *timeout, ReadTimeout: *timeout, WriteTimeout: *timeout,
				MaxRetries: -1, PoolSize: 64,
			})
			defer client.Close()
			name := "redis-" + a
			if i == 0 {
				name = "redis-primary"
			}
			nodes = append(nodes, &limiter.Node{
				Name:    name,
				Limiter: limiter.NewRedis(name, client, cfg, nil),
				Breaker: limiter.NewBreaker(*threshold, *cooldown, nil),
			})
		}
		fo := limiter.NewFailover(nodes, local, *timeout)
		fo.OnEvent = func(kind, _ string) { reg.BackendEvent(kind) }
		lim, states = fo, fo
	} else {
		log.Println("no -redis configured: running with in-memory limiter only")
	}

	srv := server.New(lim, reg, states)
	srv.FailOpen = *failOpen
	httpSrv := &http.Server{Addr: *addr, Handler: srv.Handler(), ReadHeaderTimeout: 5 * time.Second}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutdownCtx)
	}()

	log.Printf("rate limiter listening on %s (rate=%.0f/s burst=%d backends=%q)", *addr, *rate, *burst, *redisList)
	if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func envFloat(k string, d float64) float64 {
	if v := os.Getenv(k); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return d
}
