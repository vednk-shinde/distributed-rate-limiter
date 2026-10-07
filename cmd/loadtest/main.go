// Command loadtest hammers the rate limiter's /v1/check endpoint and reports
// throughput and latency percentiles. It is a dependency-free alternative to the
// Locust scenario in loadtest/locustfile.py.
package main

import (
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	var (
		target   = flag.String("url", "http://localhost:8080", "base URL of the rate limiter")
		workers  = flag.Int("c", 64, "concurrent workers")
		duration = flag.Duration("d", 10*time.Second, "test duration")
		keys     = flag.Int("keys", 1000, "number of distinct rate-limit keys")
	)
	flag.Parse()

	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns: *workers * 2, MaxIdleConnsPerHost: *workers * 2, IdleConnTimeout: 90 * time.Second,
		},
		Timeout: 5 * time.Second,
	}

	var ok, limited, failed atomic.Int64
	deadline := time.Now().Add(*duration)
	lat := make([][]time.Duration, *workers)

	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(w) + time.Now().UnixNano()))
			for time.Now().Before(deadline) {
				u := fmt.Sprintf("%s/v1/check?key=user-%d", *target, rng.Intn(*keys))
				start := time.Now()
				resp, err := client.Get(u)
				if err != nil {
					if failed.Add(1) == 1 {
						fmt.Println("first error:", err)
					}
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				lat[w] = append(lat[w], time.Since(start))
				switch resp.StatusCode {
				case http.StatusOK:
					ok.Add(1)
				case http.StatusTooManyRequests:
					limited.Add(1)
				default:
					failed.Add(1)
				}
			}
		}(w)
	}
	wg.Wait()

	var all []time.Duration
	for _, l := range lat {
		all = append(all, l...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	total := len(all)
	if total == 0 {
		fmt.Println("no successful requests; is the server running?")
		return
	}
	pct := func(p float64) time.Duration { return all[int(float64(total-1)*p)] }

	fmt.Printf("duration:      %s\n", *duration)
	fmt.Printf("workers:       %d\n", *workers)
	fmt.Printf("requests:      %d (%.0f req/s)\n", total, float64(total)/duration.Seconds())
	fmt.Printf("allowed (200): %d\n", ok.Load())
	fmt.Printf("limited (429): %d\n", limited.Load())
	fmt.Printf("errors:        %d\n", failed.Load())
	fmt.Printf("latency p50:   %s\n", pct(0.50))
	fmt.Printf("latency p95:   %s\n", pct(0.95))
	fmt.Printf("latency p99:   %s\n", pct(0.99))
	fmt.Printf("latency max:   %s\n", all[total-1])
}
