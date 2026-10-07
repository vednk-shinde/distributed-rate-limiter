package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/vednk-shinde/distributed-rate-limiter/internal/limiter"
	"github.com/vednk-shinde/distributed-rate-limiter/internal/metrics"
)

func newTestServer(burst int) (*httptest.Server, *metrics.Registry) {
	reg := metrics.New()
	lim := limiter.NewMemory(limiter.Config{Rate: 0.001, Burst: burst}, nil)
	return httptest.NewServer(New(lim, reg, nil).Handler()), reg
}

func TestCheckAllowsThen429(t *testing.T) {
	ts, reg := newTestServer(2)
	defer ts.Close()

	for i, want := range []int{200, 200, 429} {
		resp, err := http.Get(ts.URL + "/v1/check?key=u1")
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != want {
			t.Fatalf("request %d: got %d want %d", i, resp.StatusCode, want)
		}
		if want == 429 && resp.Header.Get("Retry-After") == "" {
			t.Fatal("429 must carry Retry-After")
		}
		var body checkResponse
		_ = json.NewDecoder(resp.Body).Decode(&body)
		resp.Body.Close()
		if body.Allowed != (want == 200) {
			t.Fatalf("body.allowed mismatch on request %d", i)
		}
	}
	a, d, _, _ := reg.Snapshot()
	if a != 2 || d != 1 {
		t.Fatalf("metrics allowed=%d denied=%d", a, d)
	}
}

func TestCheckValidation(t *testing.T) {
	ts, _ := newTestServer(5)
	defer ts.Close()
	for _, p := range []string{"/v1/check", "/v1/check?key=a&cost=0", "/v1/check?key=a&cost=x"} {
		resp, _ := http.Get(ts.URL + p)
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("%s: got %d want 400", p, resp.StatusCode)
		}
		resp.Body.Close()
	}
}

type failing struct{}

func (failing) Allow(context.Context, string, int) (limiter.Result, error) {
	return limiter.Result{}, errors.New("down")
}

func TestFailOpenAndFailClosed(t *testing.T) {
	s := New(failing{}, metrics.New(), nil)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	resp, _ := http.Get(ts.URL + "/v1/check?key=a")
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("fail-open should return 200, got %d", resp.StatusCode)
	}
	s.FailOpen = false
	resp, _ = http.Get(ts.URL + "/v1/check?key=a")
	resp.Body.Close()
	if resp.StatusCode != 503 {
		t.Fatalf("fail-closed should return 503, got %d", resp.StatusCode)
	}
}

func TestHealthAndMetrics(t *testing.T) {
	ts, _ := newTestServer(5)
	defer ts.Close()
	r0, _ := http.Get(ts.URL + "/v1/check?key=a")
	r0.Body.Close()

	resp, _ := http.Get(ts.URL + "/healthz")
	if resp.StatusCode != 200 {
		t.Fatalf("healthz: %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp, _ = http.Get(ts.URL + "/metrics")
	buf := new(strings.Builder)
	b := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(b)
		buf.Write(b[:n])
		if err != nil {
			break
		}
	}
	resp.Body.Close()
	for _, want := range []string{`ratelimiter_decisions_total{result="allowed"} 1`, "ratelimiter_decision_latency_seconds_count 1"} {
		if !strings.Contains(buf.String(), want) {
			t.Fatalf("metrics missing %q in:\n%s", want, buf.String())
		}
	}
}
