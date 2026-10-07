"""Locust load test for the distributed rate limiter.

    pip install locust
    locust -f loadtest/locustfile.py --host http://localhost:8080 \
        --headless -u 500 -r 100 -t 60s

Both 200 (allowed) and 429 (limited) are expected, correct responses and are
therefore NOT counted as failures. Only 5xx / connection errors are.
"""
import random

from locust import FastHttpUser, constant_pacing, task


class RateLimitedClient(FastHttpUser):
    wait_time = constant_pacing(0.01)

    # A few "hot" keys exercise the limiter; many "cold" keys exercise scale.
    HOT_KEYS = [f"hot-{i}" for i in range(5)]
    COLD_KEYS = 100_000

    @task(1)
    def hot_key(self):
        self._check(random.choice(self.HOT_KEYS))

    @task(9)
    def cold_key(self):
        self._check(f"user-{random.randrange(self.COLD_KEYS)}")

    def _check(self, key: str):
        with self.client.get(f"/v1/check?key={key}", name="/v1/check", catch_response=True) as resp:
            if resp.status_code in (200, 429):
                resp.success()
            else:
                resp.failure(f"unexpected status {resp.status_code}")
