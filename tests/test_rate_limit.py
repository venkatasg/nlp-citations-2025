"""Verify the Graph-API client never exceeds 1 request per second.

The S2 tutorial documents that an API key grants "1 request per
second rate across all endpoints." We use a single global rate
limiter (`_politeness_sleep` in `src/fetch_graph.py`) protected by
a lock; this test exercises it from multiple worker threads and
asserts the inter-fire gap is always >= the configured interval.

Run with:
    uv run -m unittest tests.test_rate_limit -v
"""

import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor


class RateLimiterTest(unittest.TestCase):
    """All assertions check the inter-call gap measured at the
    moment each thread re-emerges from `_politeness_sleep`."""

    def _measure(self, *, n_calls, n_workers, min_interval):
        """Drive `_politeness_sleep` from `n_workers` threads, with
        the module's `_MIN_INTERVAL` temporarily set to
        `min_interval`. Returns a sorted list of fire timestamps.

        We mutate the module attribute once before the workers
        start (and restore it after) rather than per-thread, because
        layering multiple `mock.patch` contexts across threads is
        not safe - one thread's restore can clobber another's set."""
        from src import fetch_graph

        original = fetch_graph._MIN_INTERVAL
        fetch_graph._MIN_INTERVAL = min_interval
        with fetch_graph._RATE_LOCK:
            fetch_graph._LAST_CALL = 0.0

        fires = []
        fires_lock = threading.Lock()

        def _one(_):
            fetch_graph._politeness_sleep()
            t = time.monotonic()
            with fires_lock:
                fires.append(t)

        try:
            with ThreadPoolExecutor(max_workers=n_workers) as pool:
                list(pool.map(_one, range(n_calls)))
        finally:
            fetch_graph._MIN_INTERVAL = original
        return sorted(fires)

    def test_serial_calls_respect_interval(self):
        """Single-threaded baseline."""
        interval = 0.05
        ts = self._measure(n_calls=10, n_workers=1, min_interval=interval)
        gaps = [ts[i + 1] - ts[i] for i in range(len(ts) - 1)]
        self.assertGreaterEqual(min(gaps), interval - 1e-3,
            f"min gap {min(gaps):.4f}s < interval {interval}s; gaps={gaps}")

    def test_concurrent_workers_respect_interval(self):
        """Without the lock, multiple workers race past the wait
        check and fire near-simultaneously. With the lock, all
        gaps remain >= interval."""
        interval = 0.05
        n_calls = 30
        ts = self._measure(n_calls=n_calls, n_workers=8,
                           min_interval=interval)
        gaps = [ts[i + 1] - ts[i] for i in range(len(ts) - 1)]
        self.assertGreaterEqual(min(gaps), interval - 1e-3,
            f"min gap {min(gaps):.4f}s < interval {interval}s "
            f"under concurrency; gaps={gaps}")

    def test_observed_rate_below_cap(self):
        """End-to-end (n / wall_time) must stay at-or-below the cap."""
        interval = 0.05
        n_calls = 30
        ts = self._measure(n_calls=n_calls, n_workers=8,
                           min_interval=interval)
        wall = ts[-1] - ts[0]
        observed_rate = (n_calls - 1) / wall
        cap = 1.0 / interval
        # Tight: 5 % tolerance for clock jitter on small intervals.
        self.assertLessEqual(observed_rate, cap * 1.05,
            f"observed {observed_rate:.2f} req/s > cap {cap:.2f} req/s "
            f"({n_calls} calls in {wall:.3f}s)")

    def test_documented_one_rps(self):
        """Round-trip at the documented 1 RPS limit. 3 calls across
        4 workers must take >= 2 seconds of wall clock (2 gaps of
        1 s each)."""
        interval = 1.0
        n_calls = 3
        ts = self._measure(n_calls=n_calls, n_workers=4,
                           min_interval=interval)
        wall = ts[-1] - ts[0]
        min_required = (n_calls - 1) * interval - 0.05
        self.assertGreaterEqual(wall, min_required,
            f"{n_calls} calls at 1 RPS finished in {wall:.3f}s "
            f"(< {min_required}s) - rate limiter not enforcing")

    def test_default_min_interval_is_one_second_with_key(self):
        """Sanity: with an API key, the default _MIN_INTERVAL is 1 s
        (the S2-documented quota). This catches a config drift that
        would silently uncap requests."""
        from src.config import GraphAPIConfig
        cfg = GraphAPIConfig()
        self.assertEqual(cfg.rps_with_key, 1.0,
            "config.GraphAPIConfig.rps_with_key must stay at 1.0 to "
            "match the documented S2 quota")
        self.assertEqual(1.0 / cfg.rps_with_key, 1.0)


if __name__ == "__main__":
    unittest.main()
