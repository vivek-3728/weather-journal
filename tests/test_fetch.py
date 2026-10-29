import unittest
from unittest.mock import patch, MagicMock
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from unittest import mock
import requests
from weather_journal.fetch import get_current, _cache


class TestFetch(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.cache_path = Path(self.temp_dir.name) / "weather-cache.db"
        _cache.clear()

    def tearDown(self):
        self.temp_dir.cleanup()

    @patch("weather_journal.fetch.requests.get")
    def test_get_current_success(self, mock_get):
        # Mock a successful API response
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "id": 2643743,
            "name": "London",
            "coord": {"lat": 51.5074, "lon": -0.1278},
            "sys": {"country": "GB"},
            "main": {"temp": 15.6, "humidity": 80},
            "weather": [{"description": "light rain"}]
        }
        mock_get.return_value = mock_resp

        result = get_current("London", api_key="fake-key", cache_path=self.cache_path)

        self.assertEqual(result["city"], "London")
        self.assertEqual(result["temp"], 15.6)
        self.assertEqual(result["humidity"], 80)
        self.assertEqual(result["condition"], "light rain")
        self.assertEqual(result["location"]["provider_id"], 2643743)
        self.assertEqual(result["location"]["requested_name"], "London")
        mock_get.assert_called_once()

    @patch("weather_journal.fetch.requests.get")
    def test_caching(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "id": 2988507,
            "name": "Paris",
            "coord": {"lat": 48.8534, "lon": 2.3488},
            "sys": {"country": "FR"},
            "main": {"temp": 20.0, "humidity": 50},
            "weather": [{"description": "clear sky"}]
        }
        mock_get.return_value = mock_resp

        # First call hits the API
        res1 = get_current("Paris", api_key="fake-key", cache_path=self.cache_path)
        self.assertEqual(mock_get.call_count, 1)

        # Second call within 10 minutes should hit the cache
        res2 = get_current("Paris", api_key="fake-key", cache_path=self.cache_path)
        self.assertEqual(mock_get.call_count, 1)  # Still 1
        self.assertEqual(res1, res2)

    @patch("weather_journal.fetch.time.sleep")
    @patch("weather_journal.fetch.requests.get")
    def test_network_retry_and_failure(self, mock_get, mock_sleep):
        # Make the request raise a ConnectionError on all attempts
        mock_get.side_effect = requests.ConnectionError("Connection timeout")

        with self.assertRaises(ValueError) as context:
            get_current("Berlin", api_key="fake-key", cache_path=self.cache_path)

        self.assertIn("Network error: Connection timeout", str(context.exception))
        # It should have retried 3 times (initial + 2 retries)
        self.assertEqual(mock_get.call_count, 3)
        # It should have slept twice
        self.assertEqual(mock_sleep.call_count, 2)
        mock_sleep.assert_any_call(1) # 2**0
        mock_sleep.assert_any_call(2) # 2**1

    def test_cache_survives_separate_python_processes(self):
        env = os.environ.copy()
        env["WEATHER_JOURNAL_CACHE"] = str(self.cache_path)
        repo_root = Path(__file__).resolve().parent.parent
        populate = """
from unittest.mock import MagicMock, patch
from weather_journal.fetch import get_current
response = MagicMock(status_code=200)
response.json.return_value = {
    "name": "Mumbai",
    "main": {"temp": 29.0, "humidity": 75},
    "weather": [{"description": "haze"}],
}
with patch("weather_journal.fetch.requests.get", return_value=response):
    get_current("Mumbai", api_key="fake-key")
"""
        consume = """
from unittest.mock import patch
from weather_journal.fetch import get_current
with patch("weather_journal.fetch.requests.get", side_effect=AssertionError("API called")):
    print(get_current("Mumbai")["city"])
"""

        first = subprocess.run(
            [sys.executable, "-c", populate], cwd=repo_root, env=env,
            text=True, capture_output=True,
        )
        self.assertEqual(first.returncode, 0, first.stderr)
        second = subprocess.run(
            [sys.executable, "-c", consume], cwd=repo_root, env=env,
            text=True, capture_output=True,
        )

        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertEqual(second.stdout.strip(), "Mumbai")

    @patch("weather_journal.fetch.requests.get")
    def test_expired_persistent_entry_is_refetched(self, mock_get):
        stale_response = MagicMock(status_code=200)
        stale_response.json.return_value = {
            "name": "Paris",
            "main": {"temp": 10.0, "humidity": 50},
            "weather": [{"description": "cloudy"}],
        }
        fresh_response = MagicMock(status_code=200)
        fresh_response.json.return_value = {
            "name": "Paris",
            "main": {"temp": 20.0, "humidity": 45},
            "weather": [{"description": "clear sky"}],
        }
        mock_get.side_effect = [stale_response, fresh_response]

        get_current("Paris", api_key="fake-key", cache_path=self.cache_path)
        _cache.clear()
        conn = sqlite3.connect(str(self.cache_path))
        try:
            conn.execute("UPDATE weather_cache SET fetched_at = 0")
            conn.commit()
        finally:
            conn.close()

        result = get_current("Paris", api_key="fake-key", cache_path=self.cache_path)

        self.assertEqual(result["temp"], 20.0)
        self.assertEqual(mock_get.call_count, 2)

    @patch("weather_journal.fetch.requests.get")
    def test_units_metric_requested(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "name": "Tokyo",
            "main": {"temp": 22.4, "humidity": 65},
            "weather": [{"description": "clear sky"}]
        }
        mock_get.return_value = mock_resp

        get_current("Tokyo", api_key="test-key", cache_path=self.cache_path)

        # Verify units=metric is explicitly requested for Celsius temperatures
        mock_get.assert_called_once()
        _, kwargs = mock_get.call_args
        self.assertEqual(kwargs["params"].get("units"), "metric")
        self.assertEqual(kwargs["params"].get("q"), "Tokyo")
        self.assertEqual(kwargs["params"].get("appid"), "test-key")

    @patch("weather_journal.fetch.requests.get")
    def test_success_response_without_location_id_does_not_crash(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "name": "Tokyo",
            "main": {"temp": 22.4, "humidity": 65},
            "weather": [{"description": "clear sky"}],
        }
        mock_get.return_value = mock_resp

        result = get_current("Tokyo", api_key="test-key", cache_path=self.cache_path)

        self.assertIsNone(result["location"]["provider_id"])

    @patch("weather_journal.fetch.requests.get")
    def test_invalid_api_key_401(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 401
        mock_resp.json.return_value = {"cod": 401, "message": "Invalid API key."}
        mock_get.return_value = mock_resp

        with self.assertRaises(ValueError) as ctx:
            get_current("Mumbai", api_key="bad-key", cache_path=self.cache_path)

        self.assertEqual(str(ctx.exception), "Invalid API key.")

    @patch("weather_journal.fetch.requests.get")
    def test_city_not_found_404(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_resp.json.return_value = {"cod": "404", "message": "city not found"}
        mock_get.return_value = mock_resp

        with self.assertRaises(ValueError) as ctx:
            get_current("NonExistentCity123", api_key="valid-key", cache_path=self.cache_path)

        self.assertEqual(str(ctx.exception), "City 'NonExistentCity123' not found.")

    @patch.dict("os.environ", {}, clear=True)
    def test_missing_api_key(self):
        with self.assertRaises(ValueError) as ctx:
            get_current("Mumbai", cache_path=self.cache_path)
        self.assertIn("Missing API key", str(ctx.exception))

    @patch("weather_journal.fetch.requests.get")
    def test_non_200_api_error(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_resp.json.return_value = {"message": "Internal Server Error"}
        mock_get.return_value = mock_resp

        with self.assertRaises(ValueError) as ctx:
            get_current("Berlin", api_key="test-key", cache_path=self.cache_path)

        self.assertIn("API Error: Internal Server Error", str(ctx.exception))

    @patch("weather_journal.cli.get_current")
    def test_cmd_log_error_output(self, mock_get_current):
        from io import StringIO
        from types import SimpleNamespace
        from contextlib import redirect_stderr
        from weather_journal.cli import cmd_log

        mock_get_current.side_effect = ValueError("City 'FakeCity' not found.")

        out = StringIO()
        with redirect_stderr(out):
            cmd_log(SimpleNamespace(city="FakeCity", key="fake-key", db="test.db"))

        self.assertEqual(out.getvalue().strip(), "Error fetching weather: City 'FakeCity' not found.")

    @staticmethod
    def _weather_response(temp, name="London"):
        response = MagicMock(status_code=200)
        response.json.return_value = {
            "id": 2643743,
            "name": name,
            "coord": {"lat": 51.5074, "lon": -0.1278},
            "sys": {"country": "GB"},
            "main": {"temp": temp, "humidity": 70},
            "weather": [{"description": "clear sky"}],
        }
        return response

    @patch("weather_journal.fetch.requests.get")
    def test_same_city_isolated_across_cache_paths(self, mock_get):
        # Two distinct SQLite cache paths must not share in-memory entries.
        other_dir = tempfile.TemporaryDirectory()
        self.addCleanup(other_dir.cleanup)
        other_path = Path(other_dir.name) / "other-weather.db"

        mock_get.side_effect = [self._weather_response(11.1), self._weather_response(22.2)]

        first = get_current("London", api_key="fake-key", cache_path=self.cache_path)
        second = get_current("London", api_key="fake-key", cache_path=other_path)

        self.assertEqual(mock_get.call_count, 2)
        self.assertEqual(first["temp"], 11.1)
        self.assertEqual(second["temp"], 22.2)
        self.assertTrue(other_path.exists())

        # Each cache path keeps its own persisted row for the same city key.
        for path, expected in ((self.cache_path, 11.1), (other_path, 22.2)):
            conn = sqlite3.connect(str(path))
            try:
                row = conn.execute(
                    "SELECT payload FROM weather_cache WHERE city_key = ?",
                    ("london",),
                ).fetchone()
            finally:
                conn.close()
            self.assertIsNotNone(row)
            self.assertIn(f'"temp":{expected}', row[0])

    @patch("weather_journal.fetch.requests.get")
    def test_repeated_requests_reuse_memory_cache_per_path(self, mock_get):
        from weather_journal import fetch

        mock_get.return_value = self._weather_response(18.5)

        results = [get_current("London", api_key="fake-key", cache_path=self.cache_path)
                   for _ in range(3)]

        self.assertEqual(mock_get.call_count, 1)
        self.assertTrue(all(r == results[0] for r in results))

        # The entry is served from the process-local cache first, before SQLite.
        key = fetch._memory_key("london", self.cache_path)
        self.assertIn(key, _cache)
        # Scoped keys: a different path must not collide with this entry.
        self.assertNotIn(fetch._memory_key("london", Path(self.temp_dir.name) / "x.db"), _cache)

    @patch("weather_journal.fetch.requests.get")
    def test_expired_memory_entry_is_refreshed(self, mock_get):
        from weather_journal import fetch

        mock_get.side_effect = [self._weather_response(5.0), self._weather_response(6.0)]

        get_current("London", api_key="fake-key", cache_path=self.cache_path)
        key = fetch._memory_key("london", self.cache_path)
        fetched_at, data, _observed_at = _cache[key]
        # Force both the memory entry and the persistent row past the TTL so the
        # refresh must go all the way back to the API.
        _cache[key] = (fetched_at - fetch.CACHE_TTL_SECONDS - 1, data)
        conn = sqlite3.connect(str(self.cache_path))
        try:
            conn.execute("UPDATE weather_cache SET fetched_at = 0")
            conn.commit()
        finally:
            conn.close()

        result = get_current("London", api_key="fake-key", cache_path=self.cache_path)

        self.assertEqual(mock_get.call_count, 2)
        self.assertEqual(result["temp"], 6.0)
        self.assertEqual(_cache[key][1]["temp"], 6.0)

    @patch.dict("os.environ", {}, clear=True)
    @patch("weather_journal.fetch.requests.get")
    def test_env_configured_cache_path_is_isolated_in_memory(self, mock_get):
        from weather_journal import fetch

        env_dir = tempfile.TemporaryDirectory()
        self.addCleanup(env_dir.cleanup)
        env_path = Path(env_dir.name) / "env-weather.db"

        with patch.dict("os.environ", {"WEATHER_JOURNAL_CACHE": str(env_path)}):
            mock_get.side_effect = [self._weather_response(30.0), self._weather_response(31.0)]
            get_current("London", api_key="fake-key")
            # Same city, explicit distinct path: must not reuse the env entry.
            direct = get_current("London", api_key="fake-key", cache_path=self.cache_path)

        self.assertEqual(mock_get.call_count, 2)
        self.assertEqual(direct["temp"], 31.0)
        self.assertIn(fetch._memory_key("london", env_path), _cache)
        self.assertIn(fetch._memory_key("london", self.cache_path), _cache)

    @patch("weather_journal.fetch.requests.get")
    def test_persistent_corrupt_payload_is_dropped_and_refetched(self, mock_get):
        mock_get.return_value = self._weather_response(12.0)
        get_current("London", api_key="fake-key", cache_path=self.cache_path)
        _cache.clear()

        conn = sqlite3.connect(str(self.cache_path))
        try:
            conn.execute("UPDATE weather_cache SET payload = ?", ("not-json",))
            conn.commit()
        finally:
            conn.close()

        mock_get.return_value = self._weather_response(13.0)
        result = get_current("London", api_key="fake-key", cache_path=self.cache_path)

        self.assertEqual(result["temp"], 13.0)
        self.assertEqual(mock_get.call_count, 2)

        conn = sqlite3.connect(str(self.cache_path))
        try:
            row = conn.execute(
                "SELECT payload FROM weather_cache WHERE city_key = ?", ("london",)
            ).fetchone()
        finally:
            conn.close()
        self.assertIn('"temp":13.0', row[0])

    @patch("weather_journal.fetch.requests.get")
    def test_persistent_future_timestamp_is_refetched(self, mock_get):
        mock_get.side_effect = [self._weather_response(14.0), self._weather_response(15.0)]
        get_current("London", api_key="fake-key", cache_path=self.cache_path)
        _cache.clear()

        conn = sqlite3.connect(str(self.cache_path))
        try:
            conn.execute("UPDATE weather_cache SET fetched_at = ?", (time.time() + 10_000,))
            conn.commit()
        finally:
            conn.close()

        result = get_current("London", api_key="fake-key", cache_path=self.cache_path)

        self.assertEqual(result["temp"], 15.0)
        self.assertEqual(mock_get.call_count, 2)


class _CoordTestBase(unittest.TestCase):
    """Shared fixtures for the concurrency/stale-write regression tests."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.cache_path = Path(self.temp_dir.name) / "weather-cache.db"
        _cache.clear()
        self._create_schema(self.cache_path)

    def tearDown(self):
        # Best-effort: remove WAL/SHM sidecars so Windows can delete the dir.
        for suffix in ("-wal", "-shm"):
            sidecar = Path(str(self.cache_path) + suffix)
            if sidecar.exists():
                try:
                    sidecar.unlink()
                except OSError:
                    pass
        _cache.clear()

    @staticmethod
    def _run_silently(suite):
        """Run a suite with all runner output captured (keeps host consoles clean)."""
        buffer = StringIO()
        with redirect_stdout(buffer):
            result = unittest.TextTestRunner(stream=buffer).run(suite)
        return result

    @staticmethod
    def _create_schema(cache_path):
        """Create the cache schema without touching the network (test fixture)."""
        from weather_journal import fetch

        conn = fetch._connect_cache(cache_path)
        conn.close()

    @staticmethod
    def _wait_for(predicate, timeout=5.0):
        deadline = time.monotonic() + timeout
        while not predicate():
            if time.monotonic() >= deadline:
                return False
            time.sleep(0.01)
        return True

    def test_schema_created_on_connect(self):
        from weather_journal import fetch
        conn = fetch._connect_cache(self.cache_path)
        names = [r[0] for r in conn.execute("SELECT name FROM sqlite_master")]
        conn.close()
        self.assertIn("weather_cache", names)
        self.assertIn("cache_refresh_leases", names)

    @staticmethod
    def _weather_response(temp, dt=None, city="London", humidity=80):
        resp = MagicMock()
        resp.status_code = 200
        payload = {
            "id": 2643743,
            "name": city,
            "coord": {"lat": 51.5074, "lon": -0.1278},
            "sys": {"country": "GB"},
            "main": {"temp": temp, "humidity": humidity},
            "weather": [{"description": "clear sky"}],
        }
        if dt is not None:
            payload["dt"] = dt
        resp.json.return_value = payload
        return resp

    def _leases(self, cache_path=None):
        conn = sqlite3.connect(str(cache_path or self.cache_path), timeout=5)
        try:
            return conn.execute(
                "SELECT city_key FROM cache_refresh_leases"
            ).fetchall()
        finally:
            conn.close()

    def _stored_row(self, city_key="london"):
        conn = sqlite3.connect(str(self.cache_path), timeout=5)
        try:
            return conn.execute(
                "SELECT fetched_at, observed_at, payload FROM weather_cache WHERE city_key = ?",
                (city_key,),
            ).fetchone()
        finally:
            conn.close()


class TestThreadCoordination(_CoordTestBase):
    """Thread-level single-flight, cache recheck, failure and lock cleanup."""

    @patch("weather_journal.fetch.requests.get")
    def test_concurrent_same_city_requests_make_one_api_call(self, mock_get):
        """Simultaneous same-city misses coordinate to a single API refresh."""
        release = threading.Event()
        mock_get.side_effect = lambda *args, **kwargs: (
            release.wait(5) and self._weather_response(10.0)
        )

        results = {}

        def worker():
            try:
                results[threading.get_ident()] = get_current(
                    "London", api_key="fake-key", cache_path=self.cache_path
                )
            except BaseException as exc:  # surfaced via the assertion below
                errors.append(exc)

        errors = []
        threads = [threading.Thread(target=worker) for _ in range(3)]
        for t in threads:
            t.start()
        # Deterministic ordering: wait until the first caller owns the lease
        # (and is therefore parked inside requests.get) before releasing the
        # others to contend for it.
        self.assertTrue(
            self._wait_for(lambda: bool(self._leases())),
            "first caller never acquired the refresh lease",
        )
        release.set()
        for t in threads:
            t.join(10)

        self.assertTrue(all(not t.is_alive() for t in threads), "a worker hung")
        self.assertEqual(errors, [], f"worker raised: {errors!r}")
        self.assertEqual(mock_get.call_count, 1)
        self.assertEqual(len(results), 3)
        for result in results.values():
            self.assertEqual(result["temp"], 10.0)

    @patch("weather_journal.fetch.requests.get")
    def test_different_cities_are_not_serialized(self, mock_get):
        """A slow refresh for one city does not block another city."""
        london_release = threading.Event()
        paris_entered = threading.Event()

        def side_effect(*args, **kwargs):
            params = kwargs.get("params", {})
            if params.get("q") == "London":
                london_release.wait(5)
                return self._weather_response(10.0, city="London")
            if params.get("q") == "Paris":
                paris_entered.set()
                return self._weather_response(20.0, city="Paris")
            raise AssertionError(f"unexpected request {params!r}")

        mock_get.side_effect = side_effect

        london_result = {}
        london_errors = []

        def london_worker():
            try:
                london_result["r"] = get_current(
                    "London", api_key="fake-key", cache_path=self.cache_path
                )
            except BaseException as exc:
                london_errors.append(exc)

        london_thread = threading.Thread(target=london_worker)
        london_thread.start()

        # Wait until London's refresh is in flight and holding its lease.
        self.assertTrue(
            self._wait_for(lambda: bool(self._leases())),
            "London never acquired its refresh lease",
        )

        paris_thread = threading.Thread(
            target=lambda: get_current(
                "Paris", api_key="fake-key", cache_path=self.cache_path
            )
        )
        paris_thread.start()
        # Paris must complete (its own lease + API call) while London is stalled.
        paris_thread.join(5)
        self.assertFalse(paris_thread.is_alive(), "Paris was serialized behind London")
        self.assertTrue(paris_entered.is_set())

        london_release.set()
        london_thread.join(10)
        self.assertEqual(london_result["r"]["city"], "London")

    @patch("weather_journal.fetch.requests.get")
    def test_waiting_caller_reuses_result_populated_by_refresh_owner(self, mock_get):
        """A waiter rechecks the cache and returns the owner's result."""
        release = threading.Event()
        mock_get.side_effect = lambda *args, **kwargs: (
            release.wait(5) and self._weather_response(11.0)
        )

        first = {}
        errors = []

        def owner():
            try:
                first["r"] = get_current(
                    "London", api_key="fake-key", cache_path=self.cache_path
                )
            except BaseException as exc:
                errors.append(exc)

        owner_thread = threading.Thread(target=owner)
        owner_thread.start()
        self.assertTrue(
            self._wait_for(lambda: bool(self._leases())),
            "owner never acquired the refresh lease",
        )

        waiter = {}

        def waiter_fn():
            try:
                waiter["r"] = get_current(
                    "London", api_key="fake-key", cache_path=self.cache_path
                )
            except BaseException as exc:
                errors.append(exc)

        waiter_thread = threading.Thread(target=waiter_fn)
        waiter_thread.start()
        # Let the waiter block on the in-process key lock, then let the owner
        # finish and release the lease; the waiter must see the cached result.
        time.sleep(0.1)
        release.set()
        owner_thread.join(10)
        waiter_thread.join(10)

        self.assertEqual(errors, [], f"workers raised: {errors!r}")
        self.assertEqual(mock_get.call_count, 1)
        self.assertEqual(waiter["r"], first["r"])
        self.assertEqual(waiter["r"]["temp"], 11.0)

    @patch("weather_journal.fetch.requests.get")
    def test_owner_failure_releases_lease_and_waiter_can_retry(self, mock_get):
        """An API failure releases the lease so a waiter can acquire and retry."""
        calls = []

        def side_effect(*args, **kwargs):
            calls.append(1)
            if len(calls) <= 3:
                raise requests.ConnectionError("boom")
            return self._weather_response(12.0)

        mock_get.side_effect = side_effect
        errors = []
        with patch("weather_journal.fetch.time.sleep"):  # skip retry backoff

            def owner():
                try:
                    with self.assertRaises(ValueError):
                        get_current(
                            "London", api_key="fake-key", cache_path=self.cache_path
                        )
                except BaseException as exc:
                    errors.append(exc)

            owner_thread = threading.Thread(target=owner)
            owner_thread.start()
            owner_thread.join(10)
            self.assertFalse(owner_thread.is_alive(), "owner hung")
            self.assertEqual(errors, [], f"owner raised: {errors!r}")
            # All 3 retry attempts failed, then ValueError surfaced.
            self.assertEqual(len(calls), 3)

        # The lease must have been released on the exception path.
        self.assertEqual(self._leases(), [])

        # A subsequent (fresh) call succeeds: the lease was released and the
        # empty cache forces a real API refresh.
        result = get_current("London", api_key="fake-key", cache_path=self.cache_path)
        self.assertEqual(result["temp"], 12.0)
        self.assertEqual(len(calls), 4)
        self.assertEqual(self._leases(), [])
        result = get_current("London", api_key="fake-key", cache_path=self.cache_path)
        self.assertEqual(result["temp"], 12.0)
        self.assertEqual(len(calls), 4)
        self.assertEqual(self._leases(), [])

    def test_exception_does_not_leave_permanent_lease(self):
        """Even a non-retryable API error must clean up the lease."""
        with patch(
            "weather_journal.fetch.requests.get",
            side_effect=requests.ConnectionError("permanent"),
        ), patch("weather_journal.fetch.time.sleep"):
            with self.assertRaises(ValueError):
                get_current("London", api_key="fake-key", cache_path=self.cache_path)

        self.assertEqual(self._leases(), [])

    @patch("weather_journal.fetch.requests.get")
    def test_expired_lease_is_reclaimed(self, mock_get):
        """A lease whose owner crashed (expired) can be reclaimed."""
        from weather_journal import fetch

        conn = fetch._connect_cache(self.cache_path)  # creates the schema
        conn.execute(
            "INSERT INTO cache_refresh_leases (city_key, owner_token, expires_at) VALUES (?, ?, ?)",
            ("london", "dead-owner", time.time() - 1),
        )
        conn.commit()
        conn.close()

        mock_get.return_value = self._weather_response(13.0)
        result = get_current("London", api_key="fake-key", cache_path=self.cache_path)

        self.assertEqual(result["temp"], 13.0)
        self.assertEqual(mock_get.call_count, 1)
        self.assertEqual(self._leases(), [])

    @patch("weather_journal.fetch.requests.get")
    def test_stale_write_cannot_overwrite_newer_sqlite_result(self, mock_get):
        """A write carrying an older observation cannot replace a newer row."""
        from weather_journal import fetch

        mock_get.return_value = self._weather_response(15.0)
        get_current("London", api_key="fake-key", cache_path=self.cache_path)
        row = self._stored_row()
        self.assertIsNotNone(row)
        self.assertIsNotNone(row[1])  # observed_at backfilled/set
        newer_observed = row[1]

        # Simulate a slow original owner finally writing its older result.
        fetch._write_cache(
            "london",
            {"city": "London", "temp": 14.0, "dt": None},
            self.cache_path,
            time.time(),
            newer_observed - 100,
        )

        row_after = self._stored_row()
        self.assertEqual(json.loads(row_after[2])["temp"], 15.0)
        self.assertEqual(row_after[1], newer_observed)

    @patch("weather_journal.fetch.requests.get")
    def test_stale_fetch_returns_newer_persisted_result(self, mock_get):
        """A rejected stale response must not escape to the caller."""
        from weather_journal import fetch

        now = time.time()
        newer = {
            "city": "London",
            "temp": 21.0,
            "humidity": 60,
            "condition": "clear sky",
            "location": {
                "provider": "openweathermap",
                "provider_id": 2643743,
                "requested_name": "London",
                "country": "GB",
                "latitude": 51.5,
                "longitude": -0.12,
                "dt": now,
            },
        }
        fetch._write_cache(
            "london", newer, self.cache_path, now, now
        )

        # Simulate an original lease owner finishing late with an older API
        # observation after a replacement owner persisted ``newer``.
        mock_get.return_value = self._weather_response(10.0, dt=now - 100)
        result = fetch._fetch_and_store(
            "London", "london", self.cache_path, "fake-key"
        )

        self.assertEqual(result["temp"], 21.0)
        row = self._stored_row()
        self.assertEqual(json.loads(row[2])["temp"], 21.0)
        self.assertEqual(row[0], now)

    @patch("weather_journal.fetch.requests.get")
    def test_stale_write_does_not_replace_newer_memory_entry(self, mock_get):
        """Memory keeps the freshest observation after a rejected stale write."""
        from weather_journal import fetch

        mock_get.return_value = self._weather_response(15.0)
        get_current("London", api_key="fake-key", cache_path=self.cache_path)
        key = fetch._memory_key("london", self.cache_path)
        self.assertIn(key, _cache)
        newer_observed = _cache[key][2]

        fetch._write_cache(
            "london",
            {"city": "London", "temp": 14.0, "dt": None},
            self.cache_path,
            time.time(),
            newer_observed - 100,
        )

        entry = _cache[key]
        self.assertEqual(entry[1]["temp"], 15.0)
        self.assertEqual(entry[2], newer_observed)

    @patch("weather_journal.fetch.requests.get")
    def test_invalid_response_does_not_overwrite_valid_cache(self, mock_get):
        """A malformed API payload must not clobber a previously valid entry."""
        from weather_journal import fetch

        mock_get.return_value = self._weather_response(9.0)
        get_current("London", api_key="fake-key", cache_path=self.cache_path)
        row_before = self._stored_row()
        self.assertIsNotNone(row_before)

        # Expire the cached entry directly in SQLite so the next call misses
        # the cache and reaches the fetch path (with the bad response).
        conn = sqlite3.connect(str(self.cache_path))
        try:
            conn.execute(
                "UPDATE weather_cache SET fetched_at = ? WHERE city_key = ?",
                (time.time() - 2 * fetch.CACHE_TTL_SECONDS, "london"),
            )
            conn.commit()
        finally:
            conn.close()
        _cache.clear()

        bad = MagicMock()
        bad.status_code = 200
        bad.json.return_value = {"cod": "404", "message": "city not found"}
        mock_get.return_value = bad

        with patch("weather_journal.fetch.time.sleep"):
            with self.assertRaises(ValueError):
                get_current("London", api_key="fake-key", cache_path=self.cache_path)

        # The invalid response must not have written anything to the cache.
        row_after = self._stored_row()
        if row_after is not None:
            self.assertEqual(json.loads(row_after[2])["temp"], 9.0)
        self.assertEqual(self._leases(), [])


class TestCrossProcessCoordination(_CoordTestBase):
    """Cross-process single-flight via the SQLite lease table."""

    def test_concurrent_processes_share_one_api_refresh(self):
        """Two processes contend for the lease; exactly one calls the API."""
        from contextlib import suppress
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

        class Handler(BaseHTTPRequestHandler):
            calls = 0

            def do_GET(self):
                Handler.calls += 1
                time.sleep(1.0)  # hold the response so both processes overlap
                body = json.dumps(
                    {
                        "id": 2643743,
                        "name": "London",
                        "coord": {"lat": 51.5, "lon": -0.12},
                        "sys": {"country": "GB"},
                        "main": {"temp": 17.0, "humidity": 60},
                        "weather": [{"description": "clear sky"}],
                        "dt": 1_700_000_000,
                    }
                ).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()

        def _stop_server():
            with suppress(OSError):
                server.shutdown()
            with suppress(OSError):
                server.server_close()
            server_thread.join(5)

        self.addCleanup(_stop_server)

        repo_root = Path(__file__).resolve().parent.parent
        worker = (
            "import os, time, sys\n"
            "from unittest.mock import patch\n"
            "from weather_journal.fetch import get_current\n"
            "city = sys.argv[1]\n"
            "base = sys.argv[2]\n"
            "started = time.time()\n"
            "with patch('weather_journal.fetch.API_BASE_URL', base):\n"
            "    r = get_current(city, api_key='fake-key')\n"
            "print(f'{r[\"temp\"]}|{time.time() - started:.2f}')\n"
        )
        env = os.environ.copy()
        env["WEATHER_JOURNAL_CACHE"] = str(self.cache_path)
        base_url = (
            f"http://127.0.0.1:{server.server_address[1]}/data/2.5/weather"
        )

        procs = [
            subprocess.Popen(
                [sys.executable, "-c", worker, "London", base_url],
                cwd=repo_root, env=env,
                text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            )
            for _ in range(2)
        ]
        outputs = []
        try:
            for p in procs:
                out, err = p.communicate(timeout=60)
                self.assertEqual(p.returncode, 0, f"{out}\n{err}")
                outputs.append(out.strip())
        finally:
            for p in procs:
                if p.poll() is None:
                    p.kill()

        # Single-flight: only one process hit the API (the handler sleeps 1s,
        # so a serialized duplicate call would push the second process's
        # end-to-end time well past the 1s response plus overhead).
        self.assertEqual(Handler.calls, 1, f"API hit {Handler.calls} times: {outputs}")
        for out in outputs:
            self.assertTrue(out.startswith("17.0|"), out)
        # Both processes finished; the loser waited on the lease, not the API.
        times = [float(out.split("|", 1)[1]) for out in outputs]
        self.assertTrue(all(t < 5.0 for t in times), times)

    def test_stale_process_write_cannot_beat_newer_result(self):
        """A crashed-and-resumed process writing late cannot win the row."""
        from weather_journal import fetch

        conn = fetch._connect_cache(self.cache_path)  # creates the schema
        now = time.time()
        conn.execute(
            "INSERT OR IGNORE INTO weather_cache "
            "(city_key, fetched_at, observed_at, payload) VALUES (?, ?, ?, ?)",
            ("london", now, now, json.dumps({"city": "London", "temp": 21.0})),
        )
        conn.commit()
        conn.close()

        # Stale writer with an older observation (e.g. an owner whose lease
        # expired while its request was still in flight).
        fetch._write_cache(
            "london",
            {"city": "London", "temp": 20.0, "dt": None},
            self.cache_path,
            now,
            now - 50,
        )

        row = self._stored_row()
        self.assertEqual(json.loads(row[2])["temp"], 21.0)
        self.assertEqual(row[1], now)


class TestSchemaCompatibility(unittest.TestCase):
    """Databases created before issue #31 keep working."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.cache_path = Path(self.temp_dir.name) / "weather-cache.db"
        _cache.clear()

    def tearDown(self):
        for suffix in ("-wal", "-shm"):
            sidecar = Path(str(self.cache_path) + suffix)
            if sidecar.exists():
                try:
                    sidecar.unlink()
                except OSError:
                    pass
        _cache.clear()

    @staticmethod
    def _weather_response(temp, dt=None, city="London", humidity=80):
        resp = MagicMock()
        resp.status_code = 200
        payload = {
            "id": 2643743,
            "name": city,
            "coord": {"lat": 51.5074, "lon": -0.1278},
            "sys": {"country": "GB"},
            "main": {"temp": temp, "humidity": humidity},
            "weather": [{"description": "clear sky"}],
        }
        if dt is not None:
            payload["dt"] = dt
        resp.json.return_value = payload
        return resp

    @staticmethod
    def _stored_row(cache_path, city_key="london"):
        conn = sqlite3.connect(str(cache_path), timeout=5)
        try:
            return conn.execute(
                "SELECT fetched_at, observed_at, payload FROM weather_cache WHERE city_key = ?",
                (city_key,),
            ).fetchone()
        finally:
            conn.close()

    def _create_legacy_db(self):
        """Old schema: no observed_at column, no lease table."""
        conn = sqlite3.connect(str(self.cache_path))
        conn.execute(
            """
            CREATE TABLE weather_cache (
                city_key TEXT PRIMARY KEY,
                fetched_at REAL NOT NULL,
                payload TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "INSERT INTO weather_cache (city_key, fetched_at, payload) VALUES (?, ?, ?)",
            ("london", time.time() - 10, json.dumps({"city": "London", "temp": 11.0})),
        )
        conn.commit()
        conn.close()

    @patch("weather_journal.fetch.requests.get")
    def test_legacy_row_is_served_and_backfilled(self, mock_get):
        """A pre-#31 row is served from cache and gains observed_at lazily."""
        self._create_legacy_db()

        result = get_current("London", api_key="fake-key", cache_path=self.cache_path)

        self.assertEqual(result["temp"], 11.0)
        mock_get.assert_not_called()

        conn = sqlite3.connect(str(self.cache_path))
        try:
            row = conn.execute(
                "SELECT observed_at FROM weather_cache WHERE city_key = 'london'"
            ).fetchone()
        finally:
            conn.close()
        self.assertIsNotNone(row[0], "observed_at was not backfilled")

    @patch("weather_journal.fetch.requests.get")
    def test_legacy_expired_row_is_refreshed_without_api_error(self, mock_get):
        """An expired legacy row (NULL observed_at) is replaced by a fresh fetch."""
        conn = sqlite3.connect(str(self.cache_path))
        conn.execute(
            """
            CREATE TABLE weather_cache (
                city_key TEXT PRIMARY KEY,
                fetched_at REAL NOT NULL,
                payload TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "INSERT INTO weather_cache (city_key, fetched_at, payload) VALUES (?, ?, ?)",
            ("london", time.time() - 5000, json.dumps({"city": "London", "temp": 11.0})),
        )
        conn.commit()
        conn.close()

        mock_get.return_value = self._weather_response(18.0)
        result = get_current("London", api_key="fake-key", cache_path=self.cache_path)

        self.assertEqual(result["temp"], 18.0)
        self.assertEqual(mock_get.call_count, 1)
        row = self._stored_row(self.cache_path)
        self.assertIsNotNone(row[1])
        self.assertEqual(json.loads(row[2])["temp"], 18.0)


if __name__ == "__main__":
    unittest.main()
