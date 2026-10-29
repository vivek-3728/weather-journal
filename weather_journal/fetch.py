"""Weather data fetching module via OpenWeatherMap API."""

import json
import os
import secrets
import sqlite3
import threading
import time
from contextlib import contextmanager
from pathlib import Path

import requests

API_BASE_URL = "https://api.openweathermap.org/data/2.5/weather"
CACHE_TTL_SECONDS = 600
DEFAULT_CACHE_PATH = Path(__file__).resolve().parent.parent / ".cache" / "weather.db"

#: Maximum time (seconds) a caller waits for another request to finish refreshing
#: the same cache key before starting a refresh itself.  Bounds the worst-case
#: refresh wait to two lease windows instead of waiting forever behind a stuck
#: owner.
COORDINATION_TIMEOUT_SECONDS = 20.0

# Keep a small process-local first-level cache as an optimization. SQLite below
# is the source of truth that makes the cache survive separate CLI runs.
_cache = {}

# Process-local per-key locks.  The outer dict lock keeps construction atomic;
# every key gets its own threading.Lock so different cities never serialize
# behind one another.  Entries are never removed (keys are bounded by the
# number of distinct city + cache-path pairs a process touches).
_locks_guard = threading.Lock()
_key_locks = {}


def _cache_path(cache_path=None):
    configured_path = cache_path or os.environ.get("WEATHER_JOURNAL_CACHE")
    return Path(configured_path) if configured_path else DEFAULT_CACHE_PATH


def _memory_key(city_key, cache_path):
    """Build an in-memory cache key scoped to the resolved SQLite cache path.

    The process-local cache is shared across every configured cache path, so it
    must never be keyed by city alone or one cache configuration would serve
    another configuration's entry.
    """
    return str(_cache_path(cache_path)), city_key


def _key_lock(city_key, cache_path):
    """Return the per-key thread lock for this (city, cache path) pair."""
    key = _memory_key(city_key, cache_path)
    with _locks_guard:
        lock = _key_locks.get(key)
        if lock is None:
            lock = threading.Lock()
            _key_locks[key] = lock
        return lock


def _connect_cache(cache_path):
    path = _cache_path(cache_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=5)
    conn.execute("PRAGMA busy_timeout = 5000")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS weather_cache (
            city_key TEXT PRIMARY KEY,
            fetched_at REAL NOT NULL,
            observed_at REAL NOT NULL,
            payload TEXT NOT NULL
        )
    """)
    # Databases created before issue #31 lack ``observed_at``; add it lazily so
    # existing cache rows are kept.  (CREATE TABLE IF NOT EXISTS never alters
    # an existing table.)
    _ensure_observed_at_column(conn)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cache_refresh_leases (
            city_key TEXT PRIMARY KEY,
            owner_token TEXT NOT NULL,
            expires_at REAL NOT NULL
        )
    """)
    conn.commit()
    return conn


def _ensure_observed_at_column(conn):
    """Add ``observed_at`` to databases created before issue #31 (idempotent)."""
    columns = {
        row[1]
        for row in conn.execute("PRAGMA table_info(weather_cache)").fetchall()
    }
    if "observed_at" not in columns:
        conn.execute("ALTER TABLE weather_cache ADD COLUMN observed_at REAL")
        conn.commit()


def _now():
    """Single injected clock so tests can reason about time deterministically."""
    return time.time()


def _read_cache(city_key, cache_path, now):
    """Return a fresh cached result (memory first, then SQLite), else None."""
    memory_key = _memory_key(city_key, cache_path)
    memory_entry = _cache.get(memory_key)
    if memory_entry is not None:
        # Only trust a complete in-memory entry.  Entries written before issue
        # #31 were 2-tuples without an ordering stamp; they are refreshed from
        # SQLite below so the stamp can be backfilled from the stored row.
        if len(memory_entry) > 2 and now - memory_entry[0] < CACHE_TTL_SECONDS:
            return memory_entry[1]

    conn = _connect_cache(cache_path)
    try:
        row = conn.execute(
            "SELECT fetched_at, observed_at, payload FROM weather_cache WHERE city_key = ?",
            (city_key,),
        ).fetchone()
        if row is None:
            return None

        fetched_at, observed_at, payload = row
        # Rows written before issue #31 have NULL observed_at; treat them as
        # "no ordering known" (they always lose ordering races, never win).
        if observed_at is None:
            observed_at = fetched_at
        if now - fetched_at >= CACHE_TTL_SECONDS or fetched_at > now:
            conn.execute("DELETE FROM weather_cache WHERE city_key = ?", (city_key,))
            conn.commit()
            return None

        try:
            data = json.loads(payload)
        except (TypeError, ValueError):
            conn.execute("DELETE FROM weather_cache WHERE city_key = ?", (city_key,))
            conn.commit()
            return None

        # Backfill the ordering stamp for rows written before issue #31 so the
        # stale-write guard can reason about them on the next read/write.
        # Such stamps always reflect the row's own observation moment, so the
        # guard can never decide a strictly-newer write lost a race against it.
        if row[1] is None:
            conn.execute(
                "UPDATE weather_cache SET observed_at = ? WHERE city_key = ? AND observed_at IS NULL",
                (observed_at, city_key),
            )
            conn.commit()
    finally:
        conn.close()

    _cache[memory_key] = (fetched_at, data, observed_at)
    return data


def _write_cache(city_key, data, cache_path, fetched_at, observed_at):
    """Persist a result unless an equally fresh or newer observation exists.

    ``observed_at`` orders results (the observation time reported by the
    provider when valid, otherwise the request-start time); ``fetched_at``
    remains the cache-freshness timestamp used by the TTL.  The UPSERT only
    applies when the new observation is strictly newer than the stored one, so
    a slow older response can never overwrite a newer result.  Ties keep the
    existing row: deterministic and harmless because either payload already
    carries the same observation moment.  ``observed_at IS NULL`` (pre-#31
    rows) means "no ordering known" and always loses, so the write payload is
    stored for reads while its ``fetched_at`` stamp still orders freshness.

    Return the payload that won the ordering race.  Callers must use this
    value rather than their local response because a newer observation may
    have been committed by another process while their request was in flight.
    """
    payload = json.dumps(data, separators=(",", ":"), sort_keys=True)
    conn = _connect_cache(cache_path)
    try:
        conn.execute(
            """
            INSERT INTO weather_cache (city_key, fetched_at, observed_at, payload)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(city_key) DO UPDATE SET
                fetched_at = excluded.fetched_at,
                observed_at = excluded.observed_at,
                payload = excluded.payload
            WHERE weather_cache.observed_at IS NULL
               OR excluded.observed_at > weather_cache.observed_at
            """,
            (city_key, fetched_at, observed_at, payload),
        )
        conn.commit()
    finally:
        conn.close()

    # Merge with any concurrently-persisted newer result before populating
    # memory, so a rejected stale write never replaces a newer in-memory entry
    # and memory agrees with the winning persisted row.
    now = _now()
    fetched_at, data, observed_at = _sqlite_winner(
        city_key, cache_path, (fetched_at, data, observed_at), now
    )
    if observed_at > _memory_observed_at(city_key, cache_path):
        _cache[_memory_key(city_key, cache_path)] = (fetched_at, data, observed_at)
    return data


def _memory_observed_at(city_key, cache_path):
    """Ordering stamp of the in-process entry, or ``-inf`` when absent."""
    entry = _cache.get(_memory_key(city_key, cache_path))
    return entry[2] if entry and len(entry) > 2 else float("-inf")


def _read_db_row(city_key, cache_path):
    """Return the persisted row ``(fetched_at, observed_at, payload)`` or None."""
    conn = _connect_cache(cache_path)
    try:
        row = conn.execute(
            "SELECT fetched_at, observed_at, payload FROM weather_cache WHERE city_key = ?",
            (city_key,),
        ).fetchone()
    finally:
        conn.close()
    return row


def _sqlite_winner(city_key, cache_path, incoming, now):
    """Merge ``incoming`` with any concurrently-written newer result.

    If another process has already persisted an equally fresh or newer
    observation, its (newer) payload wins: a caller whose stale write lost the
    race must not populate memory with its own older result.  ``-inf`` for a
    legacy row means "no ordering known" and always loses.  Expired rows are
    ignored (the incoming result is strictly newer than any expired observation).
    """
    row = _read_db_row(city_key, cache_path)
    if row is None:
        return incoming
    fetched_at, observed_at, payload = row
    if observed_at is None:
        observed_at = fetched_at
    if now - fetched_at >= CACHE_TTL_SECONDS or fetched_at > now:
        return incoming
    if observed_at >= incoming[2]:
        try:
            return (fetched_at, json.loads(payload), observed_at)
        except (TypeError, ValueError):
            return incoming
    return incoming


@contextmanager
def _refresh_lease(city_key, cache_path, timeout):
    """Hold the per-key cross-process refresh lease for this city/path pair.

    Ownership is granted by a conditional ``UPDATE`` (safe against simultaneous
    processes), or by reclaiming a lease whose owner crashed and let it expire.
    The write transaction is committed immediately and never held open across
    the network request.  ``expires_at`` doubles as the ordering sequence: it is
    always newer than every previously granted lease, so a replacement owner's
    result carries a strictly newer ``observed_at`` and cannot be overwritten by
    the original owner's stale response.
    """
    token = secrets.token_hex(8)
    deadline = time.monotonic() + timeout
    acquired = False
    conn = _connect_cache(cache_path)
    try:
        while True:
            now = _now()
            expires_at = now + timeout
            # Fast path: insert a fresh lease if none exists.  The partial
            # unique index (created in _connect_cache) guarantees a single
            # row per city_key, so this cannot double-insert under concurrency.
            try:
                conn.execute(
                    "INSERT INTO cache_refresh_leases (city_key, owner_token, expires_at) VALUES (?, ?, ?)",
                    (city_key, token, expires_at),
                )
            except sqlite3.IntegrityError:
                pass
            conn.commit()

            # Take over a lease that has expired (crashed owner) or that we
            # already own (retry after a transient failure).  The WHERE clause
            # is what makes this atomic against simultaneous waiters.
            # ``conn.total_changes`` counts rows touched by this connection's
            # statements; capturing it before the UPDATE tells us whether the
            # grant matched a row (i.e. we now own the lease).
            granted_before = conn.total_changes
            conn.execute(
                """
                UPDATE cache_refresh_leases
                SET owner_token = ?, expires_at = ?
                WHERE city_key = ?
                  AND (owner_token = ? OR expires_at <= ?)
                """,
                (token, expires_at, city_key, token, now),
            )
            if conn.total_changes > granted_before:
                conn.commit()
                acquired = True
                break
            conn.rollback()
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Timed out after {timeout:g}s waiting for another request "
                    f"to refresh '{city_key}'"
                )
            time.sleep(0.05)
        yield
    finally:
        try:
            if acquired:
                conn.execute(
                    "DELETE FROM cache_refresh_leases WHERE city_key = ? AND owner_token = ?",
                    (city_key, token),
                )
                conn.commit()
        finally:
            conn.close()


def _observation_order(data, requested_name, request_started_at):
    """Ordering timestamp for a freshly fetched observation.

    Prefers the provider's observation time (``dt``) when it is present and
    sane; otherwise falls back to the time the request started so that slow
    responses cannot win ordering against requests that started later.
    """
    raw = data.get("dt")
    if isinstance(raw, (int, float)) and 0 < raw <= request_started_at + 60:
        return float(raw)
    return request_started_at


def get_current(city, api_key=None, cache_path=None, coord_timeout=None):
    """Fetch current weather, reusing results cached for ten minutes.

    Concurrent callers for the same city and cache path are coordinated with a
    per-key in-process lock plus a cross-process SQLite lease: only one caller
    refreshes while the others wait and then reuse the fresh cache entry.  The
    lease expires if the refreshing process crashes, so a city is never blocked
    indefinitely.
    """
    city_key = city.strip().casefold()
    now = _now()
    cached_data = _read_cache(city_key, cache_path, now)
    if cached_data is not None:
        return cached_data

    key = api_key or os.environ.get("OWM_API_KEY")
    if not key:
        raise ValueError("Missing API key. Please set OWM_API_KEY environment variable or pass --key.")

    timeout = COORDINATION_TIMEOUT_SECONDS if coord_timeout is None else coord_timeout
    with _key_lock(city_key, cache_path):
        # Another thread in this process may have refreshed while we waited.
        cached_data = _read_cache(city_key, cache_path, _now())
        if cached_data is not None:
            return cached_data

        try:
            with _refresh_lease(city_key, cache_path, timeout):
                # Another process may have refreshed while we waited for the lease.
                cached_data = _read_cache(city_key, cache_path, _now())
                if cached_data is not None:
                    return cached_data
                return _fetch_and_store(city, city_key, cache_path, key)
        except TimeoutError:
            # We could not win the lease (the owner was still running when our
            # deadline passed).  The owner may have refreshed the cache in the
            # meantime, so re-check before failing: a waiting caller should
            # benefit from the refresh it waited on.  If the owner failed or
            # never refreshed, the deadline bounds the wait and we surface the
            # error instead of blocking indefinitely.
            cached_data = _read_cache(city_key, cache_path, _now())
            if cached_data is not None:
                return cached_data
            raise


def _fetch_and_store(city, city_key, cache_path, key):
    """Call the API (with retries) and store a successful result.

    Request-start time is the fallback ordering stamp; the provider's
    observation time (``dt``) wins when present and valid.  Failures raise
    before any cache write, so invalid or incomplete responses never replace a
    valid cached observation and the refresh lease is released by the caller.
    """
    params = {
        "q": city,
        "appid": key,
        "units": "metric"
    }

    request_started_at = _now()
    for attempt in range(3):
        try:
            resp = requests.get(API_BASE_URL, params=params, timeout=10)
            break
        except requests.RequestException as e:
            if attempt == 2:
                raise ValueError(f"Network error: {e}")
            time.sleep(2 ** attempt)

    if resp.status_code == 401:
        raise ValueError("Invalid API key.")
    elif resp.status_code == 404:
        raise ValueError(f"City '{city}' not found.")
    elif resp.status_code != 200:
        try:
            err_data = resp.json()
            err_msg = err_data.get("message", "Unknown error")
        except Exception:
            err_msg = resp.text or "Unknown error"
        raise ValueError(f"API Error: {err_msg}")

    try:
        data = resp.json()
    except Exception as e:
        raise ValueError(f"Invalid JSON in API response: {e}")

    # A malformed success payload must raise before any cache write so it can
    # never replace a valid cached observation (issue #31 stale-write guard).
    try:
        result = {
            "city": data["name"],
            "temp": round(data["main"]["temp"], 1),
            "humidity": data["main"]["humidity"],
            "condition": data["weather"][0]["description"],
            "location": {
                "provider": "openweathermap",
                "provider_id": data.get("id"),
                "requested_name": city,
                "country": data.get("sys", {}).get("country"),
                "latitude": data.get("coord", {}).get("lat"),
                "longitude": data.get("coord", {}).get("lon"),
                "dt": data.get("dt"),
            },
        }
    except (KeyError, IndexError, TypeError, ValueError) as exc:
        raise ValueError(f"Malformed API response payload: {exc}")

    observed_at = _observation_order(data, city, request_started_at)
    return _write_cache(city_key, result, cache_path, _now(), observed_at)
