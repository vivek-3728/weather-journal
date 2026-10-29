"""Tests for explicit schema versioning, atomic migration, and retry.

These tests exercise real rollback and retry behaviour rather than merely
checking that initialization succeeds.  No live weather API or external
network access is used; databases are created in temporary paths.

The supported schema has three versions so the tests can exercise multiple
historical schema versions (a requirement of issue #30):

* version 1 - the bootstrap schema (``locations`` and ``entries`` tables).
* version 2 - the data migration that links legacy rows to locations.
* version 3 - immutable request names and labels on individual observations.
"""

import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from weather_journal.store import (
    BOM_VERSION,
    LATEST_SCHEMA_VERSION,
    SCHEMA_VERSIONS,
    _get_schema_version,
    get_connection,
    get_history,
    save_entry,
)


def _create_bootstrap_tables(conn):
    """Create the version-1 tables on an open connection (no version bump)."""
    for statement, params in SCHEMA_VERSIONS[0][1]:
        if statement.startswith("ALTER TABLE"):
            # Only meaningful when the CREATE left the column out.
            continue
        conn.execute(statement, params or ())


def _write_entries(db_path, rows):
    """Create a version-1 database containing the given weather rows.

    ``rows`` is a list of ``(city, temp, humidity, condition, logged_at,
    location_id)`` tuples.  The resulting file carries ``user_version = 1``
    and both bootstrap tables, ready for the version-2 data migration.
    """
    conn = sqlite3.connect(str(db_path))
    try:
        _create_bootstrap_tables(conn)
        for row in rows:
            conn.execute(
                "INSERT INTO entries (city, temp, humidity, condition, logged_at, location_id)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                row,
            )
        conn.execute("PRAGMA user_version = 1")
        conn.commit()
    finally:
        conn.close()
    return db_path


def _write_broken_version1(db_path):
    """Create a version-1 database whose ``locations`` table is unusable.

    The ``locations`` table here is missing ``provider_id``, so the version-2
    data migration's ``INSERT`` (which writes ``provider_id``) fails.  This
    lets the atomicity tests verify that a failed migration rolls back both
    the schema change and the version bump.
    """
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            "CREATE TABLE locations ("
            "    id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "    provider TEXT NOT NULL,"
            "    name TEXT NOT NULL,"
            "    requested_name TEXT,"
            "    country TEXT,"
            "    latitude REAL,"
            "    longitude REAL,"
            "    location_label TEXT NOT NULL,"
            "    UNIQUE(provider)"
            ")"
        )
        _create_bootstrap_tables(conn)
        conn.execute(
            "INSERT INTO entries (city, temp, humidity, condition, logged_at, location_id)"
            " VALUES ('Mumbai', 30.0, 80, 'haze', '2026-10-01 12:00:00', NULL)"
        )
        conn.execute("PRAGMA user_version = 1")
        conn.commit()
    finally:
        conn.close()
    return db_path


class TestMigrationRoundTrip(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "older.db"

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_legacy_database_upgrades_to_latest_version(self):
        _write_entries(self.db_path, [
            ("Mumbai", 30.0, 80, "haze", "2026-10-01 12:00:00", None),
        ])
        conn = get_connection(str(self.db_path))
        self.assertEqual(_get_schema_version(conn), LATEST_SCHEMA_VERSION)
        conn.close()

        records = get_history("Mumbai", db_path=self.db_path)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["location_label"], "Mumbai [legacy]")

    def test_existing_weather_records_and_timestamps_survive_unchanged(self):
        _write_entries(self.db_path, [
            ("Mumbai", 30.0, 80, "haze", "2026-10-01 12:00:00", None),
        ])
        conn = get_connection(str(self.db_path))
        conn.close()
        conn = sqlite3.connect(str(self.db_path))
        try:
            row = conn.execute(
                "SELECT city, temp, humidity, condition, logged_at FROM entries"
            ).fetchone()
            self.assertEqual(
                row, ("Mumbai", 30.0, 80, "haze", "2026-10-01 12:00:00")
            )
        finally:
            conn.close()

    def test_multiple_legacy_cities_get_legacy_locations(self):
        _write_entries(self.db_path, [
            ("Mumbai", 30.0, 80, "haze", "2026-10-01 12:00:00", None),
            ("Delhi", 35.0, 70, "sun", "2026-10-02 12:00:00", None),
        ])
        conn = get_connection(str(self.db_path))
        conn.close()
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        try:
            cities = {
                r["city"]: r["location_label"]
                for r in conn.execute(
                    "SELECT e.city AS city, l.location_label AS location_label"
                    " FROM entries AS e"
                    " JOIN locations AS l ON l.id = e.location_id"
                )
            }
            self.assertEqual(
                cities,
                {"Mumbai": "Mumbai [legacy]", "Delhi": "Delhi [legacy]"},
            )
        finally:
            conn.close()


class TestSchemaVersioning(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "schema.db"

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_new_database_initialises_with_expected_schema_and_version(self):
        conn = get_connection(str(self.db_path))
        self.assertEqual(_get_schema_version(conn), LATEST_SCHEMA_VERSION)
        tables = {
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        self.assertIn("entries", tables)
        self.assertIn("locations", tables)
        conn.close()

    def test_completed_migrations_are_not_reapplied(self):
        get_connection(str(self.db_path)).close()
        probe = sqlite3.connect(str(self.db_path))
        try:
            first_version = _get_schema_version(probe)
        finally:
            probe.close()

        conn = get_connection(str(self.db_path))
        self.assertEqual(_get_schema_version(conn), first_version)
        conn.close()

    def test_user_version_is_zero_for_fresh_file_before_first_connection(self):
        conn = sqlite3.connect(str(self.db_path))
        try:
            self.assertEqual(_get_schema_version(conn), 0)
        finally:
            conn.close()

    def test_schema_versions_are_ordered_and_non_empty(self):
        self.assertIsInstance(SCHEMA_VERSIONS, list)
        self.assertGreaterEqual(len(SCHEMA_VERSIONS), 2)
        for _description, statements in SCHEMA_VERSIONS:
            self.assertTrue(statements)
            for statement, params in statements:
                self.assertIsInstance(statement, str)
                self.assertIsInstance(params, (tuple, list))

    def test_every_migration_statement_executes(self):
        temp_dir = tempfile.TemporaryDirectory()
        try:
            db_path = Path(temp_dir.name) / "statements.db"
            # Applying migrations through the real code path exercises every
            # statement with the idempotency guards in place.
            conn = get_connection(str(db_path))
            conn.close()
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            tables = {
                r[0]
                for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            self.assertIn("entries", tables)
            self.assertIn("locations", tables)
            conn.close()

            # Every migration statement must be a single SQL statement (no
            # accidental multi-statement strings from concatenation bugs).
            for _description, statements in SCHEMA_VERSIONS:
                for statement, params in statements:
                    self.assertNotIn(";\n", statement)
                    self.assertIsInstance(params, (tuple, list))
        finally:
            temp_dir.cleanup()



class TestAtomicityAndRetry(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "atomic.db"

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_failed_migration_rolls_back_schema_version_and_data(self):
        _write_broken_version1(self.db_path)

        with self.assertRaises(sqlite3.OperationalError):
            get_connection(str(self.db_path))

        conn = sqlite3.connect(str(self.db_path))
        try:
            # Version bump rolled back: still version 1, not partially bumped.
            self.assertEqual(_get_schema_version(conn), 1)
            # The legacy location insert rolled back too: no locations row was
            # committed before the failure.
            locations = conn.execute("SELECT COUNT(*) FROM locations").fetchone()[0]
            self.assertEqual(locations, 0)
        finally:
            conn.close()

    def test_retry_after_failure_succeeds(self):
        _write_broken_version1(self.db_path)

        with self.assertRaises(sqlite3.OperationalError):
            get_connection(str(self.db_path))

        # Repair the broken table so the migration can now complete.
        conn = sqlite3.connect(str(self.db_path))
        try:
            conn.execute("ALTER TABLE locations ADD COLUMN provider_id TEXT")
            conn.commit()
        finally:
            conn.close()

        # Retry now succeeds and advances to the latest version.
        conn = get_connection(str(self.db_path))
        self.assertEqual(_get_schema_version(conn), LATEST_SCHEMA_VERSION)
        conn.close()

    def test_successful_migration_commits_schema_and_version_together(self):
        _write_broken_version1(self.db_path)

        # Fix the schema first so the migration succeeds on the first try.
        conn = sqlite3.connect(str(self.db_path))
        try:
            conn.execute("ALTER TABLE locations ADD COLUMN provider_id TEXT")
            conn.commit()
        finally:
            conn.close()

        conn = get_connection(str(self.db_path))
        self.assertEqual(_get_schema_version(conn), LATEST_SCHEMA_VERSION)
        # The legacy entry now points at a committed legacy location.
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT e.city AS city, l.provider AS provider, l.location_label AS label"
            " FROM entries AS e JOIN locations AS l ON l.id = e.location_id"
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["provider"], "legacy")
        self.assertEqual(row["label"], "Mumbai [legacy]")
        conn.close()


class TestUnsupportedVersionHandling(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "future.db"

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_newer_schema_version_fails_without_modifying_data(self):
        conn = sqlite3.connect(str(self.db_path))
        try:
            conn.execute("PRAGMA user_version = 9999")
            conn.execute("CREATE TABLE locations (id INTEGER PRIMARY KEY AUTOINCREMENT)")
            conn.commit()
        finally:
            conn.close()

        with self.assertRaises(ValueError) as ctx:
            get_connection(str(self.db_path))

        self.assertIn("newer", str(ctx.exception))
        conn = sqlite3.connect(str(self.db_path))
        try:
            self.assertIn(
                "locations",
                {r[0] for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )},
            )
        finally:
            conn.close()


class TestIdempotentInitialization(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "idem.db"

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_get_connection_twice_is_safe(self):
        conn = get_connection(str(self.db_path))
        conn.close()
        gc = get_connection(str(self.db_path))
        gc.close()

        verify = sqlite3.connect(str(self.db_path))
        try:
            self.assertEqual(_get_schema_version(verify), LATEST_SCHEMA_VERSION)
        finally:
            verify.close()


class TestConcurrentMigration(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.temp_dir.cleanup()

    def _run_concurrent_saves(self, db_path, cities):
        script = (
            "import sys\n"
            "from weather_journal.store import save_entry\n"
            "save_entry(sys.argv[2], 25.0, 60, 'clear sky', db_path=sys.argv[1])\n"
        )
        processes = [
            subprocess.Popen(
                [sys.executable, "-c", script, str(db_path), city],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            for city in cities
        ]

        results = []
        for process in processes:
            results.append(process.communicate(timeout=10))

        for process, (_stdout, stderr) in zip(processes, results):
            self.assertEqual(process.returncode, 0, stderr)

    def test_concurrent_commands_preserve_entries_during_migration(self):
        db_path = Path(self.temp_dir.name) / "existing.db"
        save_entry("Mumbai", 30.0, 80, "haze", db_path=db_path)
        conn = sqlite3.connect(str(db_path))
        try:
            conn.execute("PRAGMA user_version = 0")
            conn.commit()
        finally:
            conn.close()

        self._run_concurrent_saves(db_path, ("Pune", "Delhi"))

        conn = sqlite3.connect(str(db_path))
        try:
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM entries").fetchone()[0],
                3,
            )
            self.assertEqual(_get_schema_version(conn), LATEST_SCHEMA_VERSION)
        finally:
            conn.close()

    def test_concurrent_commands_can_initialize_a_new_database(self):
        db_path = Path(self.temp_dir.name) / "new.db"

        self._run_concurrent_saves(db_path, ("Pune", "Delhi"))

        conn = sqlite3.connect(str(db_path))
        try:
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM entries").fetchone()[0],
                2,
            )
            self.assertEqual(_get_schema_version(conn), LATEST_SCHEMA_VERSION)
        finally:
            conn.close()


class TestLegacyCompatibilityPreserved(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "legacy.db"

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_old_database_rows_are_preserved_as_legacy_location(self):
        # A pre-versioning database: entries exists without location_id and
        # user_version is 0 (treated as fresh, so every migration runs).
        conn = sqlite3.connect(str(self.db_path))
        try:
            conn.execute(
                "CREATE TABLE entries ("
                "    id INTEGER PRIMARY KEY AUTOINCREMENT,"
                "    city TEXT NOT NULL,"
                "    temp REAL NOT NULL,"
                "    humidity INTEGER,"
                "    condition TEXT,"
                "    logged_at TEXT NOT NULL"
                ")"
            )
            conn.execute(
                "INSERT INTO entries (city, temp, humidity, condition, logged_at)"
                " VALUES ('Mumbai', 30, 80, 'haze', '2026-10-01 12:00:00')"
            )
            conn.commit()
        finally:
            conn.close()

        records = get_history("Mumbai", db_path=self.db_path)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["location_label"], "Mumbai [legacy]")


if __name__ == "__main__":
    unittest.main()
