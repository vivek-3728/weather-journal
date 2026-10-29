"""SQLite storage module for weather history."""

import sqlite3
from datetime import datetime
from pathlib import Path

DEFAULT_DB_PATH = Path(__file__).resolve().parent.parent / "weather_journal.db"


"""SQLite storage module for weather history.

Schema versioning strategy
--------------------------
The schema version is tracked with SQLite's ``PRAGMA user_version``. Each
version is a single, self-contained migration step.  The supported versions
are recorded as a module-level list of (version, description) tuples in
``SCHEMA_VERSIONS``; a new database is initialised to the latest version
in that list, and every earlier version is upgraded by walking forward
through the list.

The application supports at most ``LATEST_SCHEMA_VERSION``.  A database that
already carries a ``user_version`` greater than that value is treated as
created by a future release and rejected with a clear error instead of being
downgraded.

Migration behaviour
-------------------
- ``get_connection()`` only performs migration work when the database's
  ``user_version`` is lower than the latest supported version.  Completed
  migrations are never reapplied during ordinary connections.
- Every migration step is wrapped in an explicit transaction.  The schema
  change and the accompanying ``PRAGMA user_version`` update are committed
  together, so a failure rolls back both and a retry can safely complete the
  step.
- Ordinary reads and writes are unaffected: schema checks and migrations run
  only at connection time, inside ``get_connection()``.
"""

import sqlite3
from datetime import datetime
from pathlib import Path

DEFAULT_DB_PATH = Path(__file__).resolve().parent.parent / "weather_journal.db"

#: Ordered list of supported schema versions.  Each entry is a
#: ``(description, [(sql, params), ...])`` tuple describing one migration
#: step.  Index 0 is version 1 (the earliest shipped schema, a.k.a. the
#: beginning-of-manifest version); index 1 is version 2 (the data migration
#: that links legacy weather rows to explicit locations); index 2 is version 3
#: (immutable per-observation location labels and aliases).
SCHEMA_VERSIONS = [
    # Version 1: the bootstrap schema.  Creates the two tables the application
    # has always shipped.  ``entries.location_id`` is created up front for new
    # databases; the guarded ALTER below only runs when a legacy table is
    # missing the column.
    (
        "locations + entries bootstrap tables",
        [
            (
                "CREATE TABLE IF NOT EXISTS locations ("
                "    id INTEGER PRIMARY KEY AUTOINCREMENT,"
                "    provider TEXT NOT NULL,"
                "    provider_id TEXT,"
                "    name TEXT NOT NULL,"
                "    requested_name TEXT,"
                "    country TEXT,"
                "    latitude REAL,"
                "    longitude REAL,"
                "    location_label TEXT NOT NULL,"
                "    UNIQUE(provider, provider_id)"
                ");",
                (),
            ),
            (
                "CREATE TABLE IF NOT EXISTS entries ("
                "    id INTEGER PRIMARY KEY AUTOINCREMENT,"
                "    city TEXT NOT NULL,"
                "    temp REAL NOT NULL,"
                "    humidity INTEGER,"
                "    condition TEXT,"
                "    logged_at TEXT NOT NULL,"
                "    location_id INTEGER REFERENCES locations(id)"
                ");",
                (),
            ),
            (
                "ALTER TABLE entries ADD COLUMN location_id INTEGER REFERENCES locations(id);",
                (),
            ),
        ],
    ),
    # Version 2: link every weather row that has no location to an explicit
    # legacy location, one per distinct city.  Both statements are set-based so
    # no per-row loop or runtime parameters are needed at migration time.
    (
        "tag legacy weather rows with explicit locations",
        [
            (
                "INSERT OR IGNORE INTO locations"
                " (provider, provider_id, name, requested_name, location_label)"
                " SELECT 'legacy', LOWER(city), city, city, city || ' [legacy]'"
                " FROM (SELECT DISTINCT city FROM entries WHERE location_id IS NULL);",
                (),
            ),
            (
                "UPDATE entries SET location_id = ("
                "    SELECT id FROM locations"
                "    WHERE provider = 'legacy' AND provider_id = LOWER(entries.city)"
                ") WHERE location_id IS NULL;",
                (),
            ),
        ],
    ),
    # Version 3: shared location rows cannot represent how each individual
    # observation was requested.  Snapshot the request name and display label
    # on every entry, backfilling existing history with the best metadata that
    # remains available on its location.
    (
        "preserve per-observation location labels",
        [
            ("ALTER TABLE entries ADD COLUMN requested_name TEXT;", ()),
            ("ALTER TABLE entries ADD COLUMN location_label TEXT;", ()),
            (
                "UPDATE entries SET"
                " requested_name = (SELECT requested_name FROM locations"
                "                   WHERE id = entries.location_id),"
                " location_label = (SELECT location_label FROM locations"
                "                   WHERE id = entries.location_id)"
                " WHERE requested_name IS NULL OR location_label IS NULL;",
                (),
            ),
        ],
    ),
]

#: Latest schema version the application can apply.
LATEST_SCHEMA_VERSION = len(SCHEMA_VERSIONS)

#: The earliest schema version this release understands and can upgrade from.
BOM_VERSION = 1


def _get_schema_version(conn):
    """Return the PRAGMA user_version of *conn* (0 for a brand-new DB)."""
    return conn.execute("PRAGMA user_version").fetchone()[0]


def _set_schema_version(conn, version):
    """Set ``PRAGMA user_version`` in the caller's transaction."""
    conn.execute(f"PRAGMA user_version = {version}")


def _table_exists(conn, table):
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def _apply_migration_step(conn, index):
    """Apply migration step *index* inside a single transaction.

    Runs the step's statements, advances ``PRAGMA user_version`` to
    ``index + 1``, and commits both together.  Any failure rolls the whole
    step back (schema change and version bump) and re-raises, so the database
    is never left partially migrated and a retry can safely resume.
    """
    _description, statements = SCHEMA_VERSIONS[index]
    target_version = index + 1

    # Reserve the write lock before inspecting the schema.  A deferred
    # transaction would let multiple processes read the same old version and
    # then deadlock while both try to upgrade to a write lock.  IMMEDIATE makes
    # later migrators wait here instead.
    conn.execute("BEGIN IMMEDIATE")
    try:
        # Another process may have completed this step while this connection
        # was waiting for the write lock.  Re-check under the lock so completed
        # migrations are not applied twice.
        if _get_schema_version(conn) >= target_version:
            conn.commit()
            return False

        for statement, params in statements:
            if statement.startswith("ALTER TABLE entries ADD COLUMN "):
                # Skip when the column already exists so migration steps stay
                # idempotent for databases whose schema is ahead of their
                # user_version (including interrupted older releases).
                if _table_exists(conn, "entries"):
                    columns = {
                        row["name"]
                        for row in conn.execute("PRAGMA table_info(entries)")
                    }
                    column = statement.split("ADD COLUMN ", 1)[1].split()[0]
                    if column in columns:
                        continue
            conn.execute(statement, params or ())
        _set_schema_version(conn, target_version)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return True


class AmbiguousLocationError(ValueError):
    """Raised when a name refers to more than one stored location."""

    def __init__(self, selector, locations):
        self.selector = selector
        self.locations = locations
        choices = ", ".join(_selector_label(location) for location in locations)
        super().__init__(f"Location '{selector}' is ambiguous. Choose one of: {choices}")


def _location_label(name, requested_name, country, latitude, longitude, provider_id):
    """Build a stable, human-readable selector for a location."""
    parts = [requested_name or name]
    if country and country.lower() not in (requested_name or "").lower():
        parts.append(country)
    label = ", ".join(parts)
    if latitude is not None and longitude is not None:
        label += f" ({latitude:.4f}, {longitude:.4f})"
    if provider_id:
        label += f" [owm:{provider_id}]"
    return label


def _selector_label(location):
    """The label to suggest for a location, and to accept as its selector.

    Manual entries are labelled with the bare name the user typed, which also
    matches every other location of that name, so they get a ``[manual]`` tag
    (the same way legacy rows carry ``[legacy]``).
    """
    label = location["location_label"]
    if location["provider"] == "manual":
        return f"{label} [manual]"
    return label


def get_connection(db_path=DEFAULT_DB_PATH):
    """Establish a connection and migrate older databases in place.

    A brand-new database is initialised to the latest supported schema.
    Older databases are upgraded by walking forward through the supported
    versions.  A database whose schema version is newer than this
    application supports is rejected with a clear error rather than being
    downgraded.
    """
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")

    try:
        version = _get_schema_version(conn)

        if version == 0:
            # Fresh database: apply every supported migration from scratch.
            for index in range(LATEST_SCHEMA_VERSION):
                _apply_migration_step(conn, index)

        elif version < BOM_VERSION:
            # A database the application can never understand: refuse instead
            # of attempting an unsafe downgrade.
            raise ValueError(
                f"Database schema version {version} is older than the earliest "
                f"version this application supports ({BOM_VERSION}). "
                "The database file was created by an incompatible older release "
                "and cannot be opened. Restore the database from a backup."
            )

        elif version < LATEST_SCHEMA_VERSION:
            # Supported, older database: apply the missing steps one at a time.
            for index in range(version, LATEST_SCHEMA_VERSION):
                _apply_migration_step(conn, index)

        elif version > LATEST_SCHEMA_VERSION:
            # A future-release database: reject rather than downgrade.
            raise ValueError(
                f"Database schema version {version} is newer than the latest "
                f"supported version ({LATEST_SCHEMA_VERSION}). The database was "
                f"created by a newer release of Weather Journal and cannot be "
                f"opened by this version. Upgrade (or restore a copy of) the "
                f"database."
            )
    except Exception:
        # Do not leak the connection handle when migration or validation fails.
        conn.close()
        raise

    return conn


def _get_or_create_location(conn, city, location):
    location = location or {}
    provider = str(location.get("provider") or "manual")
    provider_id = location.get("provider_id")
    requested_name = str(location.get("requested_name") or city)
    country = location.get("country")
    latitude = location.get("latitude")
    longitude = location.get("longitude")

    # Manual entries have no provider ID, so their full user-supplied location
    # text is their identity. API locations use the provider's stable city ID.
    if provider_id is None:
        if provider == "openweathermap" and latitude is not None and longitude is not None:
            provider_id = f"coord:{float(latitude):.6f},{float(longitude):.6f}"
        else:
            provider_id = requested_name.strip().casefold()
    provider_id = str(provider_id)
    label = _location_label(
        city, requested_name, country, latitude, longitude,
        provider_id if provider == "openweathermap" else None,
    )

    conn.execute("""
        INSERT INTO locations
            (provider, provider_id, name, requested_name, country,
             latitude, longitude, location_label)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(provider, provider_id) DO UPDATE SET
            name = excluded.name,
            requested_name = excluded.requested_name,
            country = excluded.country,
            latitude = excluded.latitude,
            longitude = excluded.longitude,
            location_label = excluded.location_label
    """, (
        provider, provider_id, city, requested_name, country,
        latitude, longitude, label,
    ))
    location_id = conn.execute(
        "SELECT id FROM locations WHERE provider = ? AND provider_id = ?",
        (provider, provider_id),
    ).fetchone()["id"]
    return location_id, requested_name, label


def save_entry(city, temp, humidity, condition, logged_at=None,
               db_path=DEFAULT_DB_PATH, location=None):
    """Save a weather observation with a stable location identity."""
    conn = get_connection(db_path)
    try:
        location_id, requested_name, location_label = _get_or_create_location(
            conn, city, location
        )
        ts = logged_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conn.execute("""
            INSERT INTO entries
                (city, temp, humidity, condition, logged_at, location_id,
                 requested_name, location_label)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            city, temp, humidity, condition, ts, location_id,
            requested_name, location_label,
        ))
        conn.commit()
    finally:
        conn.close()


def _matching_locations(conn, selector):
    provider_selector = selector[4:] if selector.lower().startswith("owm:") else None
    if provider_selector:
        rows = conn.execute("""
            SELECT * FROM locations
            WHERE provider = 'openweathermap' AND provider_id = ?
        """, (provider_selector,)).fetchall()
    else:
        rows = conn.execute("""
            SELECT DISTINCT l.* FROM locations AS l
            LEFT JOIN entries AS e ON e.location_id = l.id
            WHERE LOWER(l.location_label) = LOWER(?)
               OR LOWER(l.requested_name) = LOWER(?)
               OR LOWER(l.name) = LOWER(?)
               OR LOWER(e.location_label) = LOWER(?)
               OR LOWER(e.requested_name) = LOWER(?)
               OR (l.provider = 'manual' AND (
                       LOWER(l.location_label || ' [manual]') = LOWER(?)
                    OR LOWER(e.location_label || ' [manual]') = LOWER(?)
               ))
            ORDER BY l.location_label
        """, (
            selector, selector, selector, selector, selector,
            selector, selector,
        )).fetchall()
    return [dict(row) for row in rows]


def get_history(city=None, db_path=DEFAULT_DB_PATH):
    """Retrieve records, refusing to merge ambiguous location names."""
    conn = get_connection(db_path)
    try:
        params = ()
        where = ""
        if city:
            locations = _matching_locations(conn, city)
            if not locations:
                return []
            if len(locations) > 1:
                raise AmbiguousLocationError(city, locations)
            where = "WHERE e.location_id = ?"
            params = (locations[0]["id"],)

        rows = conn.execute(f"""
            SELECT e.city, e.temp, e.humidity, e.condition, e.logged_at,
                   l.provider, l.provider_id, l.country, l.latitude,
                   l.longitude, e.requested_name, e.location_label
            FROM entries AS e
            JOIN locations AS l ON l.id = e.location_id
            {where}
            ORDER BY e.id DESC
        """, params).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()
