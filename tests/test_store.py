"""Tests for persistent location identity and database migration."""

import sqlite3
import tempfile
import unittest
from pathlib import Path

from weather_journal.store import (
    AmbiguousLocationError,
    get_history,
    save_entry,
)


class TestLocationIdentity(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "weather.db"

    def tearDown(self):
        self.temp_dir.cleanup()

    def _save_springfield(self, provider_id, requested_name, latitude, temp):
        save_entry(
            "Springfield", temp, 50, "clear sky", db_path=self.db_path,
            location={
                "provider": "openweathermap",
                "provider_id": provider_id,
                "requested_name": requested_name,
                "country": "US",
                "latitude": latitude,
                "longitude": -90.0,
            },
        )

    def test_same_named_cities_have_separate_histories(self):
        self._save_springfield(4250542, "Springfield,IL,US", 39.8, 10.0)
        self._save_springfield(4409896, "Springfield,MO,US", 37.2, 30.0)

        illinois = get_history("owm:4250542", db_path=self.db_path)
        missouri = get_history("Springfield,MO,US", db_path=self.db_path)

        self.assertEqual([entry["temp"] for entry in illinois], [10.0])
        self.assertEqual([entry["temp"] for entry in missouri], [30.0])

    def test_ambiguous_bare_name_is_rejected_instead_of_merged(self):
        self._save_springfield(4250542, "Springfield,IL,US", 39.8, 10.0)
        self._save_springfield(4409896, "Springfield,MO,US", 37.2, 30.0)

        with self.assertRaises(AmbiguousLocationError) as context:
            get_history("Springfield", db_path=self.db_path)

        self.assertEqual(len(context.exception.locations), 2)
        self.assertIn("owm:4250542", str(context.exception))
        self.assertIn("owm:4409896", str(context.exception))

    def test_coordinates_are_fallback_identity_when_provider_id_is_missing(self):
        self._save_springfield(None, "Springfield,IL,US", 39.8, 10.0)
        self._save_springfield(None, "Springfield,MO,US", 37.2, 30.0)

        illinois = get_history("owm:coord:39.800000,-90.000000", db_path=self.db_path)
        missouri = get_history("owm:coord:37.200000,-90.000000", db_path=self.db_path)

        self.assertEqual([entry["temp"] for entry in illinois], [10.0])
        self.assertEqual([entry["temp"] for entry in missouri], [30.0])

    def test_aliases_preserve_historical_labels_and_remain_selectable(self):
        first_location = {
            "provider": "openweathermap",
            "provider_id": 2643743,
            "requested_name": "London,UK",
            "country": "GB",
            "latitude": 51.5072,
            "longitude": -0.1276,
        }
        second_location = dict(first_location, requested_name="London,GB")

        save_entry(
            "London", 10.0, 50, "clear sky", db_path=self.db_path,
            location=first_location,
        )
        save_entry(
            "London", 20.0, 55, "few clouds", db_path=self.db_path,
            location=second_location,
        )

        original_alias = get_history("London,UK", db_path=self.db_path)
        newer_alias = get_history("London,GB", db_path=self.db_path)

        self.assertEqual([entry["temp"] for entry in original_alias], [20.0, 10.0])
        self.assertEqual([entry["temp"] for entry in newer_alias], [20.0, 10.0])
        self.assertTrue(original_alias[0]["location_label"].startswith("London,GB"))
        self.assertTrue(original_alias[1]["location_label"].startswith("London,UK"))

    def test_old_database_rows_are_preserved_as_legacy_location(self):
        conn = sqlite3.connect(str(self.db_path))
        conn.execute("""
            CREATE TABLE entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                city TEXT NOT NULL,
                temp REAL NOT NULL,
                humidity INTEGER,
                condition TEXT,
                logged_at TEXT NOT NULL
            )
        """)
        conn.execute("""
            INSERT INTO entries (city, temp, humidity, condition, logged_at)
            VALUES ('Mumbai', 30, 80, 'haze', '2026-10-01 12:00:00')
        """)
        conn.commit()
        conn.close()

        records = get_history("Mumbai", db_path=self.db_path)

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["location_label"], "Mumbai [legacy]")


if __name__ == "__main__":
    unittest.main()
