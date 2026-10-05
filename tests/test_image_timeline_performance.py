import unittest
from datetime import date
from unittest.mock import MagicMock, patch

import app


class ImageTimelineQueryTests(unittest.TestCase):
    def test_timeline_reduces_logs_before_expensive_joins(self):
        cursor = MagicMock()
        cursor.fetchall.return_value = []
        connection = MagicMock()
        connection.cursor.return_value = cursor

        with patch.object(app, "get_db_connection", return_value=connection):
            result = app.fetch_image_timeline(date(2026, 10, 1), date(2026, 10, 2))

        self.assertEqual(result, [])
        query, parameters = cursor.execute.call_args.args
        self.assertIn("WITH timeline_logs AS MATERIALIZED", query)
        self.assertLess(query.index("DISTINCT ON"), query.index("LEFT JOIN LATERAL"))
        self.assertEqual(parameters, (date(2026, 10, 1), date(2026, 10, 2)))

    def test_timeline_stats_are_returned_by_one_query(self):
        cursor = MagicMock()
        cursor.fetchall.return_value = [
            ("Alice", 12, 5),
            ("Bob", 7, 3),
        ]
        connection = MagicMock()
        connection.cursor.return_value = cursor

        with patch.object(app, "get_db_connection", return_value=connection):
            image_counts, part_counts, users = app.fetch_image_timeline_stats(
                date(2026, 10, 1), date(2026, 10, 2)
            )

        self.assertEqual(image_counts, {"Alice": 12, "Bob": 7})
        self.assertEqual(part_counts, {"Alice": 5, "Bob": 3})
        self.assertEqual(users, ["Alice", "Bob"])
        cursor.execute.assert_called_once()
        cursor.close.assert_called_once()
        connection.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
