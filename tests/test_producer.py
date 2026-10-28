import unittest
from datetime import date, datetime

import producer


class ProducerMappingTests(unittest.TestCase):
    def setUp(self):
        self.row = {
            "fl_date": date(2024, 1, 1),
            "origin": "JFK",
            "dest": "ATL",
            "op_unique_carrier": "DL",
            "op_carrier_fl_num": 123.0,
            "crs_dep_time": 1230,
            "dep_time": 1242.0,
            "dep_delay": 12.0,
            "arr_delay": 8.0,
            "cancelled": 0.0,
            "diverted": 0.0,
            "distance": 760.0,
        }

    def test_key_uses_task_one_flight_identity(self):
        self.assertEqual(
            producer.make_flight_id(self.row),
            "2024-01-01|DL|123|JFK|1230",
        )

    def test_scheduled_departure_is_event_time(self):
        event_time, source = producer.make_event_time(self.row)
        self.assertEqual(event_time, datetime(2024, 1, 1, 12, 30))
        self.assertEqual(source, "scheduled_departure_local")

    def test_actual_departure_does_not_replace_scheduled_time(self):
        row = {**self.row, "dep_time": 1242.0}
        event_time, source = producer.make_event_time(row)
        self.assertEqual(event_time, datetime(2024, 1, 1, 12, 30))
        self.assertEqual(source, "scheduled_departure_local")

    def test_delayed_flight_stays_in_scheduled_day(self):
        row = {**self.row, "crs_dep_time": 2350, "dep_time": 10.0, "dep_delay": 20.0}
        event_time, source = producer.make_event_time(row)
        self.assertEqual(event_time, datetime(2024, 1, 1, 23, 50))
        self.assertEqual(source, "scheduled_departure_local")

    def test_hhmm_2400_rolls_to_next_day(self):
        row = {**self.row, "crs_dep_time": 2400}
        event_time, _ = producer.make_event_time(row)
        self.assertEqual(event_time, datetime(2024, 1, 2, 0, 0))

    def test_actual_departure_is_fallback_only_if_scheduled_is_missing(self):
        row = {**self.row, "crs_dep_time": None, "dep_time": 1242.0}
        event_time, source = producer.make_event_time(row)
        self.assertEqual(event_time, datetime(2024, 1, 1, 12, 42))
        self.assertEqual(source, "actual_departure_fallback")

    def test_payload_contains_key_domain_and_timestamp(self):
        key, payload, timestamp = producer.make_payload(self.row, {"JFK": "New York"}, 7, "test-run")
        self.assertEqual(key, "2024-01-01|DL|123|JFK|1230")
        self.assertEqual(payload["origin_state"], "New York")
        self.assertEqual(payload["event_time"], "2024-01-01 12:30:00")
        self.assertEqual(payload["event_time_source"], "scheduled_departure_local")
        self.assertEqual(payload["event_sequence"], 7)
        self.assertGreater(timestamp, 0)


if __name__ == "__main__":
    unittest.main()
