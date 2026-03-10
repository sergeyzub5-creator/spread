from __future__ import annotations

import unittest

from app.core.events.bus import EventBus


class EventBusTests(unittest.TestCase):
    def test_publish_survives_subscriber_exception(self) -> None:
        bus = EventBus()
        received: list[object] = []

        def bad_callback(event: object) -> None:
            raise RuntimeError("boom")

        def good_callback(event: object) -> None:
            received.append(event)

        bus.subscribe("topic", bad_callback)
        bus.subscribe("topic", good_callback)

        bus.publish("topic", {"value": 1})

        self.assertEqual(received, [{"value": 1}])

    def test_unsubscribe_stops_future_delivery(self) -> None:
        bus = EventBus()
        received: list[str] = []

        def callback(event: str) -> None:
            received.append(event)

        bus.subscribe("topic", callback)
        bus.publish("topic", "first")
        bus.unsubscribe("topic", callback)
        bus.publish("topic", "second")

        self.assertEqual(received, ["first"])


if __name__ == "__main__":
    unittest.main()
