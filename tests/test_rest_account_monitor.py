from __future__ import annotations

import threading
import time
import unittest

from app.core.models.account import ExchangeCredentials
from app.ui.coordinator_service_parts import RestAccountMonitor


class _BlockingConnector:
    def __init__(self) -> None:
        self.started_event = threading.Event()
        self.release_event = threading.Event()

    def connect(self, _credentials: ExchangeCredentials):
        self.started_event.set()
        self.release_event.wait(timeout=1.0)
        return {"ok": True}


class _FastConnector:
    def connect(self, _credentials: ExchangeCredentials):
        return {"ok": True}


class RestAccountMonitorTests(unittest.TestCase):
    def _make_credentials(self) -> ExchangeCredentials:
        return ExchangeCredentials(
            exchange="binance",
            api_key="k",
            api_secret="s",
            api_passphrase="",
        )

    def test_stop_waits_for_thread_and_clears_handle(self) -> None:
        connector = _BlockingConnector()
        monitor = RestAccountMonitor(self._make_credentials(), connector, poll_interval_seconds=0.01)
        snapshots: list[object] = []
        errors: list[str] = []

        monitor.start(lambda payload: snapshots.append(payload), lambda message: errors.append(message))
        self.assertTrue(connector.started_event.wait(timeout=0.5))
        monitor.stop()
        connector.release_event.set()
        time.sleep(0.05)

        self.assertFalse(errors)
        self.assertIsNone(monitor._thread)

    def test_stop_prevents_late_snapshot_callback(self) -> None:
        connector = _BlockingConnector()
        monitor = RestAccountMonitor(self._make_credentials(), connector, poll_interval_seconds=0.01)
        snapshots: list[object] = []

        monitor.start(lambda payload: snapshots.append(payload), lambda _message: None)
        self.assertTrue(connector.started_event.wait(timeout=0.5))
        monitor.stop()
        connector.release_event.set()
        time.sleep(0.05)

        self.assertEqual(snapshots, [])

    def test_monitor_can_restart_after_stop(self) -> None:
        connector = _FastConnector()
        monitor = RestAccountMonitor(self._make_credentials(), connector, poll_interval_seconds=0.01)
        snapshots: list[object] = []

        monitor.start(lambda payload: snapshots.append(payload), lambda _message: None)
        time.sleep(0.03)
        monitor.stop()
        first_count = len(snapshots)

        monitor.start(lambda payload: snapshots.append(payload), lambda _message: None)
        time.sleep(0.03)
        monitor.stop()

        self.assertGreater(first_count, 0)
        self.assertGreater(len(snapshots), first_count)


if __name__ == "__main__":
    unittest.main()
