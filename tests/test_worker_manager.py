from __future__ import annotations

import unittest
from unittest.mock import patch

from app.core.events.bus import EventBus
from app.core.workers import manager as worker_manager_module
from app.core.workers.manager import WorkerManager


class _FakeRuntime:
    def __init__(self, task, market_data_service, event_bus) -> None:
        self.task = task
        self.market_data_service = market_data_service
        self.event_bus = event_bus
        self.started = False
        self.stopped = False
        self.entry_triggered = False
        self.raise_on_stop = bool(getattr(task, "raise_on_stop", False))

    def start(self) -> None:
        self.started = True

    def stop(self) -> None:
        if self.raise_on_stop:
            raise RuntimeError("stop failed")
        self.stopped = True

    def trigger_entry_signal(self) -> None:
        self.entry_triggered = True

    def execution_stream_health_snapshot(self) -> dict:
        return {"status": "HEALTHY", "streams": {"primary": {"connected": True}}}


class _FakeTask:
    def __init__(self, worker_id: str, *, raise_on_stop: bool = False) -> None:
        self.worker_id = worker_id
        self.raise_on_stop = raise_on_stop


class WorkerManagerTests(unittest.TestCase):
    def test_start_worker_replaces_existing_runtime(self) -> None:
        event_bus = EventBus()
        fake_market_data_service = object()
        manager = WorkerManager(fake_market_data_service, event_bus)
        first_task = _FakeTask("worker-1")
        second_task = _FakeTask("worker-1")

        with patch.object(worker_manager_module, "WorkerRuntime", _FakeRuntime):
            first_runtime = manager.start_worker(first_task)
            second_runtime = manager.start_worker(second_task)

        self.assertTrue(first_runtime.started)
        self.assertTrue(first_runtime.stopped)
        self.assertTrue(second_runtime.started)
        self.assertIs(manager.get_worker("worker-1"), second_runtime)

    def test_trigger_entry_signal_uses_registered_runtime(self) -> None:
        event_bus = EventBus()
        fake_market_data_service = object()
        manager = WorkerManager(fake_market_data_service, event_bus)
        task = _FakeTask("worker-2")

        with patch.object(worker_manager_module, "WorkerRuntime", _FakeRuntime):
            runtime = manager.start_worker(task)
            manager.trigger_entry_signal("worker-2")

        self.assertTrue(runtime.entry_triggered)

    def test_shutdown_stops_all_registered_runtimes(self) -> None:
        event_bus = EventBus()
        fake_market_data_service = object()
        manager = WorkerManager(fake_market_data_service, event_bus)
        task_a = _FakeTask("worker-a")
        task_b = _FakeTask("worker-b")

        with patch.object(worker_manager_module, "WorkerRuntime", _FakeRuntime):
            runtime_a = manager.start_worker(task_a)
            runtime_b = manager.start_worker(task_b)
            manager.shutdown()

        self.assertTrue(runtime_a.stopped)
        self.assertTrue(runtime_b.stopped)
        self.assertIsNone(manager.get_worker("worker-a"))
        self.assertIsNone(manager.get_worker("worker-b"))

    def test_shutdown_continues_when_one_runtime_stop_fails(self) -> None:
        event_bus = EventBus()
        fake_market_data_service = object()
        manager = WorkerManager(fake_market_data_service, event_bus)
        bad_task = _FakeTask("worker-bad", raise_on_stop=True)
        good_task = _FakeTask("worker-good")

        with patch.object(worker_manager_module, "WorkerRuntime", _FakeRuntime):
            runtime_bad = manager.start_worker(bad_task)
            runtime_good = manager.start_worker(good_task)
            manager.shutdown()

        self.assertFalse(runtime_bad.stopped)
        self.assertTrue(runtime_good.stopped)
        self.assertIsNone(manager.get_worker("worker-bad"))
        self.assertIsNone(manager.get_worker("worker-good"))

    def test_execution_stream_health_snapshot_uses_runtime_snapshot(self) -> None:
        event_bus = EventBus()
        fake_market_data_service = object()
        manager = WorkerManager(fake_market_data_service, event_bus)
        task = _FakeTask("worker-health")

        with patch.object(worker_manager_module, "WorkerRuntime", _FakeRuntime):
            manager.start_worker(task)
            snapshot = manager.execution_stream_health_snapshot("worker-health")

        self.assertEqual(snapshot.get("status"), "HEALTHY")
        self.assertTrue(snapshot.get("streams", {}).get("primary", {}).get("connected"))


if __name__ == "__main__":
    unittest.main()
