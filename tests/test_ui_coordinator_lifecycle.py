from __future__ import annotations

import threading
import unittest
from concurrent.futures import Future

from app.core.models.workers import WorkerEvent
from app.ui.coordinator_service_parts import UiCoordinatorPartsMixin


class _FakeLogger:
    def info(self, *_args, **_kwargs) -> None:
        return


class _FakeEventBus:
    def __init__(self) -> None:
        self.unsubscribed: list[tuple[str, object]] = []

    def unsubscribe(self, topic: str, callback: object) -> None:
        self.unsubscribed.append((topic, callback))


class _FakeMarketDataService:
    def __init__(self) -> None:
        self.unsubscribed: list[tuple[object, object]] = []
        self.shutdown_called = 0

    def unsubscribe_l1(self, instrument: object, callback: object) -> None:
        self.unsubscribed.append((instrument, callback))

    def shutdown(self) -> None:
        self.shutdown_called += 1


class _FakeWorkerManager:
    def __init__(self) -> None:
        self.shutdown_called = 0

    def shutdown(self) -> None:
        self.shutdown_called += 1


class _FakeMonitor:
    def __init__(self) -> None:
        self.stop_called = 0

    def stop(self) -> None:
        self.stop_called += 1


class _FakeExecutor:
    def __init__(self) -> None:
        self.shutdown_calls: list[tuple[bool, bool]] = []

    def shutdown(self, *, wait: bool, cancel_futures: bool) -> None:
        self.shutdown_calls.append((wait, cancel_futures))


class _FakeCoordinator(UiCoordinatorPartsMixin):
    def __init__(self) -> None:
        UiCoordinatorPartsMixin.__init__(self)
        self._shutdown = False
        self._logger = _FakeLogger()
        self._subscription_lock = threading.RLock()
        self._monitor_lock = threading.RLock()
        self._async_lock = threading.RLock()
        self._subscriptions: dict[str, tuple[object, object]] = {}
        self._account_monitors: dict[str, object] = {}
        self._pending_futures: set[Future] = set()
        self._async_executor = _FakeExecutor()
        self.market_data_service = _FakeMarketDataService()
        self.worker_manager = _FakeWorkerManager()
        self.event_bus = _FakeEventBus()
        self.worker_event_received = _FakeSignal()
        self.execution_stream_health_updated = _FakeSignal()

    def _on_worker_state(self, _state: object) -> None:
        return

    def _on_worker_event(self, _event: object) -> None:
        return


class UiCoordinatorLifecycleTests(unittest.TestCase):
    def test_shutdown_unsubscribes_and_stops_owned_services(self) -> None:
        coordinator = _FakeCoordinator()
        monitor = _FakeMonitor()
        instrument = object()
        callback = object()
        coordinator._subscriptions["slot-left"] = (instrument, callback)
        coordinator._account_monitors["monitor-1"] = monitor

        done_future: Future = Future()
        done_future.set_result(None)
        coordinator._pending_futures.add(done_future)

        coordinator.shutdown()

        self.assertTrue(coordinator._shutdown)
        self.assertEqual(coordinator.market_data_service.unsubscribed, [(instrument, callback)])
        self.assertEqual(coordinator.market_data_service.shutdown_called, 1)
        self.assertEqual(coordinator.worker_manager.shutdown_called, 1)
        self.assertEqual(monitor.stop_called, 1)
        self.assertEqual(coordinator._account_monitors, {})
        self.assertIn(("worker_state", coordinator._on_worker_state), coordinator.event_bus.unsubscribed)
        self.assertIn(("worker_events", coordinator._on_worker_event), coordinator.event_bus.unsubscribed)
        self.assertEqual(coordinator._async_executor.shutdown_calls, [(False, True)])

    def test_shutdown_is_idempotent(self) -> None:
        coordinator = _FakeCoordinator()
        monitor = _FakeMonitor()
        coordinator._account_monitors["monitor-1"] = monitor

        coordinator.shutdown()
        coordinator.shutdown()

        self.assertEqual(coordinator.market_data_service.shutdown_called, 1)
        self.assertEqual(coordinator.worker_manager.shutdown_called, 1)
        self.assertEqual(monitor.stop_called, 1)
        self.assertEqual(coordinator._async_executor.shutdown_calls, [(False, True)])

    def test_worker_event_propagates_execution_stream_health_signal(self) -> None:
        coordinator = _FakeCoordinator()
        event = WorkerEvent(
            worker_id="worker-1",
            event_type="execution_stream_health_warning",
            timestamp=1,
            payload={"status": "DEGRADED", "warning": {"code": "STREAM_DISCONNECTED_LONG"}},
        )

        UiCoordinatorPartsMixin._on_worker_event(coordinator, event)

        self.assertEqual(len(coordinator.worker_event_received.emitted), 1)
        self.assertEqual(len(coordinator.execution_stream_health_updated.emitted), 1)
        worker_id, payload = coordinator.execution_stream_health_updated.emitted[0]
        self.assertEqual(worker_id, "worker-1")
        self.assertEqual(payload.get("status"), "DEGRADED")
        self.assertEqual(payload.get("warning", {}).get("code"), "STREAM_DISCONNECTED_LONG")


class _FakeSignal:
    def __init__(self) -> None:
        self.emitted: list[tuple] = []

    def emit(self, *args) -> None:
        self.emitted.append(args)


if __name__ == "__main__":
    unittest.main()
