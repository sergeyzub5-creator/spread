from __future__ import annotations

import threading
import time
import unittest
from types import SimpleNamespace

from app.core.workers.runtime_guard_mixin import WorkerRuntimeGuardMixin
from app.core.workers.runtime_parts import WorkerRuntimePartsMixin


class _FakeEventBus:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._events: list[tuple[str, object]] = []

    def publish(self, topic: str, event: object) -> None:
        with self._lock:
            self._events.append((topic, event))

    def count_topic(self, topic: str) -> int:
        with self._lock:
            return sum(1 for item_topic, _ in self._events if item_topic == topic)


class _PublishRuntime(WorkerRuntimePartsMixin):
    def __init__(self, *, interval_ms: int) -> None:
        self.task = SimpleNamespace(worker_id="test-worker")
        self.event_bus = _FakeEventBus()
        self.state = SimpleNamespace(status="running", metrics={})
        self._state_lock = threading.RLock()
        self._state_publish_interval_ms = interval_ms
        self._last_state_publish_ms = 0
        self._state_publish_timer_scheduled = False

    def _refresh_derived_metrics(self) -> None:
        return


class _GuardRuntime(WorkerRuntimeGuardMixin):
    def __init__(self) -> None:
        self._is_spread_entry_runtime = True
        self.state = SimpleNamespace(status="running", metrics={"runtime_owner": None})
        self._state_lock = threading.RLock()
        self._runtime_health_mode = "HEALTHY"
        self._last_runtime_reconcile_started_ms = 0
        self.FULL_STATE_RECONCILE_INTERVAL_MS = 60000
        self._publish_calls: list[bool] = []
        self._reconcile_reasons: list[str] = []
        self._connectivity_degraded_value = True
        self._owner_context_value = "TEST_OWNER"
        self._owner_stale_reason_value = "OWNER_STALE:TEST"
        self.logger = SimpleNamespace(info=lambda *args, **kwargs: None, warning=lambda *args, **kwargs: None)

    def _refresh_derived_metrics(self) -> None:
        return

    def _maybe_reset_cycle_growth_on_idle(self, *, now_ms: int) -> None:
        return

    def _connectivity_degraded(self) -> bool:
        return self._connectivity_degraded_value

    def _current_owner_context(self) -> str | None:
        return self._owner_context_value

    def _stale_owner_reason(self, *, owner_context: str | None) -> str | None:
        if owner_context is None:
            return None
        return self._owner_stale_reason_value

    def _runtime_reconcile_active(self) -> bool:
        return False

    def _publish_state(self, *, force: bool = False) -> None:
        self._publish_calls.append(force)

    def _maybe_start_runtime_reconcile(self, *, reason: str) -> None:
        self._reconcile_reasons.append(reason)


class RuntimeStatePublishTests(unittest.TestCase):
    def test_publish_state_throttles_and_deferred_flushes(self) -> None:
        runtime = _PublishRuntime(interval_ms=30)

        runtime._publish_state()
        for _ in range(6):
            runtime._publish_state()

        self.assertEqual(runtime.event_bus.count_topic("worker_state"), 1)

        deadline = time.time() + 1.0
        while time.time() < deadline:
            if runtime.event_bus.count_topic("worker_state") >= 2:
                break
            time.sleep(0.01)

        self.assertEqual(runtime.event_bus.count_topic("worker_state"), 2)

    def test_watchdog_tick_forces_publish_on_health_transition(self) -> None:
        runtime = _GuardRuntime()

        runtime._watchdog_tick()
        self.assertEqual(runtime._runtime_health_mode, "DEGRADED_CONNECTIVITY")
        self.assertEqual(runtime._publish_calls, [True])
        self.assertEqual(runtime._reconcile_reasons, ["OWNER_STALE:TEST"])

        runtime._publish_calls.clear()
        runtime._watchdog_tick()
        self.assertEqual(runtime._publish_calls, [])

    def test_watchdog_tick_triggers_connectivity_restored_reconcile(self) -> None:
        runtime = _GuardRuntime()
        runtime._runtime_health_mode = "DEGRADED_CONNECTIVITY"
        runtime._connectivity_degraded_value = False
        runtime._owner_context_value = None
        runtime._owner_stale_reason_value = None

        runtime._watchdog_tick()

        self.assertEqual(runtime._runtime_health_mode, "HEALTHY")
        self.assertEqual(runtime._publish_calls, [True])
        self.assertEqual(runtime._reconcile_reasons, ["CONNECTIVITY_RESTORED"])

    def test_reconcile_preempt_allows_owner_stale_reason(self) -> None:
        runtime = _GuardRuntime()
        runtime._owner_context_value = "ENTRY_RECOVERY"

        allowed = runtime._runtime_reconcile_can_preempt_active_owner(reason="OWNER_STALE:ENTRY_RECOVERY")

        self.assertTrue(allowed)

    def test_reconcile_preempt_allows_critical_full_state_reason(self) -> None:
        runtime = _GuardRuntime()
        runtime._owner_context_value = "HEDGE_PROTECTION"

        allowed = runtime._runtime_reconcile_can_preempt_active_owner(reason="FULL_STATE:HEDGE_PROTECTION_TIMEOUT")

        self.assertTrue(allowed)

    def test_reconcile_preempt_defers_when_owner_active_and_reason_non_critical(self) -> None:
        runtime = _GuardRuntime()
        runtime._owner_context_value = "ENTRY_CYCLE#12"

        allowed = runtime._runtime_reconcile_can_preempt_active_owner(reason="PERIODIC_FULL_STATE")

        self.assertFalse(allowed)


if __name__ == "__main__":
    unittest.main()
