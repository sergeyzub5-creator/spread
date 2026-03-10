from __future__ import annotations

import unittest
from decimal import Decimal
from types import SimpleNamespace

from app.core.models.execution import ExecutionStreamEvent
from app.core.workers.runtime_execution_mixin import WorkerRuntimeExecutionMixin


class _FakeAdmissionRuntime:
    def __init__(self) -> None:
        self._entry_cycle_order_keys = {"left": {}, "right": {}}
        self._exit_cycle_order_keys = {"left": {}, "right": {}}
        self._tombstones = {"left": set(), "right": set()}
        self.last_resolve_kwargs: dict | None = None

    @staticmethod
    def _order_fill_key(*, order_id: str | None, client_order_id: str | None) -> str | None:
        oid = str(order_id or "").strip()
        cid = str(client_order_id or "").strip()
        if oid:
            return f"o:{oid}"
        if cid:
            return f"c:{cid}"
        return None

    def _is_order_key_tombstoned(self, *, leg_name: str, order_key: str) -> bool:
        return order_key in self._tombstones.get(leg_name, set())

    def _resolve_attempt_for_update(self, **kwargs):
        self.last_resolve_kwargs = dict(kwargs)
        attempt_id = str(kwargs.get("attempt_id") or "").strip()
        if attempt_id or kwargs.get("order_id") or kwargs.get("client_order_id"):
            return SimpleNamespace(attempt_id=attempt_id or "attempt-x", owner_epoch=1, terminal=False)
        return None


class _FakeFillRuntime(WorkerRuntimeExecutionMixin):
    def __init__(self) -> None:
        self.left_leg_state = SimpleNamespace(
            order_status="NEW",
            filled_qty=Decimal("0"),
            target_qty=Decimal("0"),
            remaining_qty=Decimal("0"),
            avg_price=None,
            side=None,
            latency_fill_ms=None,
            actual_position_qty=Decimal("0"),
            is_flat=True,
            flat_confirmed_by_exchange=False,
            last_position_resync_ts=None,
            remaining_close_qty=Decimal("0"),
        )
        self.right_leg_state = SimpleNamespace(**self.left_leg_state.__dict__)
        self.state = SimpleNamespace(metrics={"left_fill_latency_ms": None, "right_fill_latency_ms": None})
        self._leg_order_fill_tracker = {"left": {}, "right": {}}
        self._leg_order_position_effects = {"left": {}, "right": {}}
        self.active_entry_cycle = None
        self.active_exit_cycle = None

    @staticmethod
    def _decimal_or_none(value):
        if value in (None, "", "-"):
            return None
        return Decimal(str(value))

    def _leg_state(self, leg_name: str):
        return self.left_leg_state if leg_name == "left" else self.right_leg_state

    @staticmethod
    def _resolve_order_fill_tracker_keys(*, leg_name: str, order_key: str):
        del leg_name
        return [order_key]

    @staticmethod
    def _entry_cycle_pair_matches_target() -> bool:
        return False

    def _refresh_leg_position_derived_fields(self, leg_name: str, *, confirmed_by_exchange: bool) -> None:
        del leg_name, confirmed_by_exchange
        return

    def _sync_active_entry_cycle_from_legs(self) -> None:
        return

    def _sync_active_exit_cycle_from_legs(self) -> None:
        return


class RuntimeExecutionAdmissionTests(unittest.TestCase):
    def test_admission_prefers_attempt_identity(self) -> None:
        runtime = _FakeAdmissionRuntime()
        admitted, reason = WorkerRuntimeExecutionMixin._is_event_admissible(
            runtime,
            leg_name="left",
            event_order_id="123",
            event_client_order_id=None,
            event_attempt=SimpleNamespace(attempt_id="attempt-1"),
        )
        self.assertTrue(admitted)
        self.assertEqual(reason, "ATTEMPT_IDENTITY")

    def test_overlap_prefetch_client_attempt_id_routes_to_attempt(self) -> None:
        runtime = _FakeAdmissionRuntime()
        WorkerRuntimeExecutionMixin._resolve_attempt_for_event(
            runtime,
            leg_name="right",
            event_order_id="202153990703",
            event_client_order_id="attempt-4",
            preferred_attempt_id=None,
        )
        self.assertIsNotNone(runtime.last_resolve_kwargs)
        self.assertEqual(runtime.last_resolve_kwargs.get("attempt_id"), "attempt-4")

    def test_late_tombstoned_event_rejected_explicitly(self) -> None:
        runtime = _FakeAdmissionRuntime()
        runtime._tombstones["right"].add("o:77")
        admitted, reason = WorkerRuntimeExecutionMixin._is_event_admissible(
            runtime,
            leg_name="right",
            event_order_id="77",
            event_client_order_id=None,
            event_attempt=None,
        )
        self.assertFalse(admitted)
        self.assertEqual(reason, "LATE_TOMBSTONED")

    def test_unknown_foreign_event_rejected(self) -> None:
        runtime = _FakeAdmissionRuntime()
        admitted, reason = WorkerRuntimeExecutionMixin._is_event_admissible(
            runtime,
            leg_name="left",
            event_order_id="999",
            event_client_order_id="cid-999",
            event_attempt=None,
        )
        self.assertFalse(admitted)
        self.assertEqual(reason, "FOREIGN_UNKNOWN")

    def test_overlap_prefetch_fill_before_promote_is_admitted_by_order_key(self) -> None:
        runtime = _FakeAdmissionRuntime()
        runtime._entry_cycle_order_keys["right"]["o:55"] = 9
        admitted, reason = WorkerRuntimeExecutionMixin._is_event_admissible(
            runtime,
            leg_name="right",
            event_order_id="55",
            event_client_order_id=None,
            event_attempt=None,
        )
        self.assertTrue(admitted)
        self.assertEqual(reason, "ORDER_KEY_KNOWN")

    def test_duplicate_cumulative_fill_is_idempotent(self) -> None:
        runtime = _FakeFillRuntime()
        event = ExecutionStreamEvent(
            exchange="x",
            event_type="execution",
            event_time=1,
            transaction_time=1,
            symbol="SOLUSDT",
            order_id="100",
            client_order_id="attempt-1",
            order_status="FILLED",
            execution_type="TRADE",
            side="BUY",
            order_type="LIMIT",
            position_side=None,
            last_fill_qty="0.1",
            cumulative_fill_qty="0.1",
            last_fill_price="1",
            average_price="1",
            realized_pnl=None,
            raw={},
        )
        WorkerRuntimeExecutionMixin._sync_leg_event_state(runtime, leg_name="left", event=event)
        WorkerRuntimeExecutionMixin._sync_leg_event_state(runtime, leg_name="left", event=event)
        self.assertEqual(runtime.left_leg_state.filled_qty, Decimal("0.1"))


if __name__ == "__main__":
    unittest.main()
