from __future__ import annotations

from decimal import Decimal
import time
from typing import TYPE_CHECKING

from app.core.models.workers import EntryDecision, StrategyCycleState, StrategyState

if TYPE_CHECKING:
    from app.core.workers.runtime_core import WorkerRuntime


def submit_strict_entry(runtime: WorkerRuntime, decision: EntryDecision) -> None:
    planned_size = decision.planned_size
    submit_cycle = runtime._start_entry_cycle(decision, prefetch=False)
    runtime._set_strategy_state(StrategyState.ENTRY_ARMED)
    runtime._set_entry_cycle_state(StrategyCycleState.SUBMITTING)
    runtime._set_strategy_state(StrategyState.ENTRY_SUBMITTING)
    submit_kwargs = {
        "left_side": str(decision.left_action or ""),
        "right_side": str(decision.right_action or ""),
        "left_qty": runtime._format_order_size(planned_size.get("left_qty", Decimal("0"))),
        "right_qty": runtime._format_order_size(planned_size.get("right_qty", Decimal("0"))),
        "left_price_mode": str(runtime.task.runtime_params.get("left_price_mode") or "top_of_book"),
        "right_price_mode": str(runtime.task.runtime_params.get("right_price_mode") or "top_of_book"),
        "submitted_at_ms": int(time.time() * 1000),
        "entry_cycle_id": submit_cycle.cycle_id,
    }
    runtime.logger.info(
        "AUTO ENTRY START | edge=%s | direction=%s | cycle_id=%s | prefetch=%s | cycle_notional_usdt=%s | left_qty=%s | right_qty=%s",
        decision.edge_name,
        decision.direction,
        submit_cycle.cycle_id,
        False,
        planned_size.get("cycle_notional_usdt"),
        planned_size.get("left_qty"),
        planned_size.get("right_qty"),
    )
    runtime.submit_dual_test_orders(**submit_kwargs)
