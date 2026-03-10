from __future__ import annotations

import time
from decimal import Decimal
from typing import TYPE_CHECKING

from app.core.models.workers import StrategyCycleState

if TYPE_CHECKING:
    from app.core.workers.runtime_core import WorkerRuntime


def exit_tail_resync_in_progress(runtime: WorkerRuntime) -> bool:
    return runtime.active_exit_cycle is not None and bool(runtime.active_exit_cycle.tail_resync_in_progress)


def exit_recovery_allowed(runtime: WorkerRuntime) -> bool:
    cycle = runtime.active_exit_cycle
    if cycle is None:
        return False
    if cycle.state is not StrategyCycleState.ACTIVE:
        return False
    deadline_ts = int(cycle.exit_grace_deadline_ts or 0)
    now_ms = int(time.time() * 1000)
    if deadline_ts > 0 and now_ms < deadline_ts:
        return False
    if runtime._has_live_leg_orders() and not runtime._exit_has_stale_live_orders():
        return False
    deficit_leg = runtime._exit_cycle_deficit_leg()
    remaining_qty = runtime._exit_cycle_remaining_qty(deficit_leg) if deficit_leg is not None else Decimal("0")
    if deficit_leg is None or remaining_qty <= Decimal("0"):
        return False
    signature = f"{deficit_leg}:{remaining_qty}"
    if (
        cycle.last_recovery_signature == signature
        and cycle.last_recovery_attempt_ts is not None
        and (now_ms - cycle.last_recovery_attempt_ts) < runtime.EXIT_RECOVERY_DEBOUNCE_MS
    ):
        runtime.logger.info(
            "EXIT_RECOVERY_DEBOUNCED | cycle_id=%s | signature=%s | debounce_ms=%s",
            cycle.cycle_id,
            signature,
            runtime.EXIT_RECOVERY_DEBOUNCE_MS,
        )
        return False
    cycle.last_recovery_signature = signature
    cycle.last_recovery_attempt_ts = now_ms
    runtime.logger.info(
        "EXIT_RECOVERY_ALLOWED | cycle_id=%s | signature=%s | remaining_qty=%s",
        cycle.cycle_id,
        signature,
        runtime._format_order_size(remaining_qty),
    )
    runtime._sync_active_exit_cycle_metrics()
    return True
