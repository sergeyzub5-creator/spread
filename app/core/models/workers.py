from __future__ import annotations

from dataclasses import asdict, dataclass, field
from decimal import Decimal
from enum import StrEnum
from typing import Any

from app.core.models.account import ExchangeCredentials
from app.core.models.instrument import InstrumentId


class StrategyState(StrEnum):
    IDLE = "IDLE"
    ENTRY_ARMED = "ENTRY_ARMED"
    ENTRY_SUBMITTING = "ENTRY_SUBMITTING"
    ENTRY_PARTIAL = "ENTRY_PARTIAL"
    IN_POSITION = "IN_POSITION"
    EXIT_ARMED = "EXIT_ARMED"
    EXIT_SUBMITTING = "EXIT_SUBMITTING"
    EXIT_PARTIAL = "EXIT_PARTIAL"
    RECOVERY = "RECOVERY"
    FAILED = "FAILED"
    COOLDOWN = "COOLDOWN"


EntryState = StrategyState


class StrategyCycleType(StrEnum):
    ENTRY = "ENTRY"
    EXIT = "EXIT"


class StrategyCycleState(StrEnum):
    PLANNED = "PLANNED"
    SUBMITTING = "SUBMITTING"
    ACTIVE = "ACTIVE"
    COMMITTED = "COMMITTED"
    SUCCESS = "SUCCESS"
    ABORT = "ABORT"
    RESTORE_HEDGE = "RESTORE_HEDGE"


@dataclass(slots=True)
class LegState:
    exchange: str
    symbol: str
    side: str | None = None
    target_qty: Decimal = Decimal("0")
    filled_qty: Decimal = Decimal("0")
    avg_price: Decimal | None = None
    order_status: str = "IDLE"
    requested_qty: Decimal = Decimal("0")
    remaining_qty: Decimal = Decimal("0")
    actual_position_qty: Decimal = Decimal("0")
    remaining_close_qty: Decimal = Decimal("0")
    is_flat: bool = True
    last_position_resync_ts: int | None = None
    flat_confirmed_by_exchange: bool = False
    last_order_reduce_only: bool = False
    latency_ack_ms: int | None = None
    latency_fill_ms: int | None = None
    last_error: str | None = None


@dataclass(slots=True)
class StrategyPosition:
    direction: str
    entry_edge: Decimal | None
    active_edge: str | None
    left_side: str | None
    right_side: str | None
    left_target_qty: Decimal
    right_target_qty: Decimal
    left_filled_qty: Decimal
    right_filled_qty: Decimal
    left_avg_fill_price: Decimal | None
    right_avg_fill_price: Decimal | None
    entry_time: int | None
    state: StrategyState


@dataclass(slots=True)
class StrategyCycle:
    cycle_id: int
    cycle_type: StrategyCycleType
    state: StrategyCycleState
    direction: str | None
    edge_name: str | None
    edge_value: Decimal | None
    target_notional_usdt: Decimal
    left_start_qty: Decimal
    right_start_qty: Decimal
    left_target_qty: Decimal
    right_target_qty: Decimal
    left_filled_qty: Decimal = Decimal("0")
    right_filled_qty: Decimal = Decimal("0")
    started_at: int | None = None
    completed_at: int | None = None
    left_side: str | None = None
    right_side: str | None = None
    last_error: str | None = None
    left_order_id: str | None = None
    right_order_id: str | None = None
    left_client_order_id: str | None = None
    right_client_order_id: str | None = None
    left_acked: bool = False
    right_acked: bool = False
    tail_resync_in_progress: bool = False
    tail_resync_attempts: int = 0
    tail_reduce_only_seen: bool = False
    exit_grace_deadline_ts: int | None = None
    last_recovery_attempt_ts: int | None = None
    last_recovery_signature: str | None = None


@dataclass(frozen=True, slots=True)
class EntryDecision:
    edge: Decimal | None
    direction: str | None
    threshold: Decimal
    validation_result: dict[str, Any]
    planned_size: dict[str, Decimal]
    edge_name: str | None = None
    left_action: str | None = None
    right_action: str | None = None
    block_reason: str | None = None
    is_executable: bool = False
    forced_signal: bool = False


@dataclass(slots=True)
class RecoveryPlan:
    deficit_leg: str | None
    qty_to_rebalance: Decimal = Decimal("0")
    attempts_used: int = 0
    action_type: str | None = None


@dataclass(slots=True)
class OrderAttempt:
    attempt_id: str
    leg_name: str
    owner_epoch: int
    cycle_id: int | None = None
    cycle_type: str | None = None
    side: str | None = None
    reduce_only: bool = False
    position_effect: int = 1
    submitted_at_ms: int = 0
    request_sent_at_ms: int | None = None
    order_id: str | None = None
    client_order_id: str | None = None
    status: str = "SUBMITTING"
    first_event_seen: bool = False
    filled_seen: bool = False
    terminal: bool = False


@dataclass(frozen=True, slots=True)
class WorkerTask:
    worker_id: str
    left_instrument: InstrumentId
    right_instrument: InstrumentId
    entry_threshold: Decimal
    exit_threshold: Decimal
    target_notional: Decimal
    step_notional: Decimal
    execution_mode: str
    run_mode: str
    execution_credentials: ExchangeCredentials | None = None
    left_execution_credentials: ExchangeCredentials | None = None
    right_execution_credentials: ExchangeCredentials | None = None
    runtime_params: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(slots=True)
class WorkerState:
    worker_id: str
    status: str
    current_pair: tuple[InstrumentId, InstrumentId] | None
    last_error: str | None
    started_at: int | None
    stopped_at: int | None
    metrics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        pair = self.current_pair
        return {
            "worker_id": self.worker_id,
            "status": self.status,
            "current_pair": (
                {"exchange": pair[0].exchange, "market_type": pair[0].market_type, "symbol": pair[0].symbol},
                {"exchange": pair[1].exchange, "market_type": pair[1].market_type, "symbol": pair[1].symbol},
            ) if pair else None,
            "last_error": self.last_error,
            "started_at": self.started_at,
            "stopped_at": self.stopped_at,
            "metrics": dict(self.metrics),
        }


@dataclass(frozen=True, slots=True)
class WorkerEvent:
    worker_id: str
    event_type: str
    timestamp: int
    payload: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "worker_id": self.worker_id,
            "event_type": self.event_type,
            "timestamp": self.timestamp,
            "payload": dict(self.payload),
        }
