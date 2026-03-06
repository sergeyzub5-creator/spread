from __future__ import annotations

from dataclasses import asdict, dataclass, field
from decimal import Decimal
from typing import Any

from app.core.models.instrument import InstrumentId


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
        return asdict(self)


@dataclass(frozen=True, slots=True)
class WorkerEvent:
    worker_id: str
    event_type: str
    timestamp: int
    payload: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)
