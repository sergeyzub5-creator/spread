from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal
from typing import Any

from app.core.models.instrument import InstrumentId


@dataclass(frozen=True, slots=True)
class ExecutionOrderRequest:
    instrument_id: InstrumentId
    side: str
    order_type: str
    quantity: Decimal | None = None
    price: Decimal | None = None
    time_in_force: str | None = None
    position_side: str | None = None
    position_idx: int | None = None
    reduce_only: bool | None = None
    close_position: bool | None = None
    new_client_order_id: str | None = None
    response_type: str = "ACK"
    stop_price: Decimal | None = None
    activation_price: Decimal | None = None
    callback_rate: Decimal | None = None
    working_type: str | None = None
    price_protect: bool | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["instrument_id"] = self.instrument_id.to_dict()
        return payload


@dataclass(frozen=True, slots=True)
class ExecutionOrderResult:
    exchange: str
    route: str
    request_id: str
    symbol: str
    order_id: str | None
    client_order_id: str | None
    status: str
    side: str
    order_type: str
    position_side: str | None
    price: str | None
    original_qty: str | None
    executed_qty: str | None
    avg_price: str | None
    update_time: int | None
    raw: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ExecutionStreamEvent:
    exchange: str
    event_type: str
    event_time: int | None
    transaction_time: int | None
    symbol: str | None
    order_id: str | None
    client_order_id: str | None
    order_status: str | None
    execution_type: str | None
    side: str | None
    order_type: str | None
    position_side: str | None
    last_fill_qty: str | None
    cumulative_fill_qty: str | None
    last_fill_price: str | None
    average_price: str | None
    realized_pnl: str | None
    raw: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
