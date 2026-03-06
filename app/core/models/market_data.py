from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal
from typing import Literal

from app.core.models.instrument import InstrumentId


@dataclass(frozen=True, slots=True)
class QuoteL1:
    instrument_id: InstrumentId
    bid: Decimal
    ask: Decimal
    bid_qty: Decimal
    ask_qty: Decimal
    ts_exchange: int
    ts_local: int
    source: Literal["public_ws"]

    def to_dict(self) -> dict:
        return asdict(self)
