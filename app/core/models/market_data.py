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
        return {
            "instrument_id": {
                "exchange": self.instrument_id.exchange,
                "market_type": self.instrument_id.market_type,
                "symbol": self.instrument_id.symbol,
            },
            "bid": self.bid,
            "ask": self.ask,
            "bid_qty": self.bid_qty,
            "ask_qty": self.ask_qty,
            "ts_exchange": self.ts_exchange,
            "ts_local": self.ts_local,
            "source": self.source,
        }


@dataclass(frozen=True, slots=True)
class QuoteDepthLevel:
    price: Decimal
    quantity: Decimal


@dataclass(frozen=True, slots=True)
class QuoteDepth20:
    instrument_id: InstrumentId
    bids: tuple[QuoteDepthLevel, ...]
    asks: tuple[QuoteDepthLevel, ...]
    ts_local: int
    source: Literal["public_ws"]
    source_symbol: str | None = None
    snapshot_id: int = 0

    def to_dict(self) -> dict:
        return asdict(self)
