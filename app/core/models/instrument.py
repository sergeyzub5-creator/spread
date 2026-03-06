from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal


@dataclass(frozen=True, slots=True)
class InstrumentKey:
    exchange: str
    market_type: str
    symbol: str


@dataclass(frozen=True, slots=True)
class InstrumentSpec:
    base_asset: str
    quote_asset: str
    contract_type: str
    settle_asset: str
    price_precision: Decimal
    qty_precision: Decimal
    min_qty: Decimal
    min_notional: Decimal


@dataclass(frozen=True, slots=True)
class InstrumentRouting:
    ws_channel: str
    ws_symbol: str
    order_route: str


@dataclass(frozen=True, slots=True)
class InstrumentId:
    key: InstrumentKey
    spec: InstrumentSpec
    routing: InstrumentRouting

    @property
    def exchange(self) -> str:
        return self.key.exchange

    @property
    def market_type(self) -> str:
        return self.key.market_type

    @property
    def symbol(self) -> str:
        return self.key.symbol

    def to_dict(self) -> dict:
        return asdict(self)
