from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum


class ChartHistoryTimeframe(StrEnum):
    M1 = "1m"


@dataclass(slots=True)
class RawHistoryBar:
    exchange: str
    market_type: str
    symbol: str
    timeframe: ChartHistoryTimeframe
    open_time_ms: int
    close_time_ms: int
    close_price: Decimal
    reference_price_kind: str


@dataclass(slots=True)
class SpreadHistoryPoint:
    open_time_ms: int
    close_time_ms: int
    left_price: Decimal
    right_price: Decimal
    spread_pct: Decimal


@dataclass(slots=True)
class ChartHistoryRequest:
    left_exchange: str
    left_market_type: str
    left_symbol: str
    right_exchange: str
    right_market_type: str
    right_symbol: str
    timeframe: ChartHistoryTimeframe = ChartHistoryTimeframe.M1
    limit: int = 1440
    start_time_ms: int | None = None
    end_time_ms: int | None = None
