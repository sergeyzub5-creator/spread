from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(slots=True)
class PricePoint:
    timestamp_ms: int
    price: Decimal
    volume: Decimal | None = None


@dataclass(slots=True)
class PriceCandle:
    open_time_ms: int
    close_time_ms: int
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    volume: Decimal | None = None
