from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(slots=True)
class ExchangeCell:
    volume_usdt: int
    price: Decimal | None
    bid_price: Decimal | None = None
    ask_price: Decimal | None = None
    funding_rate_str: str | None = None
    next_funding_ms: int | None = None
    funding_interval_hours: int | None = None


def resolve_price(
    price: Decimal | None,
    bid_price: Decimal | None,
    ask_price: Decimal | None,
) -> Decimal | None:
    if bid_price is not None and ask_price is not None and bid_price > 0 and ask_price > 0:
        return (bid_price + ask_price) / Decimal("2")
    if price is not None and price > 0:
        return price
    return None


def select_low_high_exchange_ids(
    price_by_exchange: dict[str, Decimal],
    exchange_ids: list[str] | tuple[str, ...] | None = None,
) -> tuple[str, str] | None:
    candidates: list[tuple[str, Decimal]] = []
    if exchange_ids is None:
        iterator = price_by_exchange.items()
    else:
        iterator = ((exchange_id, price_by_exchange.get(exchange_id)) for exchange_id in exchange_ids)
    for exchange_id, price in iterator:
        if price is None or price <= 0:
            continue
        candidates.append((str(exchange_id), price))
    if len(candidates) < 2:
        return None
    low_exchange_id = min(candidates, key=lambda item: (item[1], item[0]))[0]
    high_exchange_id = max(candidates, key=lambda item: (item[1], item[0]))[0]
    if low_exchange_id == high_exchange_id:
        return None
    return low_exchange_id, high_exchange_id


def format_spread_pct(value: float | Decimal | None) -> str:
    if value is None:
        return "-"
    try:
        return f"{float(value):.2f}%"
    except Exception:
        return "-"


__all__ = [
    "ExchangeCell",
    "format_spread_pct",
    "resolve_price",
    "select_low_high_exchange_ids",
]
