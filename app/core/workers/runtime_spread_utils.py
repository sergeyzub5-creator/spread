from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from app.core.models.market_data import QuoteL1


@dataclass(frozen=True, slots=True)
class SpreadEdgeResult:
    edge_1: Decimal | None
    edge_2: Decimal | None
    best_edge: Decimal | None
    direction: str | None
    left_action: str | None
    right_action: str | None


def safe_edge(numerator_left: Decimal, denominator_right: Decimal) -> Decimal | None:
    if denominator_right <= Decimal("0"):
        return None
    return (numerator_left - denominator_right) / denominator_right


def format_edge(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return f"{value:.6f}"


def mid_spread_ratio(left_quote: QuoteL1 | None, right_quote: QuoteL1 | None) -> Decimal | None:
    """
    Magnitude of spread using mids only — same scale as safe_edge(book) for threshold compare.
    Uses (left_mid - right_mid) / right_mid; returns abs() so |mid| crosses same entry_threshold line.
    """
    if left_quote is None or right_quote is None:
        return None
    try:
        left_mid = (left_quote.bid + left_quote.ask) / Decimal("2")
        right_mid = (right_quote.bid + right_quote.ask) / Decimal("2")
    except Exception:
        return None
    if right_mid <= Decimal("0"):
        return None
    return abs((left_mid - right_mid) / right_mid)


def calculate_spread_edges(left_quote: QuoteL1 | None, right_quote: QuoteL1 | None) -> SpreadEdgeResult:
    if left_quote is None or right_quote is None:
        return SpreadEdgeResult(None, None, None, None, None, None)
    edge_1 = safe_edge(left_quote.bid, right_quote.ask)
    edge_2 = safe_edge(right_quote.bid, left_quote.ask)
    if edge_1 is None and edge_2 is None:
        return SpreadEdgeResult(None, None, None, None, None, None)
    # Strategy uses opportunity magnitude (abs spread) as trigger;
    # sign only indicates which leg is expensive/cheap right now.
    if edge_1 is not None and (edge_2 is None or abs(edge_1) >= abs(edge_2)):
        return SpreadEdgeResult(edge_1, edge_2, edge_1, "EDGE_1", "SELL", "BUY")
    return SpreadEdgeResult(edge_1, edge_2, edge_2, "EDGE_2", "BUY", "SELL")
