from __future__ import annotations

import time
from decimal import Decimal


def funding_rate_to_percent_signed(rate_str: str | None) -> str:
    if not rate_str:
        return "—"
    try:
        d = Decimal(str(rate_str)) * Decimal("100")
        sign = "+" if d >= 0 else ""
        return f"{sign}{d:.4f}%"
    except Exception:
        return str(rate_str)


def ms_until_next_funding(next_ms: int | None, now_ms: int | None = None) -> int | None:
    if next_ms is None:
        return None
    now = now_ms if now_ms is not None else int(time.time() * 1000)
    return max(0, next_ms - now)


def format_countdown(remaining_ms: int | None) -> str:
    if remaining_ms is None:
        return "—"
    sec = remaining_ms // 1000
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


__all__ = ["format_countdown", "funding_rate_to_percent_signed", "ms_until_next_funding"]
