from __future__ import annotations

from decimal import Decimal
from time import time

from PySide6.QtGui import QColor


def stringify_rate(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value, "f")


def normalize_cached_rate(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def normalize_cached_interval(value: object) -> int | None:
    try:
        result = int(str(value or "").strip())
    except Exception:
        return None
    return result if result > 0 else None


def normalize_cached_ms(value: object) -> int | None:
    try:
        result = int(str(value or "").strip())
    except Exception:
        return None
    return result if result > 0 else None


def funding_color(value: str | None) -> QColor:
    text = str(value or "").strip()
    if not text:
        return QColor("#8b91a1")
    try:
        rate = Decimal(text)
    except Exception:
        return QColor("#8b91a1")
    if rate > 0:
        return QColor("#2dd4bf")
    if rate < 0:
        return QColor("#ff5b6e")
    return QColor("#d5dae2")


def spread_color(value: str | None) -> QColor:
    text = "" if value is None else str(value).strip()
    if not text:
        return QColor("#8b91a1")
    try:
        spread = Decimal(text)
    except Exception:
        return QColor("#8b91a1")
    if spread > 0:
        return QColor("#2dd4bf")
    if spread < 0:
        return QColor("#ff5b6e")
    return QColor("#8b91a1")


def format_funding_rate(value: str | None) -> str:
    text = str(value or "").strip()
    if not text:
        return "-"
    try:
        rate = Decimal(text) * Decimal("100")
    except Exception:
        return "-"
    sign = "+" if rate > 0 else ""
    return f"{sign}{rate:.4f}%"


def format_spread_pct(value: str | None) -> str:
    text = "" if value is None else str(value).strip()
    if not text:
        return "-"
    try:
        spread = Decimal(text)
    except Exception:
        return "-"
    sign = "+" if spread > 0 else ""
    return f"{sign}{spread:.4f}%"


def now_ms() -> int:
    return int(time() * 1000)


def normalize_next_funding_ms(next_funding_ms: int | None, interval_hours: int | None) -> int | None:
    if next_funding_ms is None or next_funding_ms <= 0:
        return None
    if interval_hours is None or interval_hours <= 0:
        return next_funding_ms
    interval_ms = interval_hours * 3_600_000
    current_now_ms = now_ms()
    normalized = next_funding_ms
    while normalized < current_now_ms:
        normalized += interval_ms
    return normalized


def build_timer_text(next_funding_ms: int | None, interval_hours: int | None) -> str:
    normalized_next_ms = normalize_next_funding_ms(next_funding_ms, interval_hours)
    if normalized_next_ms is None:
        return f"({interval_hours}ч)" if interval_hours and interval_hours > 0 else "-"
    remaining_ms = max(0, normalized_next_ms - now_ms())
    total_seconds = remaining_ms // 1000
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    base = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    return f"{base} ({interval_hours}ч)" if interval_hours and interval_hours > 0 else base
