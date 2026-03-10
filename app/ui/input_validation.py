from __future__ import annotations

from decimal import Decimal, InvalidOperation


def normalize_decimal_text(value: object, default: str = "0") -> str:
    text = str(value or "").strip()
    if not text:
        return default
    return text.replace(" ", "").replace(",", ".")


def parse_decimal_text(value: object, default: str = "0") -> Decimal:
    normalized = normalize_decimal_text(value, default=default)
    try:
        return Decimal(normalized)
    except (InvalidOperation, ValueError):
        return Decimal(default)
