from __future__ import annotations


def parse_daily_volume_threshold(text: object) -> int | None:
    raw = str(text or "").strip()
    if not raw:
        return None
    sanitized = raw.replace("\u00a0", "").replace(" ", "")
    if not sanitized.isdigit():
        return None
    try:
        value = int(sanitized)
    except ValueError:
        return None
    return value if value > 0 else None


def format_volume_threshold(n: int | None) -> str:
    return str(int(n or 0)) if n is not None else "200"
