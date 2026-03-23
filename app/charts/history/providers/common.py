from __future__ import annotations

import json
import time
from decimal import Decimal
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from app.charts.history.models import ChartHistoryTimeframe, RawHistoryBar


def timeframe_to_ms(timeframe: ChartHistoryTimeframe) -> int:
    if timeframe == ChartHistoryTimeframe.M1:
        return 60_000
    raise ValueError(f"Unsupported timeframe: {timeframe}")


def now_ms() -> int:
    return int(time.time() * 1000)


def last_closed_bar_open_time_ms(timeframe: ChartHistoryTimeframe) -> int:
    timeframe_ms = timeframe_to_ms(timeframe)
    current_open_time_ms = (now_ms() // timeframe_ms) * timeframe_ms
    return current_open_time_ms - timeframe_ms


def normalize_history_limit(limit: int, *, minimum: int = 1, maximum: int = 10_000) -> int:
    return max(minimum, min(int(limit or minimum), maximum))


def decimal_or_none(value: Any) -> Decimal | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return Decimal(text)
    except Exception:
        return None


def http_get_json(url: str, params: dict[str, object] | None = None, *, timeout: float = 20.0) -> Any:
    query = urlencode({key: value for key, value in (params or {}).items() if value is not None})
    full_url = f"{url}?{query}" if query else url
    request = Request(full_url, headers={"User-Agent": "spread-sniper-ui-shell/1.0"})
    with urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def merge_history_bars(existing: list[RawHistoryBar], incoming: list[RawHistoryBar]) -> list[RawHistoryBar]:
    merged: dict[int, RawHistoryBar] = {}
    for bar in existing:
        merged[int(bar.open_time_ms)] = bar
    for bar in incoming:
        merged[int(bar.open_time_ms)] = bar
    return [merged[key] for key in sorted(merged)]


def trim_history_bars(bars: list[RawHistoryBar], limit: int) -> list[RawHistoryBar]:
    normalized_limit = normalize_history_limit(limit)
    if len(bars) <= normalized_limit:
        return list(bars)
    return list(bars[-normalized_limit:])
