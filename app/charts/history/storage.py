from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

from app.charts.history.models import ChartHistoryTimeframe, RawHistoryBar


_HISTORY_ROOT = Path(__file__).resolve().parents[3] / "data" / "chart_history_close_v1"


def build_history_cache_key(
    exchange: str,
    market_type: str,
    symbol: str,
    timeframe: ChartHistoryTimeframe,
) -> str:
    return "|".join(
        [
            str(exchange or "").strip().lower(),
            str(market_type or "").strip().lower(),
            str(symbol or "").strip().upper(),
            str(timeframe.value),
        ]
    )


def build_history_cache_path(
    exchange: str,
    market_type: str,
    symbol: str,
    timeframe: ChartHistoryTimeframe,
) -> Path:
    normalized_exchange = str(exchange or "").strip().lower()
    normalized_market_type = str(market_type or "").strip().lower()
    normalized_symbol = str(symbol or "").strip().upper()
    return _HISTORY_ROOT / normalized_exchange / normalized_market_type / normalized_symbol / f"{timeframe.value}.json"


def load_cached_history(
    exchange: str,
    market_type: str,
    symbol: str,
    timeframe: ChartHistoryTimeframe,
) -> list[RawHistoryBar]:
    path = build_history_cache_path(exchange, market_type, symbol, timeframe)
    if not path.exists():
        return []
    try:
        with open(path, encoding="utf-8") as file:
            payload = json.load(file)
    except Exception:
        return []
    rows = payload if isinstance(payload, list) else []
    out: list[RawHistoryBar] = []
    for item in rows:
        if not isinstance(item, dict):
            continue
        try:
            out.append(
                RawHistoryBar(
                    exchange=str(item.get("exchange") or "").strip().lower(),
                    market_type=str(item.get("market_type") or "").strip().lower(),
                    symbol=str(item.get("symbol") or "").strip().upper(),
                    timeframe=ChartHistoryTimeframe(str(item.get("timeframe") or ChartHistoryTimeframe.M1.value)),
                    open_time_ms=int(item.get("open_time_ms") or 0),
                    close_time_ms=int(item.get("close_time_ms") or 0),
                    close_price=Decimal(str(item.get("close_price") or "0")),
                    reference_price_kind=str(item.get("reference_price_kind") or "").strip().lower(),
                )
            )
        except Exception:
            continue
    return out


def save_cached_history(
    exchange: str,
    market_type: str,
    symbol: str,
    timeframe: ChartHistoryTimeframe,
    bars: list[RawHistoryBar],
) -> None:
    path = build_history_cache_path(exchange, market_type, symbol, timeframe)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = [
        {
            "exchange": str(bar.exchange or "").strip().lower(),
            "market_type": str(bar.market_type or "").strip().lower(),
            "symbol": str(bar.symbol or "").strip().upper(),
            "timeframe": bar.timeframe.value,
            "open_time_ms": int(bar.open_time_ms),
            "close_time_ms": int(bar.close_time_ms),
            "close_price": str(bar.close_price),
            "reference_price_kind": str(bar.reference_price_kind or "").strip().lower(),
        }
        for bar in bars
    ]
    with open(path, "w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)
