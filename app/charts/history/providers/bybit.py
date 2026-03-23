from __future__ import annotations

from app.charts.history.models import ChartHistoryRequest, ChartHistoryTimeframe, RawHistoryBar
from app.charts.history.providers.base import ChartHistoryProvider
from app.charts.history.providers.common import (
    decimal_or_none,
    http_get_json,
    last_closed_bar_open_time_ms,
    normalize_history_limit,
    timeframe_to_ms,
    trim_history_bars,
)
from app.charts.market_types import ChartInstrumentType


def _bybit_endpoint(market_type: str) -> tuple[str, str, str]:
    normalized_market_type = str(market_type or "").strip().lower()
    if normalized_market_type == ChartInstrumentType.SPOT.value:
        return ("https://api.bybit.com/v5/market/kline", "spot", "close")
    return ("https://api.bybit.com/v5/market/kline", "linear", "close")


def _parse_bybit_rows(
    rows: list[object],
    *,
    exchange: str,
    market_type: str,
    symbol: str,
    timeframe: ChartHistoryTimeframe,
    reference_price_kind: str,
) -> list[RawHistoryBar]:
    timeframe_ms = timeframe_to_ms(timeframe)
    out: list[RawHistoryBar] = []
    for item in rows:
        if not isinstance(item, list) or len(item) < 5:
            continue
        close_price = decimal_or_none(item[4])
        if close_price is None or close_price <= 0:
            continue
        try:
            open_time_ms = int(item[0])
        except Exception:
            continue
        out.append(
            RawHistoryBar(
                exchange=exchange,
                market_type=market_type,
                symbol=symbol,
                timeframe=timeframe,
                open_time_ms=open_time_ms,
                close_time_ms=open_time_ms + timeframe_ms - 1,
                close_price=close_price,
                reference_price_kind=reference_price_kind,
            )
        )
    out.sort(key=lambda bar: int(bar.open_time_ms))
    return out


class BybitChartHistoryProvider(ChartHistoryProvider):
    def load_history(self, request: ChartHistoryRequest) -> list[RawHistoryBar]:
        market_type = str(request.left_market_type or request.right_market_type or "").strip().lower()
        symbol = str(request.left_symbol or request.right_symbol or "").strip().upper()
        exchange = str(request.left_exchange or request.right_exchange or "bybit").strip().lower()
        endpoint, category, reference_price_kind = _bybit_endpoint(market_type)
        timeframe = request.timeframe
        timeframe_ms = timeframe_to_ms(timeframe)
        remaining = normalize_history_limit(request.limit)
        end_time_ms = int(request.end_time_ms) if request.end_time_ms is not None else last_closed_bar_open_time_ms(timeframe) + timeframe_ms - 1
        all_rows: list[RawHistoryBar] = []

        while remaining > 0:
            batch_limit = min(remaining, 1000)
            payload = http_get_json(
                endpoint,
                {
                    "category": category,
                    "symbol": symbol,
                    "interval": "1",
                    "limit": batch_limit,
                    "end": end_time_ms,
                },
            )
            result = payload.get("result", {}) if isinstance(payload, dict) else {}
            rows = result.get("list", []) if isinstance(result, dict) else []
            parsed_rows = _parse_bybit_rows(
                rows if isinstance(rows, list) else [],
                exchange=exchange,
                market_type=market_type,
                symbol=symbol,
                timeframe=timeframe,
                reference_price_kind=reference_price_kind,
            )
            if not parsed_rows:
                break
            all_rows = parsed_rows + all_rows
            remaining -= len(parsed_rows)
            oldest_open_time_ms = min(int(bar.open_time_ms) for bar in parsed_rows)
            next_end_time_ms = oldest_open_time_ms - 1
            if next_end_time_ms >= end_time_ms:
                break
            end_time_ms = next_end_time_ms
            if request.start_time_ms is not None and end_time_ms < int(request.start_time_ms):
                break
            if len(parsed_rows) < batch_limit:
                break

        if request.start_time_ms is not None:
            all_rows = [bar for bar in all_rows if int(bar.open_time_ms) >= int(request.start_time_ms)]
        if request.end_time_ms is not None:
            all_rows = [bar for bar in all_rows if int(bar.open_time_ms) <= int(request.end_time_ms)]
        return trim_history_bars(all_rows, request.limit)
