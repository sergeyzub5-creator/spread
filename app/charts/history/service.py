from __future__ import annotations

from collections.abc import Iterable
from decimal import Decimal

from app.charts.history.models import ChartHistoryRequest, ChartHistoryTimeframe, RawHistoryBar, SpreadHistoryPoint
from app.charts.history.providers import BinanceChartHistoryProvider, BybitChartHistoryProvider
from app.charts.history.providers.common import last_closed_bar_open_time_ms, timeframe_to_ms, trim_history_bars
from app.charts.history.storage import load_cached_history, save_cached_history


def default_history_limit(timeframe: ChartHistoryTimeframe) -> int:
    if timeframe == ChartHistoryTimeframe.M1:
        return 5000
    return 500


def infer_reference_price_kind(exchange: str, market_type: str) -> str:
    return "close"


def _bars_map(bars: Iterable[RawHistoryBar]) -> dict[int, RawHistoryBar]:
    return {int(bar.open_time_ms): bar for bar in bars}


def build_spread_history(
    left_bars: Iterable[RawHistoryBar],
    right_bars: Iterable[RawHistoryBar],
) -> list[SpreadHistoryPoint]:
    left_map = _bars_map(left_bars)
    right_map = _bars_map(right_bars)
    out: list[SpreadHistoryPoint] = []
    for open_time_ms in sorted(set(left_map) & set(right_map)):
        left_bar = left_map[open_time_ms]
        right_bar = right_map[open_time_ms]
        if left_bar.close_price <= 0 or right_bar.close_price <= 0:
            continue
        spread_pct = ((right_bar.close_price - left_bar.close_price) / left_bar.close_price) * Decimal("100")
        out.append(
            SpreadHistoryPoint(
                open_time_ms=open_time_ms,
                close_time_ms=min(int(left_bar.close_time_ms), int(right_bar.close_time_ms)),
                left_price=left_bar.close_price,
                right_price=right_bar.close_price,
                spread_pct=spread_pct,
            )
        )
    return out


_HISTORY_PROVIDERS = {
    "binance": BinanceChartHistoryProvider(),
    "bybit": BybitChartHistoryProvider(),
}


def _provider_for_exchange(exchange: str):
    return _HISTORY_PROVIDERS.get(str(exchange or "").strip().lower())


def _side_history_request(
    *,
    exchange: str,
    market_type: str,
    symbol: str,
    timeframe: ChartHistoryTimeframe,
    limit: int,
    start_time_ms: int | None,
    end_time_ms: int | None,
) -> ChartHistoryRequest:
    return ChartHistoryRequest(
        left_exchange=exchange,
        left_market_type=market_type,
        left_symbol=symbol,
        right_exchange=exchange,
        right_market_type=market_type,
        right_symbol=symbol,
        timeframe=timeframe,
        limit=limit,
        start_time_ms=start_time_ms,
        end_time_ms=end_time_ms,
    )


def _filter_history_window(
    bars: Iterable[RawHistoryBar],
    *,
    start_time_ms: int,
    end_time_ms: int,
    limit: int,
) -> list[RawHistoryBar]:
    filtered = [
        bar
        for bar in bars
        if int(bar.open_time_ms) >= int(start_time_ms) and int(bar.open_time_ms) <= int(end_time_ms)
    ]
    filtered.sort(key=lambda bar: int(bar.open_time_ms))
    return trim_history_bars(filtered, limit)


def _history_window(
    timeframe: ChartHistoryTimeframe,
    limit: int,
    *,
    start_time_ms: int | None = None,
    end_time_ms: int | None = None,
) -> tuple[int, int]:
    timeframe_ms = timeframe_to_ms(timeframe)
    normalized_limit = max(1, int(limit or default_history_limit(timeframe)))
    end_open_time_ms = (
        int(end_time_ms)
        if end_time_ms is not None
        else last_closed_bar_open_time_ms(timeframe)
    )
    start_open_time_ms = (
        int(start_time_ms)
        if start_time_ms is not None
        else end_open_time_ms - (normalized_limit - 1) * timeframe_ms
    )
    return (start_open_time_ms, end_open_time_ms)


def load_side_history(
    *,
    exchange: str,
    market_type: str,
    symbol: str,
    timeframe: ChartHistoryTimeframe = ChartHistoryTimeframe.M1,
    limit: int | None = None,
    start_time_ms: int | None = None,
    end_time_ms: int | None = None,
    force_refresh: bool = False,
) -> list[RawHistoryBar]:
    normalized_exchange = str(exchange or "").strip().lower()
    normalized_market_type = str(market_type or "").strip().lower()
    normalized_symbol = str(symbol or "").strip().upper()
    if not normalized_exchange or not normalized_market_type or not normalized_symbol:
        return []
    provider = _provider_for_exchange(normalized_exchange)
    if provider is None:
        return []

    normalized_limit = max(1, int(limit or default_history_limit(timeframe)))
    start_open_time_ms, end_open_time_ms = _history_window(
        timeframe,
        normalized_limit,
        start_time_ms=start_time_ms,
        end_time_ms=end_time_ms,
    )
    cached_bars = [] if force_refresh else load_cached_history(
        normalized_exchange,
        normalized_market_type,
        normalized_symbol,
        timeframe,
    )
    cached_window = _filter_history_window(
        cached_bars,
        start_time_ms=start_open_time_ms,
        end_time_ms=end_open_time_ms,
        limit=normalized_limit,
    )
    timeframe_ms = timeframe_to_ms(timeframe)
    if not force_refresh and cached_window:
        cached_start_open_time_ms = min(int(bar.open_time_ms) for bar in cached_window)
        cached_end_open_time_ms = max(int(bar.open_time_ms) for bar in cached_window)
        if cached_start_open_time_ms <= start_open_time_ms and cached_end_open_time_ms >= end_open_time_ms:
            return cached_window

    if force_refresh or not cached_window:
        missing_ranges: list[tuple[int, int]] = [(start_open_time_ms, end_open_time_ms)]
    else:
        cached_start_open_time_ms = min(int(bar.open_time_ms) for bar in cached_window)
        cached_end_open_time_ms = max(int(bar.open_time_ms) for bar in cached_window)
        missing_ranges = []
        if cached_start_open_time_ms > start_open_time_ms:
            missing_ranges.append((start_open_time_ms, cached_start_open_time_ms - timeframe_ms))
        if cached_end_open_time_ms < end_open_time_ms:
            missing_ranges.append((cached_end_open_time_ms + timeframe_ms, end_open_time_ms))

    if not missing_ranges:
        return cached_window

    fetched_bars: list[RawHistoryBar] = []
    for range_start_open_time_ms, range_end_open_time_ms in missing_ranges:
        if range_end_open_time_ms < range_start_open_time_ms:
            continue
        range_limit = ((range_end_open_time_ms - range_start_open_time_ms) // timeframe_ms) + 1
        fetched_bars.extend(
            provider.load_history(
                _side_history_request(
                    exchange=normalized_exchange,
                    market_type=normalized_market_type,
                    symbol=normalized_symbol,
                    timeframe=timeframe,
                    limit=range_limit,
                    start_time_ms=range_start_open_time_ms,
                    end_time_ms=range_end_open_time_ms + timeframe_ms - 1,
                )
            )
        )
    merged_bars = _filter_history_window(
        list(cached_bars) + list(fetched_bars),
        start_time_ms=start_open_time_ms,
        end_time_ms=end_open_time_ms,
        limit=normalized_limit,
    )
    if merged_bars:
        save_cached_history(
            normalized_exchange,
            normalized_market_type,
            normalized_symbol,
            timeframe,
            merged_bars,
        )
    return merged_bars


def load_spread_history(
    request: ChartHistoryRequest,
    *,
    force_refresh: bool = False,
) -> list[SpreadHistoryPoint]:
    limit = max(1, int(request.limit or default_history_limit(request.timeframe)))
    left_bars = load_side_history(
        exchange=request.left_exchange,
        market_type=request.left_market_type,
        symbol=request.left_symbol,
        timeframe=request.timeframe,
        limit=limit,
        start_time_ms=request.start_time_ms,
        end_time_ms=request.end_time_ms,
        force_refresh=force_refresh,
    )
    right_bars = load_side_history(
        exchange=request.right_exchange,
        market_type=request.right_market_type,
        symbol=request.right_symbol,
        timeframe=request.timeframe,
        limit=limit,
        start_time_ms=request.start_time_ms,
        end_time_ms=request.end_time_ms,
        force_refresh=force_refresh,
    )
    return build_spread_history(left_bars, right_bars)
