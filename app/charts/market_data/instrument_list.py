from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from decimal import Decimal
import time
from typing import Any
from urllib.request import Request, urlopen

from app.charts.exchanges import normalize_chart_symbol


_BYBIT_LINEAR_TYPE_CACHE_LOCK = threading.Lock()
_BYBIT_LINEAR_TYPE_CACHE: dict[str, str] | None = None
_BINANCE_FUNDING_INTERVAL_CACHE_LOCK = threading.Lock()
_BINANCE_FUNDING_INTERVAL_CACHE: tuple[float, dict[str, int]] | None = None
_BINANCE_FUNDING_INTERVAL_CACHE_TTL_SECONDS = 900.0
_TRADABLE_SET_CACHE_LOCK = threading.Lock()
_TRADABLE_SET_CACHE: dict[tuple[str, str], tuple[float, set[str]]] = {}
_TRADABLE_SET_CACHE_TTL_SECONDS = 300.0


@dataclass(slots=True)
class ChartInstrumentEntry:
    symbol: str
    volume: int
    reference_price: Decimal | None = None
    funding_rate: Decimal | None = None
    funding_interval_hours: int | None = None
    next_funding_ms: int | None = None


@dataclass(slots=True)
class ChartMatchedInstrumentRow:
    symbol: str
    spread_pct: Decimal | None
    left_funding_rate: Decimal | None
    left_funding_interval_hours: int | None
    left_next_funding_ms: int | None
    right_funding_rate: Decimal | None
    right_funding_interval_hours: int | None
    right_next_funding_ms: int | None


@dataclass(slots=True)
class ChartFundingUpdate:
    funding_rate: Decimal | None
    funding_interval_hours: int | None
    next_funding_ms: int | None


@dataclass(slots=True)
class BinanceFundingSnapshot:
    funding_rate: Decimal
    next_funding_ms: int | None
    mark_price: Decimal | None


def _http_get_json(url: str, *, timeout: float = 20.0) -> Any:
    request = Request(url, headers={"User-Agent": "spread-sniper-ui-shell/1.0"})
    with urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def _int_volume(value: Any) -> int:
    try:
        result = int(Decimal(str(value or "0")))
    except Exception:
        return 0
    return max(0, result)


def _decimal_or_none(value: Any) -> Decimal | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return Decimal(text)
    except Exception:
        return None


def _interval_hours(value: Any) -> int | None:
    try:
        result = int(Decimal(str(value or "0")))
    except Exception:
        return None
    return result if result > 0 else None


def _get_cached_tradable_set(exchange_code: str, market_type: str) -> set[str] | None:
    key = (str(exchange_code or "").strip().lower(), str(market_type or "").strip().lower())
    now = time.monotonic()
    with _TRADABLE_SET_CACHE_LOCK:
        cached = _TRADABLE_SET_CACHE.get(key)
        if not cached or cached[0] <= now:
            return None
        return set(cached[1])


def _set_cached_tradable_set(exchange_code: str, market_type: str, symbols: set[str]) -> set[str]:
    key = (str(exchange_code or "").strip().lower(), str(market_type or "").strip().lower())
    normalized = {str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()}
    with _TRADABLE_SET_CACHE_LOCK:
        _TRADABLE_SET_CACHE[key] = (time.monotonic() + _TRADABLE_SET_CACHE_TTL_SECONDS, set(normalized))
    return normalized


def _fetch_binance_tradable_set(market_type: str) -> set[str]:
    cached = _get_cached_tradable_set("binance", market_type)
    if cached is not None:
        return cached
    payload = _http_get_json("https://fapi.binance.com/fapi/v1/exchangeInfo")
    symbols = payload.get("symbols") if isinstance(payload, dict) else None
    if not isinstance(symbols, list):
        return _set_cached_tradable_set("binance", market_type, set())
    out: set[str] = set()
    wanted_contract_types = (
        {"PERPETUAL"} if market_type == "perpetual" else {"CURRENT_QUARTER", "NEXT_QUARTER"}
    )
    for item in symbols:
        if not isinstance(item, dict):
            continue
        if str(item.get("status") or "").strip().upper() != "TRADING":
            continue
        contract_type = str(item.get("contractType") or "").strip().upper()
        if contract_type not in wanted_contract_types:
            continue
        raw_symbol = str(item.get("symbol") or "").strip().upper()
        symbol = normalize_chart_symbol("binance", market_type, raw_symbol)
        if symbol:
            out.add(symbol)
    return _set_cached_tradable_set("binance", market_type, out)


def _fetch_bybit_tradable_set(market_type: str) -> set[str]:
    cached = _get_cached_tradable_set("bybit", market_type)
    if cached is not None:
        return cached
    out: set[str] = set()
    cursor = ""
    base_url = "https://api.bybit.com/v5/market/instruments-info?category=linear&status=Trading&limit=1000"
    wanted_contract_type = "LinearPerpetual" if market_type == "perpetual" else "LinearFutures"
    for _ in range(50):
        payload = _http_get_json(base_url + (f"&cursor={cursor}" if cursor else ""))
        result = payload.get("result", {}) if isinstance(payload, dict) else {}
        rows = result.get("list", []) if isinstance(result, dict) else []
        if not isinstance(rows, list):
            break
        for item in rows:
            if not isinstance(item, dict):
                continue
            if str(item.get("contractType") or "").strip() != wanted_contract_type:
                continue
            raw_symbol = str(item.get("symbol") or "").strip().upper()
            symbol = normalize_chart_symbol("bybit", market_type, raw_symbol)
            if symbol:
                out.add(symbol)
        cursor = str(result.get("nextPageCursor") or "").strip()
        if not cursor:
            break
    return _set_cached_tradable_set("bybit", market_type, out)


def _fetch_binance_spot_symbols_with_volume() -> dict[str, ChartInstrumentEntry]:
    payload = _http_get_json("https://api.binance.com/api/v3/ticker/24hr")
    rows = payload if isinstance(payload, list) else []
    out: dict[str, ChartInstrumentEntry] = {}
    for item in rows:
        if not isinstance(item, dict):
            continue
        symbol = normalize_chart_symbol("binance", "spot", item.get("symbol"))
        if not symbol or not symbol.endswith("USDT"):
            continue
        out[symbol] = ChartInstrumentEntry(
            symbol=symbol,
            volume=_int_volume(item.get("quoteVolume")),
            reference_price=_decimal_or_none(item.get("lastPrice")),
        )
    return out


def _int_or_none(value: Any) -> int | None:
    try:
        return int(str(value or "").strip())
    except Exception:
        return None


def _fetch_binance_funding_snapshot_map() -> dict[str, BinanceFundingSnapshot]:
    payload = _http_get_json("https://fapi.binance.com/fapi/v1/premiumIndex")
    rows = payload if isinstance(payload, list) else []
    out: dict[str, BinanceFundingSnapshot] = {}
    for item in rows:
        if not isinstance(item, dict):
            continue
        symbol = normalize_chart_symbol("binance", "perpetual", item.get("symbol"))
        funding_rate = _decimal_or_none(item.get("lastFundingRate"))
        if symbol and funding_rate is not None:
            out[symbol] = BinanceFundingSnapshot(
                funding_rate=funding_rate,
                next_funding_ms=_int_or_none(item.get("nextFundingTime")),
                mark_price=_decimal_or_none(item.get("markPrice")),
            )
    return out


def _fetch_binance_funding_interval_map() -> dict[str, int]:
    global _BINANCE_FUNDING_INTERVAL_CACHE
    now = time.monotonic()
    with _BINANCE_FUNDING_INTERVAL_CACHE_LOCK:
        cached = _BINANCE_FUNDING_INTERVAL_CACHE
        if cached and cached[0] > now:
            return dict(cached[1])
    out: dict[str, int] = {}
    try:
        payload = _http_get_json("https://fapi.binance.com/fapi/v1/fundingInfo")
    except Exception:
        return out
    rows = payload if isinstance(payload, list) else []
    for item in rows:
        if not isinstance(item, dict):
            continue
        symbol = normalize_chart_symbol("binance", "perpetual", item.get("symbol"))
        interval_hours = _interval_hours(item.get("fundingIntervalHours"))
        if symbol and interval_hours is not None:
            out[symbol] = interval_hours
    with _BINANCE_FUNDING_INTERVAL_CACHE_LOCK:
        _BINANCE_FUNDING_INTERVAL_CACHE = (time.monotonic() + _BINANCE_FUNDING_INTERVAL_CACHE_TTL_SECONDS, dict(out))
    return out


def _fetch_binance_futures_symbols_with_volume(market_type: str) -> dict[str, ChartInstrumentEntry]:
    payload = _http_get_json("https://fapi.binance.com/fapi/v1/ticker/24hr")
    rows = payload if isinstance(payload, list) else []
    tradable_set = _fetch_binance_tradable_set(market_type)
    funding_map = _fetch_binance_funding_snapshot_map() if market_type == "perpetual" else {}
    funding_interval_map = _fetch_binance_funding_interval_map() if market_type == "perpetual" else {}
    out: dict[str, ChartInstrumentEntry] = {}
    for item in rows:
        if not isinstance(item, dict):
            continue
        raw_symbol = str(item.get("symbol") or "").strip().upper()
        is_delivery = "_" in raw_symbol
        if market_type == "perpetual" and is_delivery:
            continue
        if market_type == "futures" and not is_delivery:
            continue
        symbol = normalize_chart_symbol("binance", market_type, raw_symbol)
        if not symbol:
            continue
        if tradable_set and symbol not in tradable_set:
            continue
        funding_snapshot = funding_map.get(symbol)
        out[symbol] = ChartInstrumentEntry(
            symbol=symbol,
            volume=_int_volume(item.get("quoteVolume")),
            reference_price=(
                (funding_snapshot.mark_price if funding_snapshot else None) or _decimal_or_none(item.get("lastPrice"))
                if market_type == "perpetual"
                else _decimal_or_none(item.get("lastPrice"))
            ),
            funding_rate=funding_snapshot.funding_rate if funding_snapshot else None,
            funding_interval_hours=funding_interval_map.get(symbol),
            next_funding_ms=funding_snapshot.next_funding_ms if funding_snapshot else None,
        )
    return out


def _fetch_bybit_spot_symbols_with_volume() -> dict[str, ChartInstrumentEntry]:
    payload = _http_get_json("https://api.bybit.com/v5/market/tickers?category=spot")
    result = payload.get("result", {}) if isinstance(payload, dict) else {}
    rows = result.get("list", []) if isinstance(result, dict) else []
    out: dict[str, ChartInstrumentEntry] = {}
    for item in rows:
        if not isinstance(item, dict):
            continue
        symbol = normalize_chart_symbol("bybit", "spot", item.get("symbol"))
        if not symbol or not symbol.endswith("USDT"):
            continue
        out[symbol] = ChartInstrumentEntry(
            symbol=symbol,
            volume=_int_volume(item.get("turnover24h")),
            reference_price=_decimal_or_none(item.get("lastPrice")),
        )
    return out


def _load_bybit_linear_contract_types() -> dict[str, str]:
    global _BYBIT_LINEAR_TYPE_CACHE
    with _BYBIT_LINEAR_TYPE_CACHE_LOCK:
        if _BYBIT_LINEAR_TYPE_CACHE is not None:
            return dict(_BYBIT_LINEAR_TYPE_CACHE)

    contract_types: dict[str, str] = {}
    cursor = ""
    while True:
        url = "https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000"
        if cursor:
            url += f"&cursor={cursor}"
        payload = _http_get_json(url)
        result = payload.get("result", {}) if isinstance(payload, dict) else {}
        rows = result.get("list", []) if isinstance(result, dict) else []
        for item in rows:
            if not isinstance(item, dict):
                continue
            symbol = normalize_chart_symbol("bybit", "perpetual", item.get("symbol"))
            contract_type = str(item.get("contractType") or "").strip().upper()
            if symbol and contract_type:
                contract_types[symbol] = contract_type
        cursor = str(result.get("nextPageCursor") or "").strip()
        if not cursor:
            break

    with _BYBIT_LINEAR_TYPE_CACHE_LOCK:
        _BYBIT_LINEAR_TYPE_CACHE = dict(contract_types)
    return contract_types


def _fetch_bybit_linear_symbols_with_volume(market_type: str) -> dict[str, ChartInstrumentEntry]:
    type_map = _load_bybit_linear_contract_types()
    payload = _http_get_json("https://api.bybit.com/v5/market/tickers?category=linear")
    tradable_set = _fetch_bybit_tradable_set(market_type)
    result = payload.get("result", {}) if isinstance(payload, dict) else {}
    rows = result.get("list", []) if isinstance(result, dict) else []
    out: dict[str, ChartInstrumentEntry] = {}
    for item in rows:
        if not isinstance(item, dict):
            continue
        symbol = normalize_chart_symbol("bybit", market_type, item.get("symbol"))
        if not symbol:
            continue
        contract_type = str(type_map.get(symbol) or "").upper().replace("_", "")
        is_perpetual = "PERPETUAL" in contract_type
        is_delivery = "LINEARFUTURES" in contract_type or ("FUTURES" in contract_type and not is_perpetual)
        if market_type == "perpetual" and not is_perpetual:
            continue
        if market_type == "futures" and not is_delivery:
            continue
        if tradable_set and symbol not in tradable_set:
            continue
        next_funding_ms = _int_or_none(item.get("nextFundingTime")) if market_type == "perpetual" else None
        out[symbol] = ChartInstrumentEntry(
            symbol=symbol,
            volume=_int_volume(item.get("turnover24h")),
            reference_price=(
                _decimal_or_none(item.get("markPrice"))
                if market_type == "perpetual"
                else _decimal_or_none(item.get("lastPrice"))
            ),
            funding_rate=_decimal_or_none(item.get("fundingRate")) if market_type == "perpetual" else None,
            funding_interval_hours=_interval_hours(item.get("fundingIntervalHour")) if market_type == "perpetual" else None,
            next_funding_ms=next_funding_ms,
        )
    return out


def _fetch_symbol_volume_map(exchange_code: str, market_type: str) -> dict[str, ChartInstrumentEntry]:
    exchange = str(exchange_code or "").strip().lower()
    mtype = str(market_type or "").strip().lower()
    if exchange == "binance":
        if mtype == "spot":
            return _fetch_binance_spot_symbols_with_volume()
        if mtype in ("perpetual", "futures"):
            return _fetch_binance_futures_symbols_with_volume(mtype)
    if exchange == "bybit":
        if mtype == "spot":
            return _fetch_bybit_spot_symbols_with_volume()
        if mtype in ("perpetual", "futures"):
            return _fetch_bybit_linear_symbols_with_volume(mtype)
    return {}


def _fetch_binance_spot_price_map() -> dict[str, Decimal | None]:
    payload = _http_get_json("https://api.binance.com/api/v3/ticker/24hr")
    rows = payload if isinstance(payload, list) else []
    out: dict[str, Decimal | None] = {}
    for item in rows:
        if not isinstance(item, dict):
            continue
        symbol = normalize_chart_symbol("binance", "spot", item.get("symbol"))
        if symbol:
            out[symbol] = _decimal_or_none(item.get("lastPrice"))
    return out


def _fetch_binance_futures_price_map(market_type: str) -> dict[str, Decimal | None]:
    payload = _http_get_json("https://fapi.binance.com/fapi/v1/ticker/24hr")
    rows = payload if isinstance(payload, list) else []
    tradable_set = _fetch_binance_tradable_set(market_type)
    out: dict[str, Decimal | None] = {}
    for item in rows:
        if not isinstance(item, dict):
            continue
        raw_symbol = str(item.get("symbol") or "").strip().upper()
        is_delivery = "_" in raw_symbol
        if market_type == "perpetual" and is_delivery:
            continue
        if market_type == "futures" and not is_delivery:
            continue
        symbol = normalize_chart_symbol("binance", market_type, raw_symbol)
        if symbol and (not tradable_set or symbol in tradable_set):
            out[symbol] = _decimal_or_none(item.get("lastPrice"))
    if market_type == "perpetual":
        premium_rows = _http_get_json("https://fapi.binance.com/fapi/v1/premiumIndex")
        premium_list = premium_rows if isinstance(premium_rows, list) else []
        for item in premium_list:
            if not isinstance(item, dict):
                continue
            symbol = normalize_chart_symbol("binance", "perpetual", item.get("symbol"))
            mark_price = _decimal_or_none(item.get("markPrice"))
            if symbol and mark_price is not None and (not tradable_set or symbol in tradable_set):
                out[symbol] = mark_price
    return out


def _fetch_bybit_spot_price_map() -> dict[str, Decimal | None]:
    payload = _http_get_json("https://api.bybit.com/v5/market/tickers?category=spot")
    result = payload.get("result", {}) if isinstance(payload, dict) else {}
    rows = result.get("list", []) if isinstance(result, dict) else []
    out: dict[str, Decimal | None] = {}
    for item in rows:
        if not isinstance(item, dict):
            continue
        symbol = normalize_chart_symbol("bybit", "spot", item.get("symbol"))
        if symbol:
            out[symbol] = _decimal_or_none(item.get("lastPrice"))
    return out


def _fetch_bybit_linear_price_map(market_type: str) -> dict[str, Decimal | None]:
    type_map = _load_bybit_linear_contract_types()
    payload = _http_get_json("https://api.bybit.com/v5/market/tickers?category=linear")
    tradable_set = _fetch_bybit_tradable_set(market_type)
    result = payload.get("result", {}) if isinstance(payload, dict) else {}
    rows = result.get("list", []) if isinstance(result, dict) else []
    out: dict[str, Decimal | None] = {}
    for item in rows:
        if not isinstance(item, dict):
            continue
        raw_symbol = str(item.get("symbol") or "").strip().upper()
        symbol = normalize_chart_symbol("bybit", market_type, raw_symbol)
        if not symbol:
            continue
        contract_type = str(type_map.get(symbol) or "").upper().replace("_", "")
        is_perpetual = "PERPETUAL" in contract_type
        is_delivery = "LINEARFUTURES" in contract_type or ("FUTURES" in contract_type and not is_perpetual)
        if market_type == "perpetual" and not is_perpetual:
            continue
        if market_type == "futures" and not is_delivery:
            continue
        if tradable_set and symbol not in tradable_set:
            continue
        out[symbol] = (
            _decimal_or_none(item.get("markPrice"))
            if market_type == "perpetual"
            else _decimal_or_none(item.get("lastPrice"))
        )
    return out


def _fetch_symbol_price_map(exchange_code: str, market_type: str) -> dict[str, Decimal | None]:
    exchange = str(exchange_code or "").strip().lower()
    mtype = str(market_type or "").strip().lower()
    if exchange == "binance":
        if mtype == "spot":
            return _fetch_binance_spot_price_map()
        if mtype in ("perpetual", "futures"):
            return _fetch_binance_futures_price_map(mtype)
    if exchange == "bybit":
        if mtype == "spot":
            return _fetch_bybit_spot_price_map()
        if mtype in ("perpetual", "futures"):
            return _fetch_bybit_linear_price_map(mtype)
    return {}


def _fetch_binance_funding_update_map(market_type: str) -> dict[str, ChartFundingUpdate]:
    if market_type != "perpetual":
        return {}
    funding_map = _fetch_binance_funding_snapshot_map()
    interval_map = _fetch_binance_funding_interval_map()
    out: dict[str, ChartFundingUpdate] = {}
    for symbol, snapshot in funding_map.items():
        out[symbol] = ChartFundingUpdate(
            funding_rate=snapshot.funding_rate,
            funding_interval_hours=interval_map.get(symbol),
            next_funding_ms=snapshot.next_funding_ms,
        )
    return out


def _fetch_bybit_funding_update_map(market_type: str) -> dict[str, ChartFundingUpdate]:
    if market_type != "perpetual":
        return {}
    payload = _http_get_json("https://api.bybit.com/v5/market/tickers?category=linear")
    result = payload.get("result", {}) if isinstance(payload, dict) else {}
    rows = result.get("list", []) if isinstance(result, dict) else []
    out: dict[str, ChartFundingUpdate] = {}
    type_map = _load_bybit_linear_contract_types()
    for item in rows:
        if not isinstance(item, dict):
            continue
        symbol = normalize_chart_symbol("bybit", "perpetual", item.get("symbol"))
        if not symbol:
            continue
        contract_type = str(type_map.get(symbol) or "").upper().replace("_", "")
        if "PERPETUAL" not in contract_type:
            continue
        out[symbol] = ChartFundingUpdate(
            funding_rate=_decimal_or_none(item.get("fundingRate")),
            funding_interval_hours=_interval_hours(item.get("fundingIntervalHour")),
            next_funding_ms=_int_or_none(item.get("nextFundingTime")),
        )
    return out


def _fetch_symbol_funding_map(exchange_code: str, market_type: str) -> dict[str, ChartFundingUpdate]:
    exchange = str(exchange_code or "").strip().lower()
    mtype = str(market_type or "").strip().lower()
    if exchange == "binance":
        return _fetch_binance_funding_update_map(mtype)
    if exchange == "bybit":
        return _fetch_bybit_funding_update_map(mtype)
    return {}


def _compute_spread_pct(left_entry: ChartInstrumentEntry, right_entry: ChartInstrumentEntry) -> Decimal | None:
    left_price = left_entry.reference_price
    right_price = right_entry.reference_price
    if left_price is None or right_price is None or left_price <= 0:
        return None
    return ((right_price - left_price) / left_price) * Decimal("100")


def _has_real_trading(entry: ChartInstrumentEntry) -> bool:
    if int(entry.volume or 0) <= 0:
        return False
    reference_price = entry.reference_price
    if reference_price is None:
        return False
    return reference_price > 0


def load_matched_instrument_rows(
    *,
    left_exchange: str,
    left_market_type: str,
    right_exchange: str,
    right_market_type: str,
    volume_threshold: int | None,
) -> list[ChartMatchedInstrumentRow]:
    left_map = _fetch_symbol_volume_map(left_exchange, left_market_type)
    right_map = _fetch_symbol_volume_map(right_exchange, right_market_type)
    common_symbols = sorted(set(left_map).intersection(right_map))
    threshold = int(volume_threshold or 0)
    rows: list[ChartMatchedInstrumentRow] = []
    for symbol in common_symbols:
        left_entry = left_map.get(symbol)
        right_entry = right_map.get(symbol)
        if left_entry is None or right_entry is None:
            continue
        if not _has_real_trading(left_entry) or not _has_real_trading(right_entry):
            continue
        if threshold > 0 and left_entry.volume < threshold and right_entry.volume < threshold:
            continue
        rows.append(
            ChartMatchedInstrumentRow(
                symbol=symbol,
                spread_pct=_compute_spread_pct(left_entry, right_entry),
                left_funding_rate=left_entry.funding_rate,
                left_funding_interval_hours=left_entry.funding_interval_hours,
                left_next_funding_ms=left_entry.next_funding_ms,
                right_funding_rate=right_entry.funding_rate,
                right_funding_interval_hours=right_entry.funding_interval_hours,
                right_next_funding_ms=right_entry.next_funding_ms,
            )
        )
    return rows


def load_price_spread_updates(
    *,
    left_exchange: str,
    left_market_type: str,
    right_exchange: str,
    right_market_type: str,
    symbols: list[str],
) -> dict[str, Decimal | None]:
    symbol_set = {str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()}
    if not symbol_set:
        return {}
    left_map = _fetch_symbol_price_map(left_exchange, left_market_type)
    right_map = _fetch_symbol_price_map(right_exchange, right_market_type)
    out: dict[str, Decimal | None] = {}
    for symbol in symbol_set:
        left_price = left_map.get(symbol)
        right_price = right_map.get(symbol)
        if left_price is None or right_price is None:
            out[symbol] = None
            continue
        left_entry = ChartInstrumentEntry(symbol=symbol, volume=0, reference_price=left_price)
        right_entry = ChartInstrumentEntry(symbol=symbol, volume=0, reference_price=right_price)
        out[symbol] = _compute_spread_pct(left_entry, right_entry)
    return out


def load_funding_updates(
    *,
    exchange: str,
    market_type: str,
    symbols: list[str],
) -> dict[str, ChartFundingUpdate]:
    symbol_set = {str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()}
    if not symbol_set:
        return {}
    source = _fetch_symbol_funding_map(exchange, market_type)
    return {symbol: update for symbol, update in source.items() if symbol in symbol_set}


def load_matched_instruments(
    *,
    left_exchange: str,
    left_market_type: str,
    right_exchange: str,
    right_market_type: str,
    volume_threshold: int | None,
) -> list[str]:
    return [
        row.symbol
        for row in load_matched_instrument_rows(
            left_exchange=left_exchange,
            left_market_type=left_market_type,
            right_exchange=right_exchange,
            right_market_type=right_market_type,
            volume_threshold=volume_threshold,
        )
    ]
