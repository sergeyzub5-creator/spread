from __future__ import annotations

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from threading import Lock
from urllib.request import Request, urlopen

from app.futures_spread_scanner_v2.common.logger import get_logger
from app.futures_spread_scanner_v2.runtime.endpoint_registry import get_endpoint_spec
from app.futures_spread_scanner_v2.runtime.market_helpers import ExchangeCell, resolve_price

_LOGGER = get_logger("scanner.v2.market_backend")
SUPPORTED_EXCHANGE_IDS = ("binance", "bybit")

_BASE_URLS: dict[str, str] = {
    "binance_api": "https://api.binance.com",
    "binance_fapi": "https://fapi.binance.com",
    "binance_dapi": "https://dapi.binance.com",
    "binance_papi": "https://papi.binance.com",
    "bybit": "https://api.bybit.com",
}

_TRADABLE_CACHE_TTL_SECONDS = 300.0
_PRICE_POLL_INTERVAL_SECONDS = {"binance": 2.0, "bybit": 2.0}
_BINANCE_INTERVAL_CACHE_TTL_SECONDS = 900.0
_BYBIT_INTERVAL_CACHE_TTL_SECONDS = 900.0

_TRADABLE_CACHE_LOCK = Lock()
_TRADABLE_CACHE: tuple[float, dict[str, set[str]]] | None = None
_BINANCE_INTERVAL_CACHE_LOCK = Lock()
_BINANCE_INTERVAL_CACHE: tuple[float, dict[str, int]] | None = None
_BYBIT_INTERVAL_CACHE_LOCK = Lock()
_BYBIT_INTERVAL_CACHE: tuple[float, dict[str, int]] | None = None
_PRICE_CACHE_LOCK = Lock()
_PRICE_CACHE: dict[str, dict[str, ExchangeCell]] = {exchange_id: {} for exchange_id in SUPPORTED_EXCHANGE_IDS}
_PRICE_CACHE_FETCHED_AT: dict[str, float] = {exchange_id: 0.0 for exchange_id in SUPPORTED_EXCHANGE_IDS}
_PRICE_CACHE_ERRORS: dict[str, str | None] = {exchange_id: None for exchange_id in SUPPORTED_EXCHANGE_IDS}
_PRICE_CACHE_IN_FLIGHT: set[str] = set()


def _http_get_json(url: str, *, timeout: float = 25.0):
    request = Request(url, headers={"User-Agent": "spread-sniper-ui-shell/1.0"})
    with urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def _base_url_for(exchange_id: str, path: str) -> str:
    normalized = str(exchange_id or "").strip().lower()
    if normalized == "binance":
        if path.startswith("/fapi/"):
            return _BASE_URLS["binance_fapi"]
        if path.startswith("/dapi/"):
            return _BASE_URLS["binance_dapi"]
        if path.startswith("/papi/"):
            return _BASE_URLS["binance_papi"]
        return _BASE_URLS["binance_api"]
    if normalized == "bybit":
        return _BASE_URLS["bybit"]
    raise KeyError(f"unknown exchange base url: {exchange_id}")


def scanner_endpoint_url(exchange_id: str, endpoint_key: str, **params: object) -> str:
    spec = get_endpoint_spec(exchange_id, endpoint_key)
    if spec is None:
        raise KeyError(f"unknown endpoint: {exchange_id}:{endpoint_key}")
    path = spec.path
    formatted_params: dict[str, str] = {}
    for key, value in params.items():
        if value is None:
            continue
        text = str(value)
        placeholder = "{" + key + "}"
        if placeholder in path:
            path = path.replace(placeholder, text)
        else:
            formatted_params[key] = text
    from urllib.parse import urlencode

    base = _base_url_for(exchange_id, path)
    query = urlencode(formatted_params)
    return f"{base}{path}" + (f"?{query}" if query else "")


def _dec(v) -> Decimal | None:
    if v is None:
        return None
    try:
        d = Decimal(str(v))
        return d if d > 0 else None
    except Exception:
        return None


def _fmt_rate(v) -> str | None:
    if v is None:
        return None
    try:
        return format(Decimal(str(v)), "f")
    except Exception:
        return str(v) if v else None


def _int_or_none(v) -> int | None:
    try:
        return int(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _interval_hours(v) -> int | None:
    if v is None:
        return None
    s = str(v).strip().rstrip("hH")
    try:
        hours = int(s)
    except (TypeError, ValueError):
        return None
    return hours if hours > 0 else None


def from_exchange(exchange_id: str, raw: str) -> str | None:
    s = str(raw or "").strip().upper()
    normalized = str(exchange_id or "").strip().lower()
    if normalized == "binance":
        if not s.endswith("USDT") or "_" in s:
            return None
        return s if s.isalnum() else None
    if normalized == "bybit":
        if not s.endswith("USDT"):
            return None
        return s if s.isalnum() else None
    return None


def volume_usdt_for_exchange(exchange_id: str, item: dict) -> int | None:
    normalized = str(exchange_id or "").strip().lower()
    try:
        if normalized == "binance":
            qv = item.get("quoteVolume")
            if qv is None:
                return None
            d = Decimal(str(qv))
            return int(d) if d >= 0 else None
        if normalized == "bybit":
            tv = item.get("turnover24h")
            if tv is None:
                return None
            d = Decimal(str(tv))
            return int(d) if d >= 0 else None
    except Exception:
        return None
    return None


def _mark_price_binance(item: dict) -> Decimal | None:
    return _dec(item.get("markPrice"))


def _price_bybit(item: dict) -> Decimal | None:
    return _dec(item.get("lastPrice"))


def _bid_bybit(item: dict) -> Decimal | None:
    return _dec(item.get("bid1Price"))


def _ask_bybit(item: dict) -> Decimal | None:
    return _dec(item.get("ask1Price"))


def _binance_interval_hours_by_symbol(timeout: float) -> dict[str, int]:
    global _BINANCE_INTERVAL_CACHE
    now = time.monotonic()
    with _BINANCE_INTERVAL_CACHE_LOCK:
        cached = _BINANCE_INTERVAL_CACHE
        if cached and cached[0] > now:
            return dict(cached[1])
    out: dict[str, int] = {}
    try:
        payload = _http_get_json(scanner_endpoint_url("binance", "usdm_funding_info"), timeout=timeout)
    except Exception:
        with _BINANCE_INTERVAL_CACHE_LOCK:
            cached = _BINANCE_INTERVAL_CACHE
            return dict(cached[1]) if cached else out
    if not isinstance(payload, list):
        return out
    for item in payload:
        if not isinstance(item, dict):
            continue
        sym = str(item.get("symbol") or "").strip().upper()
        interval_value = _interval_hours(item.get("fundingIntervalHours"))
        if sym and interval_value is not None:
            out[sym] = interval_value
    with _BINANCE_INTERVAL_CACHE_LOCK:
        _BINANCE_INTERVAL_CACHE = (time.monotonic() + _BINANCE_INTERVAL_CACHE_TTL_SECONDS, dict(out))
    return out


def _bybit_interval_hours_by_symbol(timeout: float) -> dict[str, int]:
    global _BYBIT_INTERVAL_CACHE
    now = time.monotonic()
    with _BYBIT_INTERVAL_CACHE_LOCK:
        cached = _BYBIT_INTERVAL_CACHE
        if cached and cached[0] > now:
            return dict(cached[1])
    out: dict[str, int] = {}
    cursor = ""
    base = scanner_endpoint_url("bybit", "market_instruments_info", category="linear", status="Trading", limit=1000)
    for _ in range(50):
        url = base + (f"&cursor={cursor}" if cursor else "")
        try:
            payload = _http_get_json(url, timeout=timeout)
        except Exception:
            break
        if not isinstance(payload, dict) or payload.get("retCode") != 0:
            break
        result = payload.get("result") or {}
        items = result.get("list")
        if not isinstance(items, list):
            break
        for item in items:
            if not isinstance(item, dict):
                continue
            symbol = str(item.get("symbol") or "").strip().upper()
            minutes = _int_or_none(item.get("fundingInterval"))
            if not symbol or minutes is None or minutes <= 0 or minutes % 60 != 0:
                continue
            out[symbol] = minutes // 60
        cursor = str(result.get("nextPageCursor") or "").strip()
        if not cursor:
            break
    with _BYBIT_INTERVAL_CACHE_LOCK:
        _BYBIT_INTERVAL_CACHE = (time.monotonic() + _BYBIT_INTERVAL_CACHE_TTL_SECONDS, dict(out))
    return out


def _bybit_funding_interval_hours(item: dict, interval_hours_by_symbol: dict[str, int] | None = None) -> int | None:
    direct_hours = _interval_hours(item.get("fundingIntervalHour"))
    if direct_hours is not None and direct_hours > 0:
        return direct_hours
    symbol = str(item.get("symbol") or "").strip().upper()
    if interval_hours_by_symbol and symbol:
        hours = interval_hours_by_symbol.get(symbol)
        if hours is not None and hours > 0:
            return hours
    return None


def _bybit_next_funding_ms(item: dict) -> int | None:
    next_ms = _int_or_none(item.get("nextFundingTime"))
    return next_ms if next_ms and next_ms > 0 else None


def fetch_binance_tradable_usdt_perpetual_canonical(timeout: float = 30.0) -> set[str]:
    url = scanner_endpoint_url("binance", "usdm_exchange_info")
    try:
        payload = _http_get_json(url, timeout=timeout)
    except Exception:
        return set()
    syms = payload.get("symbols") if isinstance(payload, dict) else None
    if not isinstance(syms, list):
        return set()
    out: set[str] = set()
    for item in syms:
        if not isinstance(item, dict):
            continue
        if str(item.get("status") or "") != "TRADING":
            continue
        contract_type = str(item.get("contractType") or "").strip().upper()
        if contract_type not in {"PERPETUAL", "TRADIFI_PERPETUAL"}:
            continue
        raw = str(item.get("symbol") or "").strip().upper()
        canonical = from_exchange("binance", raw)
        if canonical:
            out.add(canonical)
    return out


def fetch_bybit_tradable_linear_perpetual_canonical(timeout: float = 35.0) -> set[str]:
    out: set[str] = set()
    cursor = ""
    base = scanner_endpoint_url("bybit", "market_instruments_info", category="linear", status="Trading", limit=1000)
    for _ in range(50):
        url = base + (f"&cursor={cursor}" if cursor else "")
        try:
            payload = _http_get_json(url, timeout=timeout)
        except Exception:
            break
        if not isinstance(payload, dict) or payload.get("retCode") != 0:
            break
        result = payload.get("result") or {}
        items = result.get("list")
        if not isinstance(items, list):
            break
        for item in items:
            if not isinstance(item, dict):
                continue
            if str(item.get("contractType") or "") != "LinearPerpetual":
                continue
            raw = str(item.get("symbol") or "").strip().upper()
            canonical = from_exchange("bybit", raw)
            if canonical:
                out.add(canonical)
        cursor = str(result.get("nextPageCursor") or "").strip()
        if not cursor:
            break
    return out


def _fetch_binance_full_snapshot(timeout: float = 30.0, *, tradable: set[str] | None = None) -> dict[str, ExchangeCell]:
    payload_24h = _http_get_json(scanner_endpoint_url("binance", "usdm_24hr_ticker"), timeout=timeout)
    payload_premium = _http_get_json(scanner_endpoint_url("binance", "usdm_premium_index"), timeout=timeout)
    interval_hours_by_symbol = _binance_interval_hours_by_symbol(timeout)
    if not isinstance(payload_24h, list) or not isinstance(payload_premium, list):
        return {}
    out: dict[str, ExchangeCell] = {}
    for item in payload_24h:
        if not isinstance(item, dict):
            continue
        raw = str(item.get("symbol") or "").strip().upper()
        canonical = from_exchange("binance", raw)
        if not canonical or (tradable is not None and canonical not in tradable):
            continue
        volume = volume_usdt_for_exchange("binance", item)
        if volume is None or volume <= 0:
            continue
        out[canonical] = ExchangeCell(volume_usdt=int(volume), price=None, bid_price=None, ask_price=None)
    for item in payload_premium:
        if not isinstance(item, dict):
            continue
        raw = str(item.get("symbol") or "").strip().upper()
        canonical = from_exchange("binance", raw)
        if not canonical or canonical not in out:
            continue
        cell = out[canonical]
        cell.price = _mark_price_binance(item)
        cell.funding_rate_str = _fmt_rate(item.get("lastFundingRate"))
        cell.next_funding_ms = _int_or_none(item.get("nextFundingTime"))
        cell.funding_interval_hours = interval_hours_by_symbol.get(raw)
    return {canonical: cell for canonical, cell in out.items() if cell.price is not None}


def _fetch_binance_price_snapshot(timeout: float = 12.0, *, tradable: set[str] | None = None) -> dict[str, ExchangeCell]:
    payload_premium = _http_get_json(scanner_endpoint_url("binance", "usdm_premium_index"), timeout=timeout)
    interval_hours_by_symbol = _binance_interval_hours_by_symbol(timeout)
    if not isinstance(payload_premium, list):
        return {}
    out: dict[str, ExchangeCell] = {}
    for item in payload_premium:
        if not isinstance(item, dict):
            continue
        raw = str(item.get("symbol") or "").strip().upper()
        canonical = from_exchange("binance", raw)
        if not canonical or (tradable is not None and canonical not in tradable):
            continue
        mark_price = _mark_price_binance(item)
        if mark_price is None or mark_price <= 0:
            continue
        out[canonical] = ExchangeCell(
            volume_usdt=0,
            price=mark_price,
            bid_price=None,
            ask_price=None,
            funding_rate_str=_fmt_rate(item.get("lastFundingRate")),
            next_funding_ms=_int_or_none(item.get("nextFundingTime")),
            funding_interval_hours=interval_hours_by_symbol.get(raw),
        )
    return out


def _fetch_bybit_full_snapshot(timeout: float = 30.0, *, tradable: set[str] | None = None) -> dict[str, ExchangeCell]:
    payload = _http_get_json(scanner_endpoint_url("bybit", "market_tickers", category="linear"), timeout=timeout)
    if not isinstance(payload, dict) or payload.get("retCode") != 0:
        return {}
    items = payload.get("result", {}).get("list", [])
    if not isinstance(items, list):
        return {}
    interval_hours_by_symbol = _bybit_interval_hours_by_symbol(timeout)
    out: dict[str, ExchangeCell] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        raw = str(item.get("symbol") or "").strip().upper()
        canonical = from_exchange("bybit", raw)
        if not canonical or (tradable is not None and canonical not in tradable):
            continue
        volume = volume_usdt_for_exchange("bybit", item)
        if volume is None or volume <= 0:
            continue
        bid_price = _bid_bybit(item)
        ask_price = _ask_bybit(item)
        out[canonical] = ExchangeCell(
            volume_usdt=int(volume),
            price=resolve_price(_price_bybit(item), bid_price, ask_price),
            bid_price=bid_price,
            ask_price=ask_price,
            funding_rate_str=_fmt_rate(item.get("fundingRate")),
            next_funding_ms=_bybit_next_funding_ms(item),
            funding_interval_hours=_bybit_funding_interval_hours(item, interval_hours_by_symbol),
        )
    return out


def _fetch_bybit_price_snapshot(timeout: float = 12.0, *, tradable: set[str] | None = None) -> dict[str, ExchangeCell]:
    return _fetch_bybit_full_snapshot(timeout=timeout, tradable=tradable)


def _normalized_visible_exchange_ids(visible_exchange_ids: list[str] | None = None) -> list[str]:
    if visible_exchange_ids is None:
        return list(SUPPORTED_EXCHANGE_IDS)
    normalized = [str(exchange_id or "").strip().lower() for exchange_id in visible_exchange_ids if str(exchange_id or "").strip()]
    return [exchange_id for exchange_id in SUPPORTED_EXCHANGE_IDS if exchange_id in normalized]


def _clone_exchange_map(exchange_map: dict[str, ExchangeCell] | None) -> dict[str, ExchangeCell]:
    return dict(exchange_map or {})


def _fetch_price_snapshot_blocking(exchange_id: str, *, timeout: float, tradable: set[str] | None) -> dict[str, ExchangeCell]:
    if exchange_id == "binance":
        return dict(_fetch_binance_price_snapshot(timeout, tradable=tradable) or {})
    if exchange_id == "bybit":
        return dict(_fetch_bybit_price_snapshot(timeout, tradable=tradable) or {})
    return {}


def _poll_exchange_price_snapshot(exchange_id: str, *, timeout: float, tradable: set[str] | None) -> None:
    started = time.perf_counter()
    try:
        exchange_map = _fetch_price_snapshot_blocking(exchange_id, timeout=timeout, tradable=tradable)
        error: str | None = None
    except Exception as exc:
        exchange_map = {}
        error = str(exc)
    fetch_ms = int((time.perf_counter() - started) * 1000)
    with _PRICE_CACHE_LOCK:
        if exchange_map:
            _PRICE_CACHE[exchange_id] = exchange_map
            _PRICE_CACHE_FETCHED_AT[exchange_id] = time.monotonic()
            _PRICE_CACHE_ERRORS[exchange_id] = None
        elif error:
            _PRICE_CACHE_ERRORS[exchange_id] = error
        _PRICE_CACHE_IN_FLIGHT.discard(exchange_id)
    _LOGGER.info(
        "runtime exchange poll finished | exchange=%s | fetch_ms=%s | rows=%s | error=%s",
        exchange_id,
        fetch_ms,
        len(exchange_map),
        error or "-",
    )


def _ensure_price_poll_started(exchange_id: str, *, timeout: float, tradable: set[str] | None) -> None:
    now = time.monotonic()
    with _PRICE_CACHE_LOCK:
        last_fetched_at = float(_PRICE_CACHE_FETCHED_AT.get(exchange_id) or 0.0)
        in_flight = exchange_id in _PRICE_CACHE_IN_FLIGHT
        poll_interval = float(_PRICE_POLL_INTERVAL_SECONDS.get(exchange_id, 2.0) or 2.0)
        has_cache = bool(_PRICE_CACHE.get(exchange_id))
        if in_flight:
            return
        if has_cache and now - last_fetched_at < poll_interval:
            return
        _PRICE_CACHE_IN_FLIGHT.add(exchange_id)
    threading.Thread(
        target=_poll_exchange_price_snapshot,
        kwargs={"exchange_id": exchange_id, "timeout": timeout, "tradable": tradable},
        name=f"scanner-v2-{exchange_id}-poll",
        daemon=True,
    ).start()


def fetch_supported_tradable_sets(timeout: float = 30.0) -> dict[str, set[str]]:
    global _TRADABLE_CACHE
    now = time.monotonic()
    with _TRADABLE_CACHE_LOCK:
        cached = _TRADABLE_CACHE
        if cached is not None and cached[0] > now:
            return {exchange_id: set(symbols) for exchange_id, symbols in cached[1].items()}
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {
            "binance": executor.submit(fetch_binance_tradable_usdt_perpetual_canonical, timeout),
            "bybit": executor.submit(fetch_bybit_tradable_linear_perpetual_canonical, timeout),
        }
        out: dict[str, set[str]] = {}
        for exchange_id, future in futures.items():
            try:
                out[exchange_id] = set(future.result() or set())
            except Exception:
                out[exchange_id] = set()
        with _TRADABLE_CACHE_LOCK:
            _TRADABLE_CACHE = (time.monotonic() + _TRADABLE_CACHE_TTL_SECONDS, {exchange_id: set(symbols) for exchange_id, symbols in out.items()})
        return out


def fetch_supported_exchange_full_maps(
    timeout: float = 30.0,
    *,
    visible_exchange_ids: list[str] | None = None,
    tradable_only: bool = True,
) -> tuple[dict[str, dict[str, ExchangeCell]], str | None]:
    visible = _normalized_visible_exchange_ids(visible_exchange_ids)
    tradable_sets = fetch_supported_tradable_sets(timeout=min(timeout, 30.0)) if tradable_only else {}
    maps: dict[str, dict[str, ExchangeCell]] = {exchange_id: {} for exchange_id in SUPPORTED_EXCHANGE_IDS}
    errors: list[str] = []
    with ThreadPoolExecutor(max_workers=max(1, len(visible))) as executor:
        futures = {}
        if "binance" in visible:
            futures["binance"] = executor.submit(_fetch_binance_full_snapshot, timeout, tradable=tradable_sets.get("binance"))
        if "bybit" in visible:
            futures["bybit"] = executor.submit(_fetch_bybit_full_snapshot, timeout, tradable=tradable_sets.get("bybit"))
        for exchange_id, future in futures.items():
            try:
                maps[exchange_id] = dict(future.result() or {})
            except Exception as exc:
                errors.append(f"{exchange_id}:{exc}")
    return maps, "; ".join(errors) if errors else None


def fetch_supported_exchange_price_snapshot_maps(
    timeout: float = 12.0,
    *,
    visible_exchange_ids: list[str] | None = None,
    tradable_only: bool = True,
) -> tuple[dict[str, dict[str, ExchangeCell]], str | None]:
    visible = _normalized_visible_exchange_ids(visible_exchange_ids)
    tradable_sets = fetch_supported_tradable_sets(timeout=min(timeout, 30.0)) if tradable_only else {}
    maps: dict[str, dict[str, ExchangeCell]] = {exchange_id: {} for exchange_id in SUPPORTED_EXCHANGE_IDS}
    for exchange_id in visible:
        _ensure_price_poll_started(exchange_id, timeout=timeout, tradable=tradable_sets.get(exchange_id))
    errors: list[str] = []
    with _PRICE_CACHE_LOCK:
        for exchange_id in visible:
            maps[exchange_id] = _clone_exchange_map(_PRICE_CACHE.get(exchange_id))
            cached_error = _PRICE_CACHE_ERRORS.get(exchange_id)
            if cached_error:
                errors.append(f"{exchange_id}:{cached_error}")
    return maps, "; ".join(errors) if errors else None


def supported_exchange_price_cache_fetched_at(visible_exchange_ids: list[str] | None = None) -> dict[str, float | None]:
    visible = _normalized_visible_exchange_ids(visible_exchange_ids)
    with _PRICE_CACHE_LOCK:
        return {exchange_id: (float(_PRICE_CACHE_FETCHED_AT.get(exchange_id) or 0.0) or None) for exchange_id in visible}


__all__ = [
    "ExchangeCell",
    "fetch_supported_exchange_full_maps",
    "fetch_supported_exchange_price_snapshot_maps",
    "fetch_supported_tradable_sets",
    "resolve_price",
    "scanner_endpoint_url",
    "supported_exchange_price_cache_fetched_at",
]
