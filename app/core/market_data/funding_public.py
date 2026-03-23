"""
Public REST snapshots for funding rates and mark prices (no API keys).
Used by the Scanner tab: prices + funding % + next funding timestamp.
"""
from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any
from urllib.request import Request, urlopen


@dataclass(slots=True)
class FundingRow:
    exchange: str
    symbol: str
    mark_price: str | None = None
    index_price: str | None = None
    last_funding_rate: str | None = None  # decimal string, e.g. 0.0001 = 0.01%
    next_funding_time_ms: int | None = None
    error: str | None = None


def _http_get_json(url: str, *, timeout: float = 12.0) -> Any:
    request = Request(url, headers={"User-Agent": "spread-sniper-ui-shell/1.0"})
    with urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def fetch_binance_usdm_premium_index(timeout: float = 15.0) -> list[FundingRow]:
    """
    Binance USD-M: /fapi/v1/premiumIndex
    Returns all perpetuals with mark, index, lastFundingRate, nextFundingTime (ms).
    """
    url = "https://fapi.binance.com/fapi/v1/premiumIndex"
    try:
        payload = _http_get_json(url, timeout=timeout)
    except Exception as exc:
        return [FundingRow(exchange="binance", symbol="*", error=str(exc))]
    if not isinstance(payload, list):
        return [FundingRow(exchange="binance", symbol="*", error="unexpected_payload")]
    rows: list[FundingRow] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol") or "").strip().upper()
        if not symbol or not symbol.endswith("USDT"):
            continue
        next_ms = item.get("nextFundingTime")
        try:
            next_ms_int = int(next_ms) if next_ms is not None else None
        except (TypeError, ValueError):
            next_ms_int = None
        rows.append(
            FundingRow(
                exchange="binance",
                symbol=symbol,
                mark_price=_fmt_price(item.get("markPrice")),
                index_price=_fmt_price(item.get("indexPrice")),
                last_funding_rate=_fmt_rate(item.get("lastFundingRate")),
                next_funding_time_ms=next_ms_int,
            )
        )
    rows.sort(key=lambda r: r.symbol)
    return rows


def fetch_bybit_linear_tickers(timeout: float = 15.0) -> list[FundingRow]:
    """
    Bybit linear: /v5/market/tickers?category=linear
    fundingRate present; next funding time not in ticker — use 8h boundary from server time.
    """
    url = "https://api.bybit.com/v5/market/tickers?category=linear"
    try:
        payload = _http_get_json(url, timeout=timeout)
    except Exception as exc:
        return [FundingRow(exchange="bybit", symbol="*", error=str(exc))]
    if not isinstance(payload, dict) or payload.get("retCode") != 0:
        return [FundingRow(exchange="bybit", symbol="*", error="unexpected_payload")]
    lst = payload.get("result", {}).get("list", [])
    if not isinstance(lst, list):
        return [FundingRow(exchange="bybit", symbol="*", error="no_list")]
    # Bybit funding every 8h; align to next boundary if no per-symbol time
    now_ms = int(time.time() * 1000)
    next_8h_ms = _next_8h_utc_ms(now_ms)
    rows: list[FundingRow] = []
    for item in lst:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        # fundingRate as decimal string from API
        rate = item.get("fundingRate")
        rows.append(
            FundingRow(
                exchange="bybit",
                symbol=symbol,
                mark_price=_fmt_price(item.get("markPrice")),
                index_price=_fmt_price(item.get("indexPrice")),
                last_funding_rate=_fmt_rate(rate),
                next_funding_time_ms=next_8h_ms,
            )
        )
    rows.sort(key=lambda r: r.symbol)
    return rows


def _fmt_price(v: Any) -> str | None:
    if v is None:
        return None
    try:
        d = Decimal(str(v))
        if d == 0:
            return None
        return format(d, "f")
    except Exception:
        return str(v)


def _fmt_rate(v: Any) -> str | None:
    if v is None:
        return None
    try:
        d = Decimal(str(v))
        return format(d, "f")
    except Exception:
        return str(v)


def _next_8h_utc_ms(now_ms: int) -> int:
    """Next 00:00, 08:00, or 16:00 UTC after now."""
    sec = now_ms // 1000
    # UTC boundaries every 8h from epoch
    boundary = 8 * 3600
    n = (sec // boundary + 1) * boundary
    return n * 1000


def funding_rate_to_percent_signed(rate_str: str | None) -> str:
    """0.0001 -> +0.0100%; negative with minus sign."""
    if not rate_str:
        return "—"
    try:
        d = Decimal(str(rate_str)) * Decimal("100")
        sign = "+" if d >= 0 else ""
        return f"{sign}{d:.4f}%"
    except Exception:
        return str(rate_str)


def funding_rate_to_percent_str(rate_str: str | None) -> str:
    """0.0001 -> 0.0100% for display."""
    if not rate_str:
        return "—"
    try:
        d = Decimal(str(rate_str)) * Decimal("100")
        return f"{d:.4f}%"
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


def fetch_okx_funding_rate(inst_id: str, timeout: float = 8.0) -> FundingRow | None:
    """OKX single inst funding; inst_id e.g. BTC-USDT-SWAP."""
    inst_id = str(inst_id or "").strip()
    if not inst_id:
        return None
    url = f"https://www.okx.com/api/v5/public/funding-rate?instId={inst_id}"
    try:
        payload = _http_get_json(url, timeout=timeout)
    except Exception:
        return None
    if not isinstance(payload, dict) or payload.get("code") != "0":
        return None
    data = payload.get("data") or []
    if not data or not isinstance(data[0], dict):
        return None
    item = data[0]
    next_ms = item.get("nextFundingTime")
    try:
        next_ms_int = int(next_ms) if next_ms is not None else None
    except (TypeError, ValueError):
        next_ms_int = None
    return FundingRow(
        exchange="okx",
        symbol=inst_id.replace("-SWAP", "").replace("-", ""),
        mark_price=None,
        index_price=None,
        last_funding_rate=_fmt_rate(item.get("fundingRate")),
        next_funding_time_ms=next_ms_int,
    )


def fetch_okx_ticker_last(inst_id: str, timeout: float = 8.0) -> str | None:
    url = f"https://www.okx.com/api/v5/market/ticker?instId={inst_id}"
    try:
        payload = _http_get_json(url, timeout=timeout)
    except Exception:
        return None
    if not isinstance(payload, dict) or payload.get("code") != "0":
        return None
    data = payload.get("data") or []
    if not data or not isinstance(data[0], dict):
        return None
    return _fmt_price(data[0].get("last"))


def binance_symbol_to_base(symbol: str) -> str:
    s = str(symbol or "").strip().upper()
    if s.endswith("USDT"):
        return s[:-4] or s
    return s


def okx_inst_id_usdt_swap(base: str) -> str:
    return f"{str(base).strip().upper()}-USDT-SWAP"


def build_funding_lookup(rows: list[FundingRow]) -> dict[str, FundingRow]:
    """Key: uppercase symbol as on exchange (BTCUSDT) or instId."""
    out: dict[str, FundingRow] = {}
    for r in rows:
        if r.error:
            continue
        out[r.symbol.upper()] = r
    return out


@dataclass(slots=True)
class MatrixCell:
    price: str | None
    funding_signed: str  # F: +0.01%
    next_funding_time_ms: int | None


@dataclass(slots=True)
class MatrixPairRow:
    pair_label: str  # BTC/USDT
    binance: MatrixCell | None
    bybit: MatrixCell | None
    bitget: MatrixCell | None
    okx: MatrixCell | None


def fetch_bitget_usdt_futures(symbol: str, timeout: float = 8.0) -> FundingRow | None:
    """Bitget USDT-M perpetual: fund-rate + ticker for mark."""
    symbol = str(symbol or "").strip().upper()
    if not symbol:
        return None
    next_ms_int = None
    rate_str = None
    try:
        url = f"https://api.bitget.com/api/v2/mix/market/current-fund-rate?symbol={symbol}&productType=USDT-FUTURES"
        payload = _http_get_json(url, timeout=timeout)
        if isinstance(payload, dict) and str(payload.get("code")) == "00000":
            data = payload.get("data") or []
            if data and isinstance(data[0], dict):
                item = data[0]
                rate_str = _fmt_rate(item.get("fundingRate"))
                nu = item.get("nextUpdate")
                if nu is not None:
                    next_ms_int = int(nu)
    except Exception:
        pass
    mark = None
    try:
        url = f"https://api.bitget.com/api/v2/mix/market/ticker?symbol={symbol}&productType=USDT-FUTURES"
        payload = _http_get_json(url, timeout=timeout)
        if isinstance(payload, dict) and str(payload.get("code")) == "00000":
            data = payload.get("data") or []
            if data and isinstance(data[0], dict):
                mark = _fmt_price(data[0].get("markPrice") or data[0].get("lastPr"))
                if rate_str is None:
                    rate_str = _fmt_rate(data[0].get("fundingRate"))
    except Exception:
        pass
    if mark is None and rate_str is None and next_ms_int is None:
        return None
    return FundingRow(
        exchange="bitget",
        symbol=symbol,
        mark_price=mark,
        index_price=None,
        last_funding_rate=rate_str,
        next_funding_time_ms=next_ms_int,
    )


def build_matrix_pair_rows(
    *,
    bases: list[str],
    binance_rows: list[FundingRow],
    bybit_rows: list[FundingRow],
    bitget_cells: dict[str, MatrixCell] | None = None,
    okx_cells: dict[str, MatrixCell] | None = None,
) -> list[MatrixPairRow]:
    """
    bases: e.g. ["BTC","ETH"] — builds one row per base with Binance/Bybit from lists; OKX from okx_cells keyed by base.
    """
    binance_by = build_funding_lookup(binance_rows)
    bybit_by = build_funding_lookup(bybit_rows)
    bitget_cells = bitget_cells or {}
    okx_cells = okx_cells or {}
    result: list[MatrixPairRow] = []
    for base in bases:
        b = base.strip().upper()
        if not b:
            continue
        sym = f"{b}USDT"
        bn = binance_by.get(sym)
        bb = bybit_by.get(sym)
        def cell_from_row(r: FundingRow | None) -> MatrixCell | None:
            if r is None:
                return None
            return MatrixCell(
                price=r.mark_price or r.index_price,
                funding_signed=f"F: {funding_rate_to_percent_signed(r.last_funding_rate)}",
                next_funding_time_ms=r.next_funding_time_ms,
            )
        bg = bitget_cells.get(b)
        okx_cell = okx_cells.get(b)
        result.append(
            MatrixPairRow(
                pair_label=f"{b}/USDT",
                binance=cell_from_row(bn),
                bybit=cell_from_row(bb),
                bitget=bg,
                okx=okx_cell,
            )
        )
    return result


class FundingSnapshotCache:
    """Thread-safe cache refreshed in background for UI."""

    def __init__(self, refresh_interval_sec: float = 30.0) -> None:
        self._lock = threading.RLock()
        self._binance: list[FundingRow] = []
        self._bybit: list[FundingRow] = []
        self._bitget_matrix_cells: dict[str, MatrixCell] = {}
        self._okx_matrix_cells: dict[str, MatrixCell] = {}
        self._last_fetch_ms: int = 0
        self._refresh_interval_sec = max(5.0, float(refresh_interval_sec))
        self._fetch_error: str | None = None

    def snapshot(
        self,
    ) -> tuple[list[FundingRow], list[FundingRow], dict[str, MatrixCell], dict[str, MatrixCell], int, str | None]:
        with self._lock:
            return (
                list(self._binance),
                list(self._bybit),
                dict(self._bitget_matrix_cells),
                dict(self._okx_matrix_cells),
                self._last_fetch_ms,
                self._fetch_error,
            )

    def refresh_blocking(self) -> None:
        err_parts: list[str] = []
        binance_rows = fetch_binance_usdm_premium_index()
        if binance_rows and binance_rows[0].error:
            err_parts.append(f"binance:{binance_rows[0].error}")
            binance_rows = []
        bybit_rows = fetch_bybit_linear_tickers()
        if bybit_rows and bybit_rows[0].error:
            err_parts.append(f"bybit:{bybit_rows[0].error}")
            bybit_rows = []
        # Bitget + OKX per base (small set)
        bitget_cells: dict[str, MatrixCell] = {}
        okx_cells: dict[str, MatrixCell] = {}
        for base in ("BTC", "ETH", "XRP", "SOL", "DOGE"):
            sym = f"{base}USDT"
            bg_row = fetch_bitget_usdt_futures(sym)
            if bg_row is not None:
                bitget_cells[base] = MatrixCell(
                    price=bg_row.mark_price,
                    funding_signed=f"F: {funding_rate_to_percent_signed(bg_row.last_funding_rate)}",
                    next_funding_time_ms=bg_row.next_funding_time_ms,
                )
            inst = okx_inst_id_usdt_swap(base)
            fr = fetch_okx_funding_rate(inst)
            last = fetch_okx_ticker_last(inst)
            if fr is not None or last is not None:
                okx_cells[base] = MatrixCell(
                    price=last or (getattr(fr, "mark_price", None) if fr else None),
                    funding_signed=f"F: {funding_rate_to_percent_signed(fr.last_funding_rate if fr else None)}",
                    next_funding_time_ms=fr.next_funding_time_ms if fr else None,
                )
        with self._lock:
            self._binance = binance_rows
            self._bybit = bybit_rows
            self._bitget_matrix_cells = bitget_cells
            self._okx_matrix_cells = okx_cells
            self._last_fetch_ms = int(time.time() * 1000)
            self._fetch_error = "; ".join(err_parts) if err_parts else None

    def refresh_async(self) -> None:
        def _run() -> None:
            try:
                self.refresh_blocking()
            except Exception:
                pass

        threading.Thread(target=_run, name="funding-snapshot-refresh", daemon=True).start()
