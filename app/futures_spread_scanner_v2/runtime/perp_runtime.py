from __future__ import annotations

import threading
import time

from PySide6.QtCore import QTimer, Signal

from app.futures_spread_scanner_v2.common.logger import get_logger
from app.futures_spread_scanner_v2.runtime.funding_utils import format_countdown, funding_rate_to_percent_signed, ms_until_next_funding
from app.futures_spread_scanner_v2.runtime.market_backend import (
    fetch_supported_exchange_full_maps,
    fetch_supported_exchange_price_snapshot_maps,
    supported_exchange_price_cache_fetched_at,
)
from app.futures_spread_scanner_v2.runtime.contracts import BasePerpRuntime, PerpRowState, PerpSnapshot
from app.futures_spread_scanner_v2.runtime.market_helpers import ExchangeCell, resolve_price
from app.futures_spread_scanner_v2.common.i18n import tr
from app.futures_spread_scanner_v2.common.price_format import format_compact_price


class _ExchangePerpRuntime(BasePerpRuntime):
    _full_maps_ready = Signal(object, object)
    _price_maps_ready = Signal(object, object)

    def __init__(self, exchange_id: str, title: str, top_volume_limit: int | None = None) -> None:
        super().__init__()
        self._logger = get_logger(f"scanner.v2.perp.{str(exchange_id or '').strip().lower() or 'unknown'}")
        self._exchange_id = str(exchange_id or "").strip().lower()
        self._title = str(title or "").strip() or self._exchange_id.title()
        self._top_volume_limit = max(1, int(top_volume_limit or 200))
        self._quote_freshness_ms = 10_000
        self._loading = False
        self._full_map_by_canonical: dict[str, ExchangeCell] = {}
        self._selected_canonicals: list[str] = []
        self._full_refresh_in_flight = False
        self._price_refresh_in_flight = False
        self._full_refresh_pending = False
        self._price_refresh_pending = False
        self._snapshot = PerpSnapshot(
            exchange_id=self._exchange_id,
            title=self._title,
            status_ok=False,
            is_fresh=False,
            snapshot_age_ms=None,
            loading=False,
            status_hint="",
            rows=[],
        )
        self._full_maps_ready.connect(self._apply_full_maps)
        self._price_maps_ready.connect(self._apply_price_maps)
        self._price_timer = QTimer(self)
        self._price_timer.setInterval(2000)
        self._price_timer.timeout.connect(self._request_price_refresh)
        self._price_timer.start()
        self._request_full_refresh()
        self._request_price_refresh()

    @staticmethod
    def _safe_emit(signal, *args) -> bool:
        try:
            signal.emit(*args)
            return True
        except RuntimeError:
            return False

    def set_top_volume_limit(self, top_volume_limit: int | None) -> None:
        next_limit = max(1, int(top_volume_limit or 200))
        if next_limit == self._top_volume_limit:
            return
        self._top_volume_limit = next_limit

    def snapshot(self) -> PerpSnapshot:
        return self._snapshot

    def force_refresh(self) -> None:
        self._request_full_refresh()
        self._request_price_refresh()

    @staticmethod
    def _build_timer_text(next_funding_ms: int | None, interval_hours: int | None) -> str:
        if next_funding_ms is None:
            return "-"
        timer_text = format_countdown(ms_until_next_funding(next_funding_ms))
        if interval_hours is not None and interval_hours > 0:
            return f"{timer_text} ({interval_hours}ч)"
        return timer_text

    @staticmethod
    def _format_price_text(price: object) -> str:
        return format_compact_price(price)

    def _price_snapshot_age_ms(self) -> int | None:
        fetched_at = supported_exchange_price_cache_fetched_at([self._exchange_id]).get(self._exchange_id)
        if fetched_at is None:
            return None
        try:
            age_ms = int(max(0.0, time.monotonic() - float(fetched_at)) * 1000)
        except Exception:
            return None
        return age_ms

    def _is_snapshot_fresh(self) -> bool:
        age_ms = self._price_snapshot_age_ms()
        return age_ms is not None and age_ms <= int(self._quote_freshness_ms or 10_000)

    def _request_full_refresh(self) -> None:
        if self._full_refresh_in_flight:
            self._full_refresh_pending = True
            return
        self._full_refresh_in_flight = True
        self._full_refresh_pending = False
        self._loading = True
        self._rebuild_snapshot()
        if not self._safe_emit(self.loading_changed, True):
            self._logger.info("safe emit skipped | signal=loading_changed | phase=full_refresh_start")

        def _run() -> None:
            try:
                maps, err = fetch_supported_exchange_full_maps(
                    timeout=30.0,
                    visible_exchange_ids=[self._exchange_id],
                    tradable_only=True,
                )
            except Exception as exc:
                maps, err = {}, str(exc)
            if not self._safe_emit(self._full_maps_ready, maps, err):
                self._logger.info("safe emit skipped | signal=full_maps_ready")

        threading.Thread(
            target=_run,
            name=f"experimental-{self._exchange_id}-full-refresh",
            daemon=True,
        ).start()

    def _request_price_refresh(self) -> None:
        if self._price_refresh_in_flight:
            self._price_refresh_pending = True
            return
        self._price_refresh_in_flight = True
        self._price_refresh_pending = False

        def _run() -> None:
            try:
                maps, err = fetch_supported_exchange_price_snapshot_maps(
                    timeout=12.0,
                    visible_exchange_ids=[self._exchange_id],
                    tradable_only=True,
                )
            except Exception as exc:
                maps, err = {}, str(exc)
            if not self._safe_emit(self._price_maps_ready, maps, err):
                self._logger.info("safe emit skipped | signal=price_maps_ready")

        threading.Thread(
            target=_run,
            name=f"experimental-{self._exchange_id}-price-refresh",
            daemon=True,
        ).start()

    def _apply_full_maps(self, maps: object, err: object) -> None:
        self._full_refresh_in_flight = False
        _ = err
        exchange_map = (maps or {}).get(self._exchange_id) if isinstance(maps, dict) else {}
        if isinstance(exchange_map, dict):
            sorted_items = sorted(
                exchange_map.items(),
                key=lambda item: (-int(getattr(item[1], "volume_usdt", 0) or 0), item[0]),
            )
            selected_items = sorted_items[: max(1, int(self._top_volume_limit or 200))]
            self._selected_canonicals = [str(canonical or "").strip().upper() for canonical, _cell in selected_items]
            self._full_map_by_canonical = {
                str(canonical or "").strip().upper(): cell
                for canonical, cell in selected_items
            }
            self._rebuild_snapshot()
        if self._full_refresh_pending:
            self._request_full_refresh()
            return
        self._loading = False
        self._rebuild_snapshot()
        if not self._safe_emit(self.loading_changed, False):
            self._logger.info("safe emit skipped | signal=loading_changed | phase=full_refresh_end")

    def _apply_price_maps(self, maps: object, err: object) -> None:
        self._price_refresh_in_flight = False
        _ = err
        exchange_map = (maps or {}).get(self._exchange_id) if isinstance(maps, dict) else {}
        if isinstance(exchange_map, dict):
            for canonical in list(self._selected_canonicals):
                next_cell = exchange_map.get(canonical)
                if next_cell is None:
                    continue
                current = self._full_map_by_canonical.get(canonical)
                if current is None:
                    self._full_map_by_canonical[canonical] = next_cell
                    continue
                current.price = next_cell.price
                current.bid_price = next_cell.bid_price
                current.ask_price = next_cell.ask_price
                current.funding_rate_str = next_cell.funding_rate_str
                current.next_funding_ms = next_cell.next_funding_ms
                current.funding_interval_hours = next_cell.funding_interval_hours
            self._rebuild_snapshot()
        if self._price_refresh_pending:
            self._request_price_refresh()

    def _rebuild_snapshot(self) -> None:
        rows: list[PerpRowState] = []
        for canonical in self._selected_canonicals:
            cell = self._full_map_by_canonical.get(canonical)
            if cell is None:
                continue
            funding_rate_raw = getattr(cell, "funding_rate_str", None)
            try:
                funding_sort_value = float(str(funding_rate_raw)) if funding_rate_raw is not None else None
            except Exception:
                funding_sort_value = None
            display_price = resolve_price(
                getattr(cell, "price", None),
                getattr(cell, "bid_price", None),
                getattr(cell, "ask_price", None),
            )
            try:
                price_value = float(display_price) if display_price is not None else None
            except Exception:
                price_value = None
            price_text = self._format_price_text(display_price) if display_price is not None else "-"
            rows.append(
                PerpRowState(
                    kind="row",
                    canonical=str(canonical or "").strip().upper(),
                    volume_usdt=int(getattr(cell, "volume_usdt", 0) or 0),
                    price_value=price_value,
                    bid_price_value=float(getattr(cell, "bid_price", 0) or 0) if getattr(cell, "bid_price", None) is not None else None,
                    ask_price_value=float(getattr(cell, "ask_price", 0) or 0) if getattr(cell, "ask_price", None) is not None else None,
                    price_text=price_text,
                    accent=None,
                    funding_text=funding_rate_to_percent_signed(funding_rate_raw),
                    funding_sort_value=funding_sort_value,
                    funding_rate_raw=None if funding_rate_raw is None else str(funding_rate_raw),
                    interval_hours=getattr(cell, "funding_interval_hours", None),
                    timer_text=self._build_timer_text(
                        getattr(cell, "next_funding_ms", None),
                        getattr(cell, "funding_interval_hours", None),
                    ),
                )
            )
        is_fresh = self._is_snapshot_fresh()
        snapshot_age_ms = self._price_snapshot_age_ms()
        if self._loading:
            status_hint = tr("scanner.refreshing")
        elif not self._full_map_by_canonical:
            status_hint = tr("scanner.status_loading")
        elif is_fresh:
            status_hint = tr("scanner.experimental_status_fresh", age_ms=int(snapshot_age_ms or 0))
        else:
            status_hint = tr("scanner.experimental_status_stale", age_ms=int(snapshot_age_ms or 0))
        next_snapshot = PerpSnapshot(
            exchange_id=self._exchange_id,
            title=self._title,
            status_ok=bool(self._full_map_by_canonical) and is_fresh,
            is_fresh=is_fresh,
            snapshot_age_ms=snapshot_age_ms,
            loading=self._loading,
            status_hint=status_hint,
            rows=rows,
        )
        if next_snapshot == self._snapshot:
            return
        self._snapshot = next_snapshot
        if not self._safe_emit(self.snapshot_changed):
            self._logger.info("safe emit skipped | signal=snapshot_changed")


class BinancePerpRuntime(_ExchangePerpRuntime):
    def __init__(self, top_volume_limit: int | None = None) -> None:
        super().__init__("binance", "Binance", top_volume_limit)


class BybitPerpRuntime(_ExchangePerpRuntime):
    def __init__(self, top_volume_limit: int | None = None) -> None:
        super().__init__("bybit", "Bybit", top_volume_limit)


_BINANCE_PERP_RUNTIME: BinancePerpRuntime | None = None
_BYBIT_PERP_RUNTIME: BybitPerpRuntime | None = None


def get_shared_binance_perp_runtime(top_volume_limit: int | None = None) -> BinancePerpRuntime:
    global _BINANCE_PERP_RUNTIME
    if _BINANCE_PERP_RUNTIME is None:
        _BINANCE_PERP_RUNTIME = BinancePerpRuntime(top_volume_limit)
    else:
        _BINANCE_PERP_RUNTIME.set_top_volume_limit(top_volume_limit)
    return _BINANCE_PERP_RUNTIME


def get_shared_bybit_perp_runtime(top_volume_limit: int | None = None) -> BybitPerpRuntime:
    global _BYBIT_PERP_RUNTIME
    if _BYBIT_PERP_RUNTIME is None:
        _BYBIT_PERP_RUNTIME = BybitPerpRuntime(top_volume_limit)
    else:
        _BYBIT_PERP_RUNTIME.set_top_volume_limit(top_volume_limit)
    return _BYBIT_PERP_RUNTIME


__all__ = [
    "BinancePerpRuntime",
    "BybitPerpRuntime",
    "get_shared_binance_perp_runtime",
    "get_shared_bybit_perp_runtime",
]
