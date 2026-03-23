from __future__ import annotations

from PySide6.QtCore import QSize

from app.charts.cache import build_chart_selection_cache_key, load_chart_window_settings, save_chart_window_settings
from app.charts.exchanges.catalog import get_chart_exchange_meta
from app.charts.market_types import CHART_INSTRUMENT_TYPE_LABELS, ChartInstrumentType
from app.charts.ui.cell_formatters import normalize_cached_interval, normalize_cached_ms, normalize_cached_rate
from app.charts.ui.helpers import build_local_exchange_icon, format_volume_threshold, parse_daily_volume_threshold


class PriceChartWindowStateMixin:
    def _restore_settings(self) -> None:
        data = load_chart_window_settings()
        left_exchange = str(data.get("left_exchange") or "").strip().lower()
        right_exchange = str(data.get("right_exchange") or "").strip().lower()
        left_market_type = str(data.get("left_market_type") or "").strip().lower()
        right_market_type = str(data.get("right_market_type") or "").strip().lower()
        volume_text = str(data.get("daily_volume_text") or "").strip()
        volume_threshold = data.get("daily_volume_threshold")
        cached_pairs = data.get("cached_pairs_by_selection")
        bookmark_order = data.get("bookmark_order_by_selection")
        cached_rows = data.get("cached_rows_by_selection")
        should_cleanup_legacy_cache = isinstance(cached_pairs, dict) and isinstance(cached_rows, dict)

        if left_exchange:
            self._slot_state["left"]["exchange"] = left_exchange
            meta = get_chart_exchange_meta(left_exchange)
            self._exchange_buttons["left"].setText(meta["base_name"])
            self._exchange_buttons["left"].setIcon(build_local_exchange_icon(left_exchange, size=15))
            self._exchange_buttons["left"].setIconSize(QSize(15, 15))
        if right_exchange:
            self._slot_state["right"]["exchange"] = right_exchange
            meta = get_chart_exchange_meta(right_exchange)
            self._exchange_buttons["right"].setText(meta["base_name"])
            self._exchange_buttons["right"].setIcon(build_local_exchange_icon(right_exchange, size=15))
            self._exchange_buttons["right"].setIconSize(QSize(15, 15))

        known_market_types = {item.value for item in ChartInstrumentType}
        if left_market_type and left_market_type in known_market_types:
            self._slot_state["left"]["market_type"] = left_market_type
            self._market_type_buttons["left"].setText(CHART_INSTRUMENT_TYPE_LABELS[ChartInstrumentType(left_market_type)])
        if right_market_type and right_market_type in known_market_types:
            self._slot_state["right"]["market_type"] = right_market_type
            self._market_type_buttons["right"].setText(CHART_INSTRUMENT_TYPE_LABELS[ChartInstrumentType(right_market_type)])

        if isinstance(volume_threshold, int):
            self._daily_volume_threshold = volume_threshold if volume_threshold > 0 else None
        else:
            self._daily_volume_threshold = parse_daily_volume_threshold(volume_text)
        self.volume_edit.setText(format_volume_threshold(self._daily_volume_threshold))

        if isinstance(cached_pairs, dict):
            self._cached_symbols_by_selection = {
                str(key): [str(item).strip().upper() for item in values if str(item).strip()]
                for key, values in cached_pairs.items()
                if isinstance(values, list)
            }
            self._live_rows_by_selection = {
                str(key): [
                    {
                        "symbol": str(item).strip().upper(),
                        "spread_pct": None,
                        "left_funding_rate": None,
                        "left_funding_interval_hours": None,
                        "left_next_funding_ms": None,
                        "right_funding_rate": None,
                        "right_funding_interval_hours": None,
                        "right_next_funding_ms": None,
                    }
                    for item in values
                    if str(item).strip()
                ]
                for key, values in self._cached_symbols_by_selection.items()
            }
        elif isinstance(cached_rows, dict):
            normalized_rows: dict[str, list[dict[str, str | int | None]]] = {}
            for key, values in cached_rows.items():
                if not isinstance(values, list):
                    continue
                rows: list[dict[str, str | int | None]] = []
                for item in values:
                    if not isinstance(item, dict):
                        continue
                    symbol = str(item.get("symbol") or "").strip().upper()
                    if not symbol:
                        continue
                    rows.append(
                        {
                            "symbol": symbol,
                            "spread_pct": normalize_cached_rate(item.get("spread_pct")),
                            "left_funding_rate": normalize_cached_rate(item.get("left_funding_rate")),
                            "left_funding_interval_hours": normalize_cached_interval(item.get("left_funding_interval_hours")),
                            "left_next_funding_ms": normalize_cached_ms(item.get("left_next_funding_ms")),
                            "right_funding_rate": normalize_cached_rate(item.get("right_funding_rate")),
                            "right_funding_interval_hours": normalize_cached_interval(item.get("right_funding_interval_hours")),
                            "right_next_funding_ms": normalize_cached_ms(item.get("right_next_funding_ms")),
                        }
                    )
                normalized_rows[str(key)] = rows
            self._live_rows_by_selection = normalized_rows
            self._cached_symbols_by_selection = {
                key: [str(row.get("symbol") or "").strip().upper() for row in rows if str(row.get("symbol") or "").strip()]
                for key, rows in normalized_rows.items()
            }

        if isinstance(bookmark_order, dict):
            self._bookmark_order_by_selection = {
                str(key): [str(item).strip().upper() for item in values if str(item).strip()]
                for key, values in bookmark_order.items()
                if isinstance(values, list)
            }
        if should_cleanup_legacy_cache:
            self._persist_settings()

    def _current_cache_key(self) -> str:
        return build_chart_selection_cache_key(
            self._slot_state["left"]["exchange"],
            self._slot_state["left"]["market_type"],
            self._slot_state["right"]["exchange"],
            self._slot_state["right"]["market_type"],
            self._daily_volume_threshold,
        )

    def _current_bookmark_order(self) -> list[str]:
        return list(self._bookmark_order_by_selection.get(self._current_cache_key(), []))

    def _set_current_bookmark_order(self, symbols: list[str]) -> None:
        self._bookmark_order_by_selection[self._current_cache_key()] = [
            str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()
        ]

    def _is_bookmarked(self, symbol: str) -> bool:
        normalized = str(symbol or "").strip().upper()
        return bool(normalized and normalized in set(self._current_bookmark_order()))

    def _visible_bookmark_symbols(self) -> list[str]:
        current_symbols = set(self._active_symbols())
        return [symbol for symbol in self._current_bookmark_order() if symbol in current_symbols]

    def _toggle_bookmark(self, symbol: str) -> None:
        normalized = str(symbol or "").strip().upper()
        if not normalized:
            return
        order = self._current_bookmark_order()
        if normalized in order:
            order = [item for item in order if item != normalized]
        else:
            order.append(normalized)
        self._set_current_bookmark_order(order)
        self._persist_settings()
        self._render_cached_instruments(force_rebuild=True)

    def _move_bookmark_symbol(self, symbol: str, target_row: int) -> None:
        normalized = str(symbol or "").strip().upper()
        if not normalized:
            return
        order = self._visible_bookmark_symbols()
        if normalized not in order:
            return
        old_index = order.index(normalized)
        if target_row <= self._header_row_index:
            new_index = 0
        else:
            target_symbol = self._row_symbol(target_row)
            if target_symbol in order:
                new_index = order.index(target_symbol)
                if new_index > old_index:
                    new_index -= 1
            else:
                new_index = len(order) - 1
        updated = [item for item in order if item != normalized]
        updated.insert(max(0, min(new_index, len(updated))), normalized)
        self._set_current_bookmark_order(updated)
        self._persist_settings()
        self._render_cached_instruments(force_rebuild=True)

    def _persist_settings(self) -> None:
        save_chart_window_settings(
            left_exchange=self._slot_state["left"]["exchange"],
            right_exchange=self._slot_state["right"]["exchange"],
            left_market_type=self._slot_state["left"]["market_type"],
            right_market_type=self._slot_state["right"]["market_type"],
            daily_volume_text=self.volume_edit.text().strip(),
            daily_volume_threshold=self._daily_volume_threshold or 0,
            cached_pairs_by_selection=self._cached_symbols_by_selection,
            bookmark_order_by_selection=self._bookmark_order_by_selection,
        )
