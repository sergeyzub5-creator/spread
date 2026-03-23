from __future__ import annotations

import threading
import time

from app.charts.history import ChartHistoryRequest, ChartHistoryTimeframe, load_spread_history
from app.charts.market_data import load_matched_instrument_rows
from app.charts.models import PricePoint
from app.charts.ui.cell_formatters import stringify_rate
from app.charts.ui.helpers import format_volume_threshold, parse_daily_volume_threshold


class PriceChartWindowDataMixin:
    def _resolved_history_source(self, symbol: str) -> dict[str, str]:
        normalized_symbol = str(symbol or "").strip().upper()
        source = dict(self._selected_history_source or {})
        source_symbol = str(source.get("symbol") or "").strip().upper()
        if source_symbol == normalized_symbol:
            return {
                "left_exchange": str(source.get("left_exchange") or "").strip().lower(),
                "left_market_type": str(source.get("left_market_type") or "").strip().lower(),
                "right_exchange": str(source.get("right_exchange") or "").strip().lower(),
                "right_market_type": str(source.get("right_market_type") or "").strip().lower(),
                "symbol": normalized_symbol,
            }
        return {
            "left_exchange": str(self._slot_state["left"]["exchange"] or "").strip().lower(),
            "left_market_type": str(self._slot_state["left"]["market_type"] or "").strip().lower(),
            "right_exchange": str(self._slot_state["right"]["exchange"] or "").strip().lower(),
            "right_market_type": str(self._slot_state["right"]["market_type"] or "").strip().lower(),
            "symbol": normalized_symbol,
        }

    def _merge_history_points(self, current_points: list[object], incoming_points: list[object]) -> list[object]:
        merged = {int(item.open_time_ms): item for item in list(current_points or [])}
        for item in list(incoming_points or []):
            merged[int(item.open_time_ms)] = item
        return [merged[key] for key in sorted(merged)]

    def _history_cache_key(self, symbol: str) -> tuple[str, str, str, str, str, str, int]:
        source = self._resolved_history_source(symbol)
        return (
            source["left_exchange"],
            source["left_market_type"],
            source["right_exchange"],
            source["right_market_type"],
            source["symbol"],
            ChartHistoryTimeframe.M1.value,
            int(self._history_limit),
        )

    def _history_request_for_symbol(self, symbol: str) -> ChartHistoryRequest:
        source = self._resolved_history_source(symbol)
        return ChartHistoryRequest(
            left_exchange=source["left_exchange"],
            left_market_type=source["left_market_type"],
            left_symbol=source["symbol"],
            right_exchange=source["right_exchange"],
            right_market_type=source["right_market_type"],
            right_symbol=source["symbol"],
            timeframe=ChartHistoryTimeframe.M1,
            limit=int(self._history_limit),
        )

    def _apply_spread_history_points(self, symbol: str, history_points: list[object]) -> None:
        points = list(history_points or [])
        if not points:
            self.chart_widget.set_status_text(f"Нет 1м истории для {symbol}")
            return
        self.chart_widget.set_series_name(symbol)
        rendered_points = [
            PricePoint(timestamp_ms=int(item.open_time_ms), price=item.spread_pct)
            for item in points
        ]
        self.chart_widget.set_points(rendered_points)

    def _prepend_spread_history_points(self, history_points: list[object]) -> None:
        points = list(history_points or [])
        if not points:
            return
        rendered_points = [
            PricePoint(timestamp_ms=int(item.open_time_ms), price=item.spread_pct)
            for item in points
        ]
        self.chart_widget.prepend_points(rendered_points)

    def _request_spread_history(self, symbol: str | None) -> None:
        normalized_symbol = str(symbol or "").strip().upper()
        self._slot_state["left"]["symbol"] = normalized_symbol or None
        self._slot_state["right"]["symbol"] = normalized_symbol or None
        if not normalized_symbol:
            self.chart_widget.set_status_text("Выберите инструмент")
            return

        cache_key = self._history_cache_key(normalized_symbol)
        cached_points = self._history_session_cache.get(cache_key)
        cache_age_ms = int(time.time() * 1000) - int(self._history_session_cache_ts_ms.get(cache_key, 0))
        if cached_points:
            self._apply_spread_history_points(normalized_symbol, list(cached_points))
            if cache_age_ms < 30_000:
                return

        self._history_load_revision += 1
        revision = self._history_load_revision
        if not cached_points:
            self.chart_widget.set_status_text(f"Загрузка 1м истории: {normalized_symbol}")
        request = self._history_request_for_symbol(normalized_symbol)

        def _run() -> None:
            try:
                points = load_spread_history(request)
                self.spread_history_loaded.emit(revision, normalized_symbol, points, "")
            except Exception as exc:
                self.spread_history_loaded.emit(revision, normalized_symbol, None, str(exc))

        threading.Thread(target=_run, name=f"chart-history-{normalized_symbol}-{revision}", daemon=True).start()

    def _on_spread_history_loaded(self, revision: int, symbol: str, history_points: object, error: str) -> None:
        if revision != self._history_load_revision and revision != self._history_prepend_revision:
            return
        selected_symbol = str(self._selected_row_symbol or "").strip().upper()
        if selected_symbol and selected_symbol != str(symbol or "").strip().upper():
            return
        if error:
            self.chart_widget.set_status_text(f"Ошибка истории: {error}")
            return
        points = list(history_points or [])
        cache_key = self._history_cache_key(symbol)
        existing_points = self._history_session_cache.get(cache_key, [])
        merged_points = self._merge_history_points(existing_points, points)
        self._history_session_cache[cache_key] = merged_points
        self._history_session_cache_ts_ms[cache_key] = int(time.time() * 1000)
        self._history_pending_before_ms.pop(cache_key, None)
        if revision == self._history_prepend_revision:
            self._prepend_spread_history_points(points)
            return
        self._apply_spread_history_points(symbol, merged_points)

    def _request_older_spread_history(self, before_open_time_ms: int) -> None:
        normalized_symbol = str(self._selected_row_symbol or "").strip().upper()
        if not normalized_symbol:
            return
        cache_key = self._history_cache_key(normalized_symbol)
        if self._history_pending_before_ms.get(cache_key) == int(before_open_time_ms):
            return
        self._history_pending_before_ms[cache_key] = int(before_open_time_ms)
        self._history_prepend_revision += 1
        revision = self._history_prepend_revision
        request = self._history_request_for_symbol(normalized_symbol)
        timeframe_ms = 60_000
        older_limit = 3000
        request.end_time_ms = int(before_open_time_ms) - 1
        request.start_time_ms = request.end_time_ms - ((older_limit - 1) * timeframe_ms)
        request.limit = older_limit

        def _run() -> None:
            try:
                points = load_spread_history(request)
                self.spread_history_loaded.emit(revision, normalized_symbol, points, "")
            except Exception as exc:
                self.spread_history_loaded.emit(revision, normalized_symbol, None, str(exc))

        threading.Thread(target=_run, name=f"chart-history-older-{normalized_symbol}-{revision}", daemon=True).start()

    def _active_symbols(self, cache_key: str | None = None) -> list[str]:
        key = cache_key or self._current_cache_key()
        rows = self._live_rows_by_selection.get(key, [])
        return [str(row.get("symbol") or "").strip().upper() for row in rows if str(row.get("symbol") or "").strip()]

    def _prioritized_symbols_for_loading(self, cache_key: str | None = None) -> list[str]:
        key = cache_key or self._current_cache_key()
        active_symbols = self._active_symbols(key)
        if not active_symbols:
            return []
        active_set = {str(symbol).strip().upper() for symbol in active_symbols if str(symbol).strip()}
        bookmarks = [symbol for symbol in self._visible_bookmark_symbols() if symbol in active_set]
        bookmark_set = set(bookmarks)
        visible = [
            symbol
            for symbol in self._visible_priority_symbols()
            if symbol in active_set and symbol not in bookmark_set
        ]
        visible_set = set(visible)
        remaining = [
            symbol
            for symbol in active_symbols
            if symbol in active_set and symbol not in bookmark_set and symbol not in visible_set
        ]
        ordered: list[str] = []
        seen: set[str] = set()
        for group in (bookmarks, visible, remaining):
            for symbol in group:
                normalized = str(symbol).strip().upper()
                if not normalized or normalized in seen:
                    continue
                seen.add(normalized)
                ordered.append(normalized)
        return ordered

    def _apply_background_config(self) -> None:
        cache_key = self._current_cache_key()
        symbols = self._prioritized_symbols_for_loading(cache_key)
        signature = (
            cache_key,
            str(self._slot_state["left"]["exchange"] or ""),
            str(self._slot_state["left"]["market_type"] or ""),
            str(self._slot_state["right"]["exchange"] or ""),
            str(self._slot_state["right"]["market_type"] or ""),
            tuple(symbols),
        )
        if self._last_background_priority_signature == signature:
            return
        self._last_background_priority_signature = signature
        self._live_refresh_worker.update_config(
            {
                "cache_key": cache_key,
                "left_exchange": str(self._slot_state["left"]["exchange"] or ""),
                "left_market_type": str(self._slot_state["left"]["market_type"] or ""),
                "right_exchange": str(self._slot_state["right"]["exchange"] or ""),
                "right_market_type": str(self._slot_state["right"]["market_type"] or ""),
                "symbols": symbols,
            }
        )

    def _refresh_instruments(self) -> None:
        text = self.volume_edit.text().strip()
        self._daily_volume_threshold = parse_daily_volume_threshold(text)
        self.volume_edit.setText(format_volume_threshold(self._daily_volume_threshold))
        self._load_revision += 1
        revision = self._load_revision
        self.refresh_button.setEnabled(False)
        self.loaded_count_label.setText("Загрузка...")
        self.refresh_button.setText("Загрузка...")

        left_exchange = str(self._slot_state["left"]["exchange"] or "")
        left_market_type = str(self._slot_state["left"]["market_type"] or "")
        right_exchange = str(self._slot_state["right"]["exchange"] or "")
        right_market_type = str(self._slot_state["right"]["market_type"] or "")
        volume_threshold = self._daily_volume_threshold

        def _run() -> None:
            try:
                rows = load_matched_instrument_rows(
                    left_exchange=left_exchange,
                    left_market_type=left_market_type,
                    right_exchange=right_exchange,
                    right_market_type=right_market_type,
                    volume_threshold=volume_threshold,
                )
                self.instruments_loaded.emit(revision, rows, "")
            except Exception as exc:
                self.instruments_loaded.emit(revision, None, str(exc))

        threading.Thread(target=_run, name=f"chart-instruments-{revision}", daemon=True).start()

    def _on_instruments_loaded(self, revision: int, symbols: object, error: str) -> None:
        if revision != self._load_revision:
            return
        self.refresh_button.setEnabled(True)
        self.refresh_button.setText("Обновить")
        if error:
            self.market_table.setRowCount(2)
            self._install_sort_capsule_row()
            for column_index in range(5):
                self.market_table.removeCellWidget(1, column_index)
                self._table_item(1, column_index).setText("")
            self._table_item(1, 2).setText(f"Ошибка: {error}")
            return
        rows: list[dict[str, str | None]] = []
        for item in list(symbols or []):
            symbol = str(getattr(item, "symbol", "") or "").strip().upper()
            if not symbol:
                continue
            rows.append(
                {
                    "symbol": symbol,
                    "spread_pct": stringify_rate(getattr(item, "spread_pct", None)),
                    "left_funding_rate": stringify_rate(getattr(item, "left_funding_rate", None)),
                    "left_funding_interval_hours": getattr(item, "left_funding_interval_hours", None),
                    "left_next_funding_ms": getattr(item, "left_next_funding_ms", None),
                    "right_funding_rate": stringify_rate(getattr(item, "right_funding_rate", None)),
                    "right_funding_interval_hours": getattr(item, "right_funding_interval_hours", None),
                    "right_next_funding_ms": getattr(item, "right_next_funding_ms", None),
                }
            )
        cache_key = self._current_cache_key()
        self._live_rows_by_selection[cache_key] = rows
        self._cached_symbols_by_selection[cache_key] = [
            str(row.get("symbol") or "").strip().upper()
            for row in rows
            if str(row.get("symbol") or "").strip()
        ]
        self._persist_settings()
        self._apply_background_config()
        self._render_cached_instruments(force_rebuild=True)
        if self._selected_row_symbol and self._selected_row_symbol in self._cached_symbols_by_selection.get(cache_key, []):
            self._request_spread_history(self._selected_row_symbol)
        elif not self._selected_row_symbol:
            self.chart_widget.set_status_text("Выберите инструмент")

    def _on_price_updates_loaded(self, cache_key: str, updates: object, error: str) -> None:
        if error:
            return
        rows = self._live_rows_by_selection.get(cache_key)
        if rows is None or not isinstance(updates, dict):
            return
        changed_symbols: set[str] = set()
        for row in rows:
            symbol = str(row.get("symbol") or "").strip().upper()
            if not symbol:
                continue
            spread = updates.get(symbol)
            new_value = stringify_rate(spread) if spread is not None else None
            if row.get("spread_pct") == new_value:
                continue
            row["spread_pct"] = new_value
            changed_symbols.add(symbol)
        if cache_key == self._current_cache_key() and changed_symbols:
            if self._sort_mode == "spread":
                ordered_rows = self._ordered_rows(list(rows))
                if not self._schedule_reorder_if_needed(ordered_rows):
                    self._update_spread_cells_in_place(changed_symbols, cache_key)
            else:
                self._update_spread_cells_in_place(changed_symbols, cache_key)

    def _on_funding_updates_loaded(self, cache_key: str, side: str, updates: object, error: str) -> None:
        if error:
            return
        rows = self._live_rows_by_selection.get(cache_key)
        if rows is None or not isinstance(updates, dict):
            return
        rate_key = "left_funding_rate" if side == "left" else "right_funding_rate"
        interval_key = "left_funding_interval_hours" if side == "left" else "right_funding_interval_hours"
        next_key = "left_next_funding_ms" if side == "left" else "right_next_funding_ms"
        changed_symbols: set[str] = set()
        for row in rows:
            symbol = str(row.get("symbol") or "").strip().upper()
            if not symbol:
                continue
            update = updates.get(symbol)
            if update is None:
                continue
            new_rate = stringify_rate(getattr(update, "funding_rate", None))
            new_interval = getattr(update, "funding_interval_hours", None)
            new_next = getattr(update, "next_funding_ms", None)
            if row.get(rate_key) == new_rate and row.get(interval_key) == new_interval and row.get(next_key) == new_next:
                continue
            row[rate_key] = new_rate
            row[interval_key] = new_interval
            row[next_key] = new_next
            changed_symbols.add(symbol)
        if cache_key == self._current_cache_key() and changed_symbols:
            active_sort = "left_funding" if side == "left" else "right_funding"
            if self._sort_mode == active_sort:
                ordered_rows = self._ordered_rows(list(rows))
                if not self._schedule_reorder_if_needed(ordered_rows):
                    self._update_funding_cells_in_place(changed_symbols, side, cache_key)
            else:
                self._update_funding_cells_in_place(changed_symbols, side, cache_key)
