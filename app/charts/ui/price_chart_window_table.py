from __future__ import annotations

from decimal import Decimal

from PySide6.QtCore import Qt
from PySide6.QtGui import QColor
from PySide6.QtWidgets import QTableWidgetItem

from app.charts.ui.cell_formatters import normalize_cached_interval, normalize_cached_ms
from app.charts.ui.cell_widgets import apply_cell_row_style, set_bookmark_cell, set_funding_cell, set_instrument_cell, set_spread_cell, tick_funding_timer


class PriceChartWindowTableMixin:
    def _sort_indicator(self) -> str:
        return "↓" if self._sort_descending else "↑"

    def _refresh_sort_headers(self) -> None:
        for mode, button in self._sort_buttons.items():
            if mode == "bookmark":
                button.setText("★")
                button.setProperty("selected", False)
            elif mode == "instrument":
                button.setText("Инструмент")
                button.setProperty("selected", False)
            else:
                title = "Ставка" if mode in ("left_funding", "right_funding") else "Спред %"
                if self._sort_mode == mode:
                    title = f"{title} {self._sort_indicator()}"
                button.setText(title)
                button.setProperty("selected", self._sort_mode == mode)
            button.style().unpolish(button)
            button.style().polish(button)
            button.update()

    def _set_sort(self, sort_mode: str) -> None:
        if self._sort_mode == sort_mode:
            self._sort_descending = not self._sort_descending
        else:
            self._sort_mode = sort_mode
            self._sort_descending = True
        self._pending_reorder_rows = None
        self._reorder_timer.stop()
        self._refresh_sort_headers()
        self._render_cached_instruments(force_rebuild=True)

    def _ordered_rows(self, rows: list[dict[str, str | int | None]] | None = None) -> list[dict[str, str | int | None]]:
        source_rows = rows if rows is not None else list(self._live_rows_by_selection.get(self._current_cache_key(), []))
        bookmark_positions = {symbol: index for index, symbol in enumerate(self._current_bookmark_order())}

        def _decimal_value(value: object) -> Decimal:
            text = str(value or "").strip()
            if not text:
                return Decimal("-999999999")
            try:
                return Decimal(text)
            except Exception:
                return Decimal("-999999999")

        def _metric(row: dict[str, str | int | None]) -> Decimal:
            if self._sort_mode == "left_funding":
                return _decimal_value(row.get("left_funding_rate"))
            if self._sort_mode == "right_funding":
                return _decimal_value(row.get("right_funding_rate"))
            return _decimal_value(row.get("spread_pct"))

        bookmarked_rows = []
        regular_rows = []
        for row in source_rows:
            symbol = str(row.get("symbol") or "").strip().upper()
            if symbol in bookmark_positions:
                bookmarked_rows.append(row)
            else:
                regular_rows.append(row)
        bookmarked_rows.sort(key=lambda row: bookmark_positions.get(str(row.get("symbol") or "").strip().upper(), 999999))
        regular_rows.sort(key=_metric, reverse=self._sort_descending)
        return bookmarked_rows + regular_rows

    def _ordered_symbols(self, rows: list[dict[str, str | int | None]]) -> list[str]:
        return [str(row.get("symbol") or "").strip().upper() for row in rows if str(row.get("symbol") or "").strip()]

    def _current_rows_map(self, cache_key: str | None = None) -> dict[str, dict[str, str | int | None]]:
        key = cache_key or self._current_cache_key()
        return {
            str(row.get("symbol") or "").strip().upper(): row
            for row in self._live_rows_by_selection.get(key, [])
            if str(row.get("symbol") or "").strip()
        }

    def _update_spread_cells_in_place(self, symbols: set[str], cache_key: str) -> None:
        rows_map = self._current_rows_map(cache_key)
        for symbol in symbols:
            row_index = self._row_index_by_symbol.get(symbol)
            row = rows_map.get(symbol)
            if row_index is None or row is None or row_index not in self._rendered_data_rows:
                continue
            set_spread_cell(self.market_table, row_index, 3, row.get("spread_pct"))

    def _update_funding_cells_in_place(self, symbols: set[str], side: str, cache_key: str) -> None:
        rows_map = self._current_rows_map(cache_key)
        column_index = 1 if side == "left" else 4
        rate_key = "left_funding_rate" if side == "left" else "right_funding_rate"
        interval_key = "left_funding_interval_hours" if side == "left" else "right_funding_interval_hours"
        next_key = "left_next_funding_ms" if side == "left" else "right_next_funding_ms"
        for symbol in symbols:
            row_index = self._row_index_by_symbol.get(symbol)
            row = rows_map.get(symbol)
            if row_index is None or row is None or row_index not in self._rendered_data_rows:
                continue
            set_funding_cell(self.market_table, row_index, column_index, row.get(rate_key), row.get(interval_key), row.get(next_key))

    def _max_symbol_shift(self, new_symbols: list[str]) -> int:
        if not self._visible_row_symbols or len(new_symbols) != len(self._visible_row_symbols):
            return max(len(new_symbols), len(self._visible_row_symbols))
        previous_positions = {symbol: index for index, symbol in enumerate(self._visible_row_symbols)}
        max_shift = 0
        for new_index, symbol in enumerate(new_symbols):
            old_index = previous_positions.get(symbol)
            if old_index is None:
                return max(len(new_symbols), len(self._visible_row_symbols))
            max_shift = max(max_shift, abs(new_index - old_index))
        return max_shift

    def _schedule_reorder_if_needed(self, ordered_rows: list[dict[str, str | int | None]]) -> bool:
        new_symbols = self._ordered_symbols(ordered_rows)
        if new_symbols == self._visible_row_symbols or self._max_symbol_shift(new_symbols) < 2:
            return False
        self._pending_reorder_rows = ordered_rows
        self._reorder_timer.start()
        return True

    def _apply_pending_reorder(self) -> None:
        if self._pending_reorder_rows is not None:
            rows = self._pending_reorder_rows
            self._pending_reorder_rows = None
            self._rebuild_table(rows)

    def _visible_data_row_bounds(self, *, buffer_rows: int | None = None) -> tuple[int, int]:
        row_count = self.market_table.rowCount()
        if row_count <= 1:
            return (1, 0)
        top_row = max(1, self.market_table.rowAt(0))
        bottom_row = self.market_table.rowAt(max(0, self.market_table.viewport().height() - 1))
        if bottom_row < 1:
            bottom_row = row_count - 1
        extra_rows = self._lazy_render_buffer if buffer_rows is None else max(0, int(buffer_rows))
        return (top_row, min(row_count - 1, bottom_row + extra_rows))

    def _visible_priority_symbols(self) -> list[str]:
        start_row, end_row = self._visible_data_row_bounds(buffer_rows=0)
        if end_row < start_row:
            return []
        symbols: list[str] = []
        for row_index in range(start_row, end_row + 1):
            symbol = self._row_symbol(row_index)
            if symbol:
                symbols.append(symbol)
        return symbols

    def _clear_data_row_widgets(self, row_index: int) -> None:
        for column_index in (0, 1, 2, 3, 4):
            self.market_table.removeCellWidget(row_index, column_index)

    def _render_data_row_widgets(self, row_index: int, row: dict[str, str | int | None]) -> None:
        set_bookmark_cell(self.market_table, row_index, self._is_bookmarked(str(row.get("symbol") or "")))
        set_funding_cell(self.market_table, row_index, 1, row.get("left_funding_rate"), row.get("left_funding_interval_hours"), row.get("left_next_funding_ms"))
        set_instrument_cell(self.market_table, row_index, str(row.get("symbol") or ""), 2)
        set_spread_cell(self.market_table, row_index, 3, row.get("spread_pct"))
        set_funding_cell(self.market_table, row_index, 4, row.get("right_funding_rate"), row.get("right_funding_interval_hours"), row.get("right_next_funding_ms"))

    def _render_visible_rows_window(self) -> None:
        if not hasattr(self, "market_table"):
            return
        start_row, end_row = self._visible_data_row_bounds()
        desired_rows = set(range(start_row, end_row + 1)) if end_row >= start_row else set()
        visible_start_row, visible_end_row = self._visible_data_row_bounds(buffer_rows=0)
        visible_rows = set(range(visible_start_row, visible_end_row + 1)) if visible_end_row >= visible_start_row else set()
        bookmarked_rows = {
            row_index for row_index in desired_rows if self._is_bookmarked(self._row_symbol(row_index))
        }
        render_order = (
            sorted(bookmarked_rows)
            + sorted(visible_rows - bookmarked_rows)
            + sorted(desired_rows - visible_rows - bookmarked_rows)
        )
        for row_index in self._rendered_data_rows - desired_rows:
            self._clear_data_row_widgets(row_index)
        for row_index in render_order:
            if row_index in self._rendered_data_rows:
                continue
            display_index = row_index - 1
            if 0 <= display_index < len(self._display_rows):
                self._render_data_row_widgets(row_index, self._display_rows[display_index])
                self._apply_row_visual_state(row_index)
        self._rendered_data_rows = desired_rows
        self._apply_background_config()

    def _rebuild_table(self, rows: list[dict[str, str | int | None]]) -> None:
        self.market_table.setUpdatesEnabled(False)
        try:
            self.market_table.setRowCount(len(rows) + 1)
            self._install_sort_capsule_row()
            self._display_rows = list(rows)
            self._rendered_data_rows = set()
            self._visible_row_symbols = []
            self._row_index_by_symbol = {}
            for visible_index, row in enumerate(rows):
                row_index = visible_index + 1
                symbol = str(row.get("symbol") or "").strip().upper()
                self._visible_row_symbols.append(symbol)
                self._row_index_by_symbol[symbol] = row_index
                self.market_table.setRowHeight(row_index, 52)
                for column_index in range(5):
                    item = self._table_item(row_index, column_index)
                    item.setText("")
                    item.setFlags(Qt.ItemFlag.ItemIsEnabled | Qt.ItemFlag.ItemIsSelectable)
                self._apply_row_visual_state(row_index)
            if self._selected_row_symbol and self._selected_row_symbol not in self._row_index_by_symbol:
                self._selected_row_symbol = None
                self._selected_history_source = None
                self.chart_widget.set_status_text("Выберите инструмент")
        finally:
            self.market_table.setUpdatesEnabled(True)
        self._render_visible_rows_window()

    def _render_cached_instruments(self, *, force_rebuild: bool = False) -> None:
        rows = self._ordered_rows()
        ordered_symbols = self._ordered_symbols(rows)
        if force_rebuild or ordered_symbols != self._visible_row_symbols:
            self._rebuild_table(rows)
        else:
            self._display_rows = list(rows)
            self._render_visible_rows_window()
        bookmark_count = len(self._visible_bookmark_symbols())
        boundary_row = self._header_row_index + 1 + bookmark_count if bookmark_count and bookmark_count < len(rows) else -1
        self.market_table.set_bookmark_boundary_row(boundary_row)
        self.loaded_count_label.setText(f"Загружено пар: {len(rows)}")

    def _table_item(self, row_index: int, column_index: int) -> QTableWidgetItem:
        item = self.market_table.item(row_index, column_index)
        if item is None:
            item = QTableWidgetItem()
            self.market_table.setItem(row_index, column_index, item)
        return item

    def _row_symbol(self, row_index: int) -> str:
        if row_index <= self._header_row_index:
            return ""
        visible_index = row_index - 1
        if visible_index < 0 or visible_index >= len(self._visible_row_symbols):
            return ""
        return str(self._visible_row_symbols[visible_index] or "").strip().upper()

    def _apply_row_visual_state(self, row_index: int) -> None:
        if row_index <= self._header_row_index or row_index >= self.market_table.rowCount():
            return
        for column_index in range(self.market_table.columnCount()):
            item = self._table_item(row_index, column_index)
            item.setBackground(QColor(0, 0, 0, 0))
            item.setForeground(QColor("#d5dae2"))
            cell = self.market_table.cellWidget(row_index, column_index)
            if cell is not None:
                apply_cell_row_style(cell)
        self.market_table.set_hover_row(self._hover_row_index)
        self.market_table.set_selected_row(self._row_index_by_symbol.get(self._selected_row_symbol or "", -1))

    def _refresh_all_row_visual_states(self) -> None:
        for row_index in range(self._header_row_index + 1, self.market_table.rowCount()):
            self._apply_row_visual_state(row_index)

    def _on_table_cell_pressed(self, row_index: int, column_index: int) -> None:
        if row_index == self._header_row_index:
            return
        if column_index == 0 and not self._bookmark_drag_active:
            self._toggle_bookmark(self._row_symbol(row_index))
            return
        self._selected_row_symbol = self._row_symbol(row_index) or None
        self._selected_history_source = (
            {
                "left_exchange": str(self._slot_state["left"]["exchange"] or ""),
                "left_market_type": str(self._slot_state["left"]["market_type"] or ""),
                "right_exchange": str(self._slot_state["right"]["exchange"] or ""),
                "right_market_type": str(self._slot_state["right"]["market_type"] or ""),
                "symbol": str(self._selected_row_symbol or ""),
            }
            if self._selected_row_symbol
            else None
        )
        self._refresh_all_row_visual_states()
        self._request_spread_history(self._selected_row_symbol)

    def _tick_funding_timers(self) -> None:
        rows_map = self._current_rows_map()
        for row_index in sorted(self._rendered_data_rows):
            if row_index <= self._header_row_index:
                continue
            row = rows_map.get(self._row_symbol(row_index))
            if row is None:
                continue
            for column_index, next_key, interval_key in (
                (1, "left_next_funding_ms", "left_funding_interval_hours"),
                (4, "right_next_funding_ms", "right_funding_interval_hours"),
            ):
                tick_funding_timer(self.market_table, row_index, column_index, normalize_cached_ms(row.get(next_key)), normalize_cached_interval(row.get(interval_key)))
