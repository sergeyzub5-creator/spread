from __future__ import annotations

from typing import Any

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QFrame, QLabel, QTableWidget, QVBoxLayout, QWidget

from app.charts.ui.cell_formatters import build_timer_text, format_funding_rate, format_spread_pct, funding_color, spread_color


def apply_cell_row_style(cell: QWidget) -> None:
    cell.setStyleSheet(
        "background-color: transparent;"
        "border: none;"
    )


def set_instrument_cell(table: QTableWidget, row_index: int, symbol: str, column_index: int = 2) -> None:
    cell = table.cellWidget(row_index, column_index)
    if cell is None:
        cell = _create_instrument_cell()
        table.setCellWidget(row_index, column_index, cell)
    _update_instrument_cell(cell, symbol)


def set_bookmark_cell(table: QTableWidget, row_index: int, bookmarked: bool) -> None:
    cell = table.cellWidget(row_index, 0)
    if cell is None:
        cell = _create_bookmark_cell()
        table.setCellWidget(row_index, 0, cell)
    _update_bookmark_cell(cell, bookmarked)


def set_spread_cell(table: QTableWidget, row_index: int, column_index: int, spread_pct: str | None) -> None:
    cell = table.cellWidget(row_index, column_index)
    if cell is None:
        cell = _create_spread_cell()
        table.setCellWidget(row_index, column_index, cell)
    _update_spread_cell(cell, spread_pct)


def set_funding_cell(
    table: QTableWidget,
    row_index: int,
    column_index: int,
    rate_value: str | None,
    interval_hours: int | None,
    next_funding_ms: int | None,
) -> None:
    cell = table.cellWidget(row_index, column_index)
    if cell is None:
        cell = _create_funding_cell()
        table.setCellWidget(row_index, column_index, cell)
    _update_funding_cell(cell, rate_value, interval_hours, next_funding_ms)


def tick_funding_timer(
    table: QTableWidget,
    row_index: int,
    column_index: int,
    next_funding_ms: int | None,
    interval_hours: int | None,
) -> None:
    cell = table.cellWidget(row_index, column_index)
    if cell is None:
        return
    timer_label = getattr(cell, "_timer_label", None)
    if timer_label is None:
        return
    timer_label.setText(build_timer_text(next_funding_ms, interval_hours))


def _create_instrument_cell() -> QWidget:
    frame = QFrame()
    frame.setObjectName("chartInstrumentCell")
    frame.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents, True)
    layout = QVBoxLayout(frame)
    layout.setContentsMargins(6, 3, 6, 3)
    layout.setSpacing(0)
    symbol_label = QLabel("")
    symbol_label.setObjectName("chartInstrumentSymbolLabel")
    symbol_label.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents, True)
    layout.addStretch(1)
    layout.addWidget(symbol_label, 0, Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
    layout.addStretch(1)
    frame._symbol_label = symbol_label
    apply_cell_row_style(frame)
    return frame


def _create_bookmark_cell() -> QWidget:
    frame = QFrame()
    frame.setObjectName("chartBookmarkCell")
    frame.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents, True)
    layout = QVBoxLayout(frame)
    layout.setContentsMargins(0, 0, 0, 0)
    layout.setSpacing(0)
    star_label = QLabel("☆")
    star_label.setObjectName("chartBookmarkStarLabel")
    star_label.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents, True)
    layout.addStretch(1)
    layout.addWidget(star_label, 0, Qt.AlignmentFlag.AlignCenter)
    layout.addStretch(1)
    frame._star_label = star_label
    apply_cell_row_style(frame)
    return frame


def _update_bookmark_cell(cell: QWidget, bookmarked: bool) -> None:
    star_label = getattr(cell, "_star_label", None)
    if star_label is None:
        return
    star_label.setText("★" if bookmarked else "☆")
    star_label.setStyleSheet(f"color: {'#f5c542' if bookmarked else '#697181'};")


def _update_instrument_cell(cell: QWidget, symbol: str) -> None:
    symbol_label = getattr(cell, "_symbol_label", None)
    if symbol_label is None:
        return
    symbol_label.setText(symbol)


def _create_spread_cell() -> QWidget:
    frame = QFrame()
    frame.setObjectName("chartSpreadCell")
    frame.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents, True)
    layout = QVBoxLayout(frame)
    layout.setContentsMargins(6, 3, 6, 3)
    layout.setSpacing(0)
    label = QLabel("-")
    label.setObjectName("chartInstrumentSpreadLabel")
    label.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents, True)
    layout.addStretch(1)
    layout.addWidget(label, 0, Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
    layout.addStretch(1)
    frame._spread_label = label
    apply_cell_row_style(frame)
    return frame


def _update_spread_cell(cell: QWidget, spread_pct: str | None) -> None:
    spread_label = getattr(cell, "_spread_label", None)
    if spread_label is None:
        return
    spread_label.setText(format_spread_pct(spread_pct))
    spread_label.setStyleSheet(f"color: {spread_color(spread_pct).name()};")


def _create_funding_cell() -> QWidget:
    frame = QFrame()
    frame.setObjectName("chartFundingCell")
    frame.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents, True)
    layout = QVBoxLayout(frame)
    layout.setContentsMargins(6, 3, 6, 3)
    layout.setSpacing(0)
    rate_label = QLabel("-")
    rate_label.setObjectName("chartFundingRateLabel")
    rate_label.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents, True)
    timer_label = QLabel("-")
    timer_label.setObjectName("chartFundingTimerLabel")
    timer_label.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents, True)
    layout.addWidget(rate_label, 0, Qt.AlignmentFlag.AlignRight)
    layout.addWidget(timer_label, 0, Qt.AlignmentFlag.AlignRight)
    frame._rate_label = rate_label
    frame._timer_label = timer_label
    apply_cell_row_style(frame)
    return frame


def _update_funding_cell(
    cell: QWidget,
    rate_value: str | None,
    interval_hours: int | None,
    next_funding_ms: int | None,
) -> None:
    rate_label = getattr(cell, "_rate_label", None)
    timer_label = getattr(cell, "_timer_label", None)
    if rate_label is None or timer_label is None:
        return
    color = funding_color(rate_value)
    rate_label.setText(format_funding_rate(rate_value))
    rate_label.setStyleSheet(f"color: {color.name()};")
    timer_label.setText(build_timer_text(next_funding_ms, interval_hours))
