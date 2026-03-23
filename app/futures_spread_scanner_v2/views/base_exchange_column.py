from __future__ import annotations

from PySide6.QtCore import QRectF, Qt, Signal
from PySide6.QtGui import QColor, QFont, QPainter, QPen
from PySide6.QtWidgets import QFrame, QHBoxLayout, QLabel, QVBoxLayout, QWidget

from app.futures_spread_scanner_v2.views.common import (
    BaseExchangeRowViewModel,
    _ExchangeTitleLabel,
    _ClickableHeaderLabel,
    _RUNTIME_WIDGET_ROW_HEIGHT,
    _RUNTIME_WIDGET_SEPARATOR_ROW_HEIGHT,
    _base_exchange_split_x,
    _set_font_point_size_safe,
    _runtime_widget_stylesheet,
)
from app.futures_spread_scanner_v2.common.i18n import tr


class _BaseExchangeRowsCanvas(QWidget):
    wheel_scrolled = Signal(int)

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self._rows: list[BaseExchangeRowViewModel] = []
        self._scroll_offset = 0
        self.setMinimumHeight(320)
        self.setAutoFillBackground(False)

    def set_rows(self, rows: list[BaseExchangeRowViewModel]) -> None:
        if rows == self._rows:
            return
        self._rows = list(rows)
        self.update()

    def set_scroll_offset(self, offset: int) -> None:
        next_offset = max(0, int(offset))
        if next_offset == self._scroll_offset:
            return
        self._scroll_offset = next_offset
        self.update()

    def content_height(self) -> int:
        return sum(_RUNTIME_WIDGET_SEPARATOR_ROW_HEIGHT if row.kind == "separator" else _RUNTIME_WIDGET_ROW_HEIGHT for row in self._rows)

    @staticmethod
    def _funding_color(text: str) -> QColor:
        if str(text or "").startswith("+"):
            return QColor("#22c55e")
        if str(text or "").startswith("-") and str(text or "") != "-":
            return QColor("#ef4444")
        return QColor("#f1f5fb")

    def paintEvent(self, event) -> None:  # type: ignore[override]
        _ = event
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, False)
        rect = self.rect()
        if rect.width() <= 0 or rect.height() <= 0:
            return
        grid = QColor("#3d4e63")
        text_primary = QColor("#f1f5fb")
        text_muted = QColor("#8ea0b6")
        price_font = QFont("Segoe UI", 9)
        _set_font_point_size_safe(price_font, 12)
        price_font.setBold(True)
        funding_font = QFont("Segoe UI", 9)
        _set_font_point_size_safe(funding_font, 9)
        funding_font.setBold(False)
        timer_font = QFont("Segoe UI", 9)
        _set_font_point_size_safe(timer_font, 9)

        visible_rows: list[BaseExchangeRowViewModel] = []
        scan_y = -self._scroll_offset
        for row in self._rows:
            row_height = _RUNTIME_WIDGET_SEPARATOR_ROW_HEIGHT if row.kind == "separator" else _RUNTIME_WIDGET_ROW_HEIGHT
            row_top = scan_y
            row_bottom = scan_y + row_height
            if row_bottom <= 0:
                scan_y += row_height
                continue
            if row_top >= rect.height():
                break
            visible_rows.append(row)
            scan_y += row_height

        price_width = _base_exchange_split_x(int(rect.width()))
        funding_x = rect.left() + price_width
        y = 0
        for row in visible_rows:
            row_height = _RUNTIME_WIDGET_SEPARATOR_ROW_HEIGHT if row.kind == "separator" else _RUNTIME_WIDGET_ROW_HEIGHT
            row_rect = QRectF(rect.left(), y, rect.width(), row_height)
            painter.fillRect(int(rect.left()), int(row_rect.bottom() - 1), max(0, int(rect.width())), 1, grid)
            painter.fillRect(int(funding_x), int(row_rect.top()), 1, max(0, int(row_height)), grid)
            if row.kind != "separator":
                price_rect = QRectF(rect.left() + 8, y + 7, price_width - 16, row_height - 14)
                if row.accent == "low":
                    border_color = QColor(74, 222, 128, 150)
                elif row.accent == "high":
                    border_color = QColor(248, 113, 113, 150)
                elif row.accent == "same":
                    border_color = QColor(148, 163, 184, 150)
                else:
                    border_color = QColor(0, 0, 0, 0)
                pen = QPen(border_color)
                pen.setWidth(2)
                painter.setPen(pen)
                painter.setBrush(Qt.BrushStyle.NoBrush)
                painter.drawRoundedRect(price_rect, 10, 10)
                painter.setFont(price_font)
                painter.setPen(text_primary)
                painter.drawText(price_rect, int(Qt.AlignmentFlag.AlignCenter), row.price_text)
                funding_rect = QRectF(funding_x + 8, y + 8, rect.width() - price_width - 16, row_height - 16)
                painter.setFont(funding_font)
                painter.setPen(self._funding_color(row.funding_text))
                painter.drawText(QRectF(funding_rect.left(), funding_rect.top(), funding_rect.width(), 18), int(Qt.AlignmentFlag.AlignHCenter | Qt.AlignmentFlag.AlignTop), row.funding_text)
                painter.setFont(timer_font)
                painter.setPen(text_muted)
                painter.drawText(QRectF(funding_rect.left(), funding_rect.top() + 19, funding_rect.width(), 16), int(Qt.AlignmentFlag.AlignHCenter | Qt.AlignmentFlag.AlignTop), row.timer_text)
            y += row_height

        if not visible_rows:
            painter.setFont(timer_font)
            painter.setPen(text_muted)
            painter.drawText(QRectF(rect), int(Qt.AlignmentFlag.AlignCenter), tr("experimental.starter_empty"))
        painter.end()

    def wheelEvent(self, event) -> None:  # type: ignore[override]
        delta = int(event.angleDelta().y())
        if delta:
            self.wheel_scrolled.emit(delta)
            event.accept()
            return
        super().wheelEvent(event)


class BaseExchangeColumnView(QFrame):
    def __init__(self, title: str, exchange_id: str, parent=None) -> None:
        super().__init__(parent)
        self.setStyleSheet(_runtime_widget_stylesheet())
        self.setObjectName("scannerTableBlock")
        self.setProperty("scanner_block", "exchange")
        self.setMinimumWidth(180)
        self.setMinimumHeight(560)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(1, 0, 1, 4)
        layout.setSpacing(0)
        self._title_label = _ExchangeTitleLabel(title, self)
        self._title_label.setObjectName("scannerTableBlockTitle")
        self._title_label.setProperty("scanner_block", "exchange")
        self._title_label.setProperty("exchange_id", exchange_id)
        self._title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._title_label.set_exchange_logo(exchange_id)
        layout.addWidget(self._title_label, 0)

        self._subheader = QFrame(self)
        self._subheader.setObjectName("experimentalExchangeSubHeader")
        self._subheader.setFixedHeight(38)
        sub_layout = QHBoxLayout(self._subheader)
        sub_layout.setContentsMargins(0, 0, 0, 0)
        sub_layout.setSpacing(0)
        self._price_header = QLabel(tr("scanner.subcol_price"), self._subheader)
        self._price_header.setObjectName("experimentalExchangeSubHeaderLabel")
        self._price_header.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._funding_header = _ClickableHeaderLabel(tr("scanner.subcol_funding"), self._subheader)
        self._funding_header.setObjectName("experimentalExchangeSubHeaderLabel")
        self._funding_header.setAlignment(Qt.AlignmentFlag.AlignCenter)
        divider = QFrame(self._subheader)
        self._divider = divider
        divider.setFixedWidth(1)
        divider.setStyleSheet("background:#3d4e63; border:none;")
        sub_layout.addWidget(self._price_header, 0)
        sub_layout.addWidget(divider, 0)
        sub_layout.addWidget(self._funding_header, 1)
        layout.addWidget(self._subheader, 0)

        self._canvas = _BaseExchangeRowsCanvas(self)
        self._canvas.setObjectName("experimentalBaseExchangeRowsCanvas")
        layout.addWidget(self._canvas, 1)

    def set_rows(self, rows: list[BaseExchangeRowViewModel]) -> None:
        self._canvas.set_rows(rows)

    def set_scroll_offset(self, offset: int) -> None:
        self._canvas.set_scroll_offset(offset)

    def content_height(self) -> int:
        return self._canvas.content_height()

    def set_status_ok(self, ok: bool) -> None:
        self._title_label.set_status_ok(ok)

    def set_status_hint(self, text: str) -> None:
        self._title_label.setToolTip(str(text or "").strip())

    def resizeEvent(self, event) -> None:  # type: ignore[override]
        super().resizeEvent(event)
        self._sync_header_split()

    def _sync_header_split(self) -> None:
        subheader_width = int(self._subheader.width())
        if subheader_width <= 0:
            return
        split_x = _base_exchange_split_x(subheader_width)
        divider_width = int(self._divider.width())
        self._price_header.setFixedWidth(max(0, split_x))
        funding_width = max(0, subheader_width - split_x - divider_width)
        self._funding_header.setFixedWidth(funding_width)


__all__ = ["BaseExchangeColumnView"]
