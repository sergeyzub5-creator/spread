from __future__ import annotations

from PySide6.QtCore import QRectF, Qt, Signal
from PySide6.QtGui import QColor, QFont, QPainter
from PySide6.QtWidgets import QFrame, QHBoxLayout, QLabel, QVBoxLayout, QWidget

from app.futures_spread_scanner_v2.runtime.contracts import OutputRowState
from app.futures_spread_scanner_v2.views.common import (
    _ClickableHeaderLabel,
    _RUNTIME_WIDGET_ROW_HEIGHT,
    _RUNTIME_WIDGET_SEPARATOR_ROW_HEIGHT,
    _runtime_palette,
    _set_font_point_size_safe,
    _runtime_widget_stylesheet,
)


class _OutputRowsCanvas(QWidget):
    wheel_scrolled = Signal(int)

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self._rows: list[OutputRowState] = []
        self._scroll_offset = 0
        self.setAttribute(Qt.WidgetAttribute.WA_OpaquePaintEvent, False)
        self.setAutoFillBackground(False)
        self.setObjectName("experimentalOutputRowsCanvas")

    def set_rows(self, rows: list[OutputRowState]) -> None:
        self._rows = list(rows)
        self.updateGeometry()
        self.update()

    def set_scroll_offset(self, offset: int) -> None:
        next_offset = max(0, int(offset))
        if next_offset == self._scroll_offset:
            return
        self._scroll_offset = next_offset
        self.update()

    def content_height(self) -> int:
        total = 0
        for row in self._rows:
            total += _RUNTIME_WIDGET_SEPARATOR_ROW_HEIGHT if getattr(row, "kind", "") == "separator" else _RUNTIME_WIDGET_ROW_HEIGHT
        return total

    def wheelEvent(self, event) -> None:  # type: ignore[override]
        delta = int(event.angleDelta().y())
        if delta:
            self.wheel_scrolled.emit(delta)
            event.accept()
            return
        super().wheelEvent(event)

    def paintEvent(self, event) -> None:  # type: ignore[override]
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, False)
        palette = _runtime_palette()
        row_line = QColor(palette["border"])
        fg = QColor(palette["text_primary"])
        muted = QColor(palette["text_muted"])
        positive = QColor("#22c55e")
        negative = QColor("#ef4444")
        rect = self.rect()
        if rect.width() <= 0 or rect.height() <= 0:
            return
        painter.setClipRect(rect.adjusted(0, 0, 0, -1))
        visible_rows: list[tuple[OutputRowState, int]] = []
        scan_y = -self._scroll_offset
        for row in self._rows:
            height = _RUNTIME_WIDGET_SEPARATOR_ROW_HEIGHT if getattr(row, "kind", "") == "separator" else _RUNTIME_WIDGET_ROW_HEIGHT
            row_top = scan_y
            row_bottom = scan_y + height
            if row_bottom <= 0:
                scan_y += height
                continue
            if row_top >= rect.height():
                break
            visible_rows.append((row, height))
            scan_y += height

        y = 0
        for row, height in visible_rows:
            row_rect = QRectF(0, y, rect.width(), height)
            painter.fillRect(int(row_rect.left()), int(row_rect.bottom() - 1), max(0, int(row_rect.width())), 1, row_line)
            if getattr(row, "kind", "") != "separator":
                value_text = str(getattr(row, "value_text", "-") or "-")
                accent = getattr(row, "accent", None)
                if accent == "positive":
                    text_color = positive
                elif accent == "negative":
                    text_color = negative
                elif accent == "neutral":
                    text_color = muted
                else:
                    text_color = fg
                font = QFont("Segoe UI", 9)
                _set_font_point_size_safe(font, 11)
                font.setBold(False)
                painter.setFont(font)
                painter.setPen(text_color)
                painter.drawText(row_rect, int(Qt.AlignmentFlag.AlignCenter), value_text)
            y += height
        painter.end()


class OutputColumnView(QFrame):
    def __init__(self, title: str, parent=None) -> None:
        super().__init__(parent)
        self.setStyleSheet(_runtime_widget_stylesheet())
        self.setObjectName("scannerTableBlock")
        self.setProperty("scanner_block", "output")
        self.setMinimumWidth(76)
        self.setMinimumHeight(560)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(1, 0, 1, 4)
        layout.setSpacing(0)
        self._title_label = QLabel(title, self)
        self._title_label.setObjectName("scannerTableBlockTitle")
        self._title_label.setProperty("scanner_block", "output")
        self._title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(self._title_label, 0)

        self._subheader = QFrame(self)
        self._subheader.setObjectName("experimentalExchangeSubHeader")
        self._subheader.setFixedHeight(38)
        sub_layout = QHBoxLayout(self._subheader)
        sub_layout.setContentsMargins(0, 0, 0, 0)
        sub_layout.setSpacing(0)
        self._value_header = _ClickableHeaderLabel(title, self._subheader)
        self._value_header.setObjectName("experimentalExchangeSubHeaderLabel")
        self._value_header.setAlignment(Qt.AlignmentFlag.AlignCenter)
        sub_layout.addWidget(self._value_header, 1)
        layout.addWidget(self._subheader, 0)

        self._canvas = _OutputRowsCanvas(self)
        layout.addWidget(self._canvas, 1)

    def set_rows(self, rows: list[OutputRowState]) -> None:
        self._canvas.set_rows(rows)

    def set_scroll_offset(self, offset: int) -> None:
        self._canvas.set_scroll_offset(offset)

    def content_height(self) -> int:
        return self._canvas.content_height()


__all__ = ["OutputColumnView"]
