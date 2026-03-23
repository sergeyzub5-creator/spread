from __future__ import annotations

from PySide6.QtGui import QColor, QPainter, QPen
from PySide6.QtWidgets import QTableWidget, QWidget


class ChartMarketTable(QTableWidget):
    def __init__(self, rows: int, columns: int, parent: QWidget | None = None) -> None:
        super().__init__(rows, columns, parent)
        self._hover_row = -1
        self._selected_row = -1
        self._bookmark_boundary_row = -1

    def set_hover_row(self, row_index: int) -> None:
        if row_index == self._hover_row:
            return
        self._hover_row = row_index
        self.viewport().update()

    def set_selected_row(self, row_index: int) -> None:
        if row_index == self._selected_row:
            return
        self._selected_row = row_index
        self.viewport().update()

    def set_bookmark_boundary_row(self, row_index: int) -> None:
        if row_index == self._bookmark_boundary_row:
            return
        self._bookmark_boundary_row = row_index
        self.viewport().update()

    def paintEvent(self, event) -> None:
        super().paintEvent(event)
        painter = QPainter(self.viewport())
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        self._paint_row_overlay(
            painter,
            self._hover_row,
            fill_color=QColor("#161b24"),
            border_color=QColor("#636c79"),
        )
        self._paint_row_overlay(
            painter,
            self._selected_row,
            fill_color=QColor("#1a253c"),
            border_color=QColor("#d6dde8"),
        )
        self._paint_bookmark_boundary(painter)
        painter.end()

    def _paint_row_overlay(
        self,
        painter: QPainter,
        row_index: int,
        *,
        fill_color: QColor,
        border_color: QColor,
    ) -> None:
        if row_index < 0 or row_index >= self.rowCount() or self.columnCount() <= 0:
            return
        left_rect = self.visualRect(self.model().index(row_index, 0))
        right_rect = self.visualRect(self.model().index(row_index, self.columnCount() - 1))
        if not left_rect.isValid() or not right_rect.isValid():
            return
        row_rect = left_rect.united(right_rect).adjusted(2, 1, -2, -1)
        if row_rect.width() <= 0 or row_rect.height() <= 0:
            return
        painter.setPen(QPen(border_color, 1))
        painter.setBrush(fill_color)
        painter.drawRoundedRect(row_rect, 10, 10)

    def _paint_bookmark_boundary(self, painter: QPainter) -> None:
        row_index = self._bookmark_boundary_row
        if row_index <= 0 or row_index >= self.rowCount() or self.columnCount() <= 0:
            return
        left_rect = self.visualRect(self.model().index(row_index, 0))
        right_rect = self.visualRect(self.model().index(row_index, self.columnCount() - 1))
        if not left_rect.isValid() or not right_rect.isValid():
            return
        line_rect = left_rect.united(right_rect)
        y = line_rect.top()
        painter.setPen(QPen(QColor("#566179"), 2))
        painter.drawLine(line_rect.left() + 2, y, line_rect.right() - 2, y)
