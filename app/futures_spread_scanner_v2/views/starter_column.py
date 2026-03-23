from __future__ import annotations

from PySide6.QtCore import QPoint, QRectF, Qt, Signal
from PySide6.QtGui import QColor, QFont, QPainter, QPen
from PySide6.QtWidgets import QApplication, QFrame, QLabel, QVBoxLayout, QWidget

from app.futures_spread_scanner_v2.views.common import (
    StarterRowViewModel,
    _PairSearchEdit,
    _RUNTIME_WIDGET_ROW_HEIGHT,
    _RUNTIME_WIDGET_SEPARATOR_ROW_HEIGHT,
    _set_font_point_size_safe,
    _runtime_widget_stylesheet,
)
from app.futures_spread_scanner_v2.common.i18n import tr


class _StarterRowsCanvas(QWidget):
    bookmark_toggled = Signal(str)
    bookmark_drag_released = Signal(str, int)
    wheel_scrolled = Signal(int)

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self._rows: list[StarterRowViewModel] = []
        self._scroll_offset = 0
        self._drag_canonical: str | None = None
        self._drag_start_pos: QPoint | None = None
        self._pressed_row_index: int = -1
        self._drag_armed = False
        self._drop_row_index: int = -1
        self.setMinimumHeight(320)
        self.setAutoFillBackground(False)

    def set_rows(self, rows: list[StarterRowViewModel]) -> None:
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

    def _row_at_y(self, y: float) -> tuple[int, StarterRowViewModel | None]:
        cursor = -float(self._scroll_offset)
        for idx, candidate in enumerate(self._rows):
            row_height = _RUNTIME_WIDGET_SEPARATOR_ROW_HEIGHT if candidate.kind == "separator" else _RUNTIME_WIDGET_ROW_HEIGHT
            if cursor <= y < cursor + row_height:
                return idx, candidate
            cursor += row_height
        return -1, None

    def _row_top_for_index(self, row_index: int) -> int | None:
        if row_index < 0 or row_index >= len(self._rows):
            return None
        cursor = -int(self._scroll_offset)
        for idx, candidate in enumerate(self._rows):
            row_height = _RUNTIME_WIDGET_SEPARATOR_ROW_HEIGHT if candidate.kind == "separator" else _RUNTIME_WIDGET_ROW_HEIGHT
            if idx == row_index:
                return cursor
            cursor += row_height
        return None

    def paintEvent(self, event) -> None:  # type: ignore[override]
        _ = event
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, False)
        rect = self.rect()
        if rect.width() <= 0 or rect.height() <= 0:
            return
        painter.setClipRect(rect.adjusted(0, 0, 0, -1))
        grid = QColor("#3d4e63")
        text_primary = QColor("#f1f5fb")
        text_muted = QColor("#8ea0b6")
        star_active = QColor("#ffcf33")
        star_col_width = 38
        y = 0
        row_font = QFont("Segoe UI", 9)
        _set_font_point_size_safe(row_font, 9)
        sep_font = QFont("Segoe UI", 9)
        _set_font_point_size_safe(sep_font, 9)
        sep_font.setBold(True)

        rows: list[StarterRowViewModel] = []
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
            rows.append(row)
            scan_y += row_height
        for row in rows:
            row_height = _RUNTIME_WIDGET_SEPARATOR_ROW_HEIGHT if row.kind == "separator" else _RUNTIME_WIDGET_ROW_HEIGHT
            row_rect = QRectF(rect.left(), y, rect.width(), row_height)
            painter.fillRect(int(rect.left()), int(row_rect.bottom() - 1), max(0, int(rect.width())), 1, grid)
            painter.fillRect(int(rect.left() + star_col_width), int(row_rect.top()), 1, max(0, int(row_height)), grid)
            if row.kind == "separator":
                painter.setFont(sep_font)
                painter.setPen(text_primary)
                painter.drawText(row_rect, int(Qt.AlignmentFlag.AlignCenter), tr("scanner.bookmarks_separator"))
            else:
                painter.setFont(row_font)
                painter.setPen(star_active if row.bookmarked else text_muted)
                painter.drawText(QRectF(rect.left(), y, star_col_width, row_height), int(Qt.AlignmentFlag.AlignCenter), "★" if row.bookmarked else "☆")
                painter.setPen(text_primary)
                painter.drawText(QRectF(rect.left() + star_col_width + 10, y, rect.width() - star_col_width - 16, row_height), int(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignLeft), row.canonical)
            y += row_height

        if self._drag_armed and self._drop_row_index >= 0:
            drop_top = self._row_top_for_index(self._drop_row_index)
            if drop_top is not None and 0 <= drop_top <= rect.height():
                indicator_color = QColor("#7aa2ff")
                painter.setPen(QPen(indicator_color, 2))
                left = int(rect.left() + 8)
                right = int(rect.right() - 8)
                painter.drawLine(left, drop_top, right, drop_top)
                painter.setBrush(indicator_color)
                painter.setPen(Qt.PenStyle.NoPen)
                painter.drawEllipse(left - 2, drop_top - 3, 6, 6)
                painter.drawEllipse(right - 4, drop_top - 3, 6, 6)
        if not rows:
            painter.setFont(row_font)
            painter.setPen(text_muted)
            painter.drawText(QRectF(rect), int(Qt.AlignmentFlag.AlignCenter), tr("experimental.starter_empty"))
        painter.end()

    def mousePressEvent(self, event) -> None:  # type: ignore[override]
        if event.button() != Qt.MouseButton.LeftButton:
            return super().mousePressEvent(event)
        row_index, row = self._row_at_y(float(event.position().y()))
        if row_index < 0 or row is None or row.kind != "pair":
            return super().mousePressEvent(event)
        self._pressed_row_index = row_index
        self._drag_armed = False
        if row.bookmarked:
            self._drag_canonical = row.canonical
            self._drag_start_pos = event.position().toPoint()
        super().mousePressEvent(event)

    def mouseMoveEvent(self, event) -> None:  # type: ignore[override]
        if self._drag_canonical and self._drag_start_pos is not None:
            if (event.position().toPoint() - self._drag_start_pos).manhattanLength() > QApplication.startDragDistance():
                self._drag_armed = True
                self.setCursor(Qt.CursorShape.SizeVerCursor)
                row_index, _row = self._row_at_y(float(event.position().y()))
                self._drop_row_index = row_index
                self.update()
                event.accept()
                return
        super().mouseMoveEvent(event)

    def mouseReleaseEvent(self, event) -> None:  # type: ignore[override]
        if event.button() == Qt.MouseButton.LeftButton:
            release_row_index, release_row = self._row_at_y(float(event.position().y()))
            if self._drag_canonical and self._drag_armed:
                row_index, _row = self._row_at_y(float(event.position().y()))
                self.bookmark_drag_released.emit(self._drag_canonical, row_index)
            elif release_row is not None and release_row.kind == "pair" and release_row_index == self._pressed_row_index and event.position().x() <= 38:
                self.bookmark_toggled.emit(release_row.canonical)
            self._drag_canonical = None
            self._drag_start_pos = None
            self._pressed_row_index = -1
            self._drag_armed = False
            self._drop_row_index = -1
            self.unsetCursor()
            self.update()
        super().mouseReleaseEvent(event)

    def wheelEvent(self, event) -> None:  # type: ignore[override]
        delta = int(event.angleDelta().y())
        if delta:
            self.wheel_scrolled.emit(delta)
            event.accept()
            return
        super().wheelEvent(event)


class StarterColumnView(QFrame):
    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self.setStyleSheet(_runtime_widget_stylesheet())
        self.setObjectName("scannerTableBlock")
        self.setProperty("scanner_block", "pair")
        self.setMinimumWidth(168)
        self.setMinimumHeight(560)
        self._all_rows: list[StarterRowViewModel] = []
        self._search_text = ""

        layout = QVBoxLayout(self)
        layout.setContentsMargins(1, 0, 1, 4)
        layout.setSpacing(0)
        self._title_label = QLabel(tr("scanner.matrix_col_pair"), self)
        self._title_label.setObjectName("scannerTableBlockTitle")
        self._title_label.setProperty("scanner_block", "pair")
        self._title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(self._title_label, 0)

        self._search_shell = QFrame(self)
        self._search_shell.setObjectName("scannerPairSearchRow")
        self._search_shell.setFixedHeight(38)
        search_layout = QVBoxLayout(self._search_shell)
        search_layout.setContentsMargins(8, 5, 8, 5)
        search_layout.setSpacing(0)
        self._search_edit = _PairSearchEdit(self._search_shell)
        self._search_edit.setObjectName("scannerPairSearchEdit")
        self._search_edit.setPlaceholderText(tr("experimental.pair_search"))
        self._search_edit.textChanged.connect(self._on_search_changed)
        search_layout.addWidget(self._search_edit)
        layout.addWidget(self._search_shell, 0)

        self._canvas = _StarterRowsCanvas(self)
        self._canvas.setObjectName("experimentalStarterRowsCanvas")
        layout.addWidget(self._canvas, 1)

    def set_rows(self, rows: list[StarterRowViewModel]) -> None:
        self._all_rows = list(rows)
        self._apply_filter()

    def set_scroll_offset(self, offset: int) -> None:
        self._canvas.set_scroll_offset(offset)

    def content_height(self) -> int:
        return self._canvas.content_height()

    def _on_search_changed(self, text: str) -> None:
        self._search_text = str(text or "").strip().upper()
        self._apply_filter()

    def _apply_filter(self) -> None:
        if not self._search_text:
            self._canvas.set_rows(self._all_rows)
            return
        out: list[StarterRowViewModel] = []
        for row in self._all_rows:
            if row.kind == "separator":
                out.append(row)
                continue
            if self._search_text in row.canonical.upper():
                out.append(row)
        self._canvas.set_rows(out)


__all__ = ["StarterColumnView"]
