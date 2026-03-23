from __future__ import annotations

from PySide6.QtCore import Signal
from PySide6.QtWidgets import QVBoxLayout, QWidget

from app.futures_spread_scanner_v2.views.starter_column import StarterColumnView
from app.futures_spread_scanner_v2.runtime.starter_runtime import StarterPairsRuntime
from app.futures_spread_scanner_v2.runtime.view_models import StarterPairsViewModel
from app.futures_spread_scanner_v2.views.common import StarterRowViewModel, _runtime_widget_stylesheet


class StarterRuntimeWidget(QWidget):
    content_changed = Signal()
    wheel_scrolled = Signal(int)

    def __init__(self, runtime: StarterPairsRuntime, parent=None) -> None:
        super().__init__(parent)
        self.setStyleSheet(_runtime_widget_stylesheet())
        self._view_model = StarterPairsViewModel(runtime)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        self.column = StarterColumnView(self)
        layout.addWidget(self.column, 1)
        self.column._search_edit.textChanged.connect(self._view_model.set_search_text)
        self.column._canvas.bookmark_toggled.connect(self._view_model.toggle_bookmark)
        self.column._canvas.bookmark_drag_released.connect(self._on_bookmark_drag_released)
        self.column._canvas.wheel_scrolled.connect(self.wheel_scrolled.emit)
        self._view_model.changed.connect(self._apply_snapshot)
        self._apply_snapshot()

    def _apply_snapshot(self) -> None:
        snapshot = self._view_model.snapshot()
        rows: list[StarterRowViewModel] = [
            StarterRowViewModel(
                kind=row.kind,
                canonical=row.canonical,
                bookmarked=row.bookmarked,
            )
            for row in snapshot.rows
        ]
        self.column.set_rows(rows)
        self.content_changed.emit()

    def set_scroll_offset(self, offset: int) -> None:
        self.column.set_scroll_offset(offset)

    def content_height(self) -> int:
        return self.column.content_height()

    def _bookmark_drop_index_for_row(self, row_index: int) -> int | None:
        snapshot = self._view_model.snapshot()
        rows = list(getattr(snapshot, "rows", []) or [])
        if row_index < 0:
            return None
        bookmark_rows: list[int] = []
        for display_index, row in enumerate(rows):
            if getattr(row, "kind", "") == "separator":
                break
            if getattr(row, "bookmarked", False):
                bookmark_rows.append(display_index)
            else:
                break
        if not bookmark_rows:
            return None
        if row_index <= bookmark_rows[0]:
            return 0
        if row_index > bookmark_rows[-1]:
            return len(bookmark_rows) - 1
        for bookmark_index, display_index in enumerate(bookmark_rows):
            if row_index <= display_index:
                return bookmark_index
        return len(bookmark_rows) - 1

    def _on_bookmark_drag_released(self, canonical: str, row_index: int) -> None:
        target_index = self._bookmark_drop_index_for_row(row_index)
        self._view_model.reorder_bookmark(canonical, target_index)


__all__ = ["StarterRuntimeWidget", "StarterColumnView"]
