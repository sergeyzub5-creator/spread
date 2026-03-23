from __future__ import annotations

import time

from PySide6.QtCore import QTimer, Signal
from PySide6.QtWidgets import QVBoxLayout, QWidget

from app.futures_spread_scanner_v2.views.output_column import OutputColumnView
from app.futures_spread_scanner_v2.runtime.contracts import BaseOutputRuntime, OutputSnapshot
from app.futures_spread_scanner_v2.runtime.view_models import OutputColumnViewModel
from app.futures_spread_scanner_v2.runtime.workspace_runtime import WorkspaceRuntime
from app.futures_spread_scanner_v2.views.common import _runtime_widget_stylesheet
from app.futures_spread_scanner_v2.common.i18n import tr


def _sort_suffix(descending: bool) -> str:
    return "?" if descending else "?"


class OutputRuntimeWidget(QWidget):
    content_changed = Signal()
    wheel_scrolled = Signal(int)

    def __init__(
        self,
        runtime: BaseOutputRuntime,
        workspace_runtime: WorkspaceRuntime,
        parent=None,
    ) -> None:
        super().__init__(parent)
        self.setStyleSheet(_runtime_widget_stylesheet())
        self._workspace_runtime = workspace_runtime
        self._sort_descending = True
        self._sort_active = False
        self._view_model = OutputColumnViewModel(runtime)
        self._sort_throttle_ms = 1000
        self._last_sort_push_at = 0.0
        self._sort_update_pending = False
        self._last_pushed_sort_values: dict[str, float] = {}
        self._active_sort_timer = QTimer(self)
        self._active_sort_timer.setSingleShot(True)
        self._active_sort_timer.timeout.connect(self._flush_pending_sort)

        initial_snapshot = self._view_model.snapshot()
        initial_title = str(getattr(initial_snapshot, "title", "") or tr("scanner.col_annual"))

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        self.column = OutputColumnView(initial_title, self)
        layout.addWidget(self.column, 1)
        self.column._canvas.wheel_scrolled.connect(self.wheel_scrolled.emit)
        self.column._value_header.clicked.connect(self._on_header_clicked)
        self._view_model.changed.connect(self._apply_snapshot)
        self._view_model.changed.connect(self._request_active_sort_refresh)
        self._workspace_runtime.snapshot_changed.connect(self._sync_sort_state_from_workspace)
        self._sync_sort_state_from_workspace()
        self._apply_snapshot()
        if self._sort_active:
            self._request_active_sort_refresh()

    def _apply_snapshot(self) -> None:
        snapshot: OutputSnapshot = self._view_model.snapshot()
        self.column._title_label.setText(snapshot.title)
        header_text = snapshot.title
        if self._sort_active:
            header_text = f'{header_text} {"↓" if self._sort_descending else "↑"}'
        self.column._value_header.setText(header_text)
        self.column.set_rows(list(snapshot.rows))
        self.content_changed.emit()

    def set_scroll_offset(self, offset: int) -> None:
        self.column.set_scroll_offset(offset)

    def content_height(self) -> int:
        return self.column.content_height()

    def _on_header_clicked(self) -> None:
        if self._sort_active:
            self._sort_descending = not self._sort_descending
        else:
            self._sort_active = True
            self._sort_descending = True
        self._push_output_sort()
        self._apply_snapshot()

    def _current_output_sort_values(self) -> dict[str, float]:
        snapshot: OutputSnapshot = self._view_model.snapshot()
        return {
            str(row.canonical or "").strip().upper(): float(row.sort_value)
            for row in snapshot.rows
            if getattr(row, "kind", "") == "row" and getattr(row, "sort_value", None) is not None
        }

    def _push_output_sort(self) -> None:
        snapshot: OutputSnapshot = self._view_model.snapshot()
        sort_values = self._current_output_sort_values()
        self._last_sort_push_at = time.monotonic()
        self._sort_update_pending = False
        self._last_pushed_sort_values = dict(sort_values)
        self._workspace_runtime.set_external_sort(
            "output",
            str(getattr(snapshot, "runtime_id", "") or "output"),
            str(getattr(snapshot, "runtime_id", "") or "output"),
            sort_values,
            descending=self._sort_descending,
        )

    def _sync_sort_state_from_workspace(self) -> None:
        workspace_snapshot = self._workspace_runtime.snapshot()
        snapshot: OutputSnapshot = self._view_model.snapshot()
        runtime_id = str(getattr(snapshot, "runtime_id", "") or "").strip().lower()
        is_active = (
            str(getattr(workspace_snapshot, "sort_role", "") or "").strip().lower() == "output"
            and str(getattr(workspace_snapshot, "sort_source_id", "") or "").strip().lower() == runtime_id
        )
        next_descending = bool(getattr(workspace_snapshot, "sort_descending", True))
        changed = is_active != self._sort_active or (is_active and next_descending != self._sort_descending)
        self._sort_active = is_active
        if is_active:
            self._sort_descending = next_descending
        else:
            self._sort_update_pending = False
            self._active_sort_timer.stop()
            self._last_pushed_sort_values = {}
        if changed:
            self._apply_snapshot()

    def _request_active_sort_refresh(self) -> None:
        if not self._sort_active:
            return
        current_values = self._current_output_sort_values()
        if current_values == self._last_pushed_sort_values:
            return
        elapsed_ms = int((time.monotonic() - self._last_sort_push_at) * 1000)
        if elapsed_ms >= self._sort_throttle_ms:
            self._push_output_sort()
            return
        self._sort_update_pending = True
        remaining_ms = max(1, self._sort_throttle_ms - elapsed_ms)
        self._active_sort_timer.start(remaining_ms)

    def _flush_pending_sort(self) -> None:
        if not self._sort_active or not self._sort_update_pending:
            return
        self._push_output_sort()


__all__ = ["OutputRuntimeWidget", "OutputColumnView"]
