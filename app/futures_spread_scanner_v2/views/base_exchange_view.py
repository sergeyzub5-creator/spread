from __future__ import annotations

import time

from PySide6.QtCore import QTimer, Signal
from PySide6.QtWidgets import QVBoxLayout, QWidget

from app.futures_spread_scanner_v2.views.base_exchange_column import BaseExchangeColumnView
from app.futures_spread_scanner_v2.runtime.comparison_runtime import BaseComparisonRuntime
from app.futures_spread_scanner_v2.runtime.contracts import BasePerpRuntime, PerpSnapshot
from app.futures_spread_scanner_v2.runtime.starter_runtime import StarterPairsRuntime
from app.futures_spread_scanner_v2.runtime.view_models import (
    BaseComparisonViewModel,
    PerpColumnViewModel,
    StarterPairsViewModel,
)
from app.futures_spread_scanner_v2.runtime.workspace_runtime import WorkspaceRuntime
from app.futures_spread_scanner_v2.views.common import BaseExchangeRowViewModel, _runtime_widget_stylesheet
from app.futures_spread_scanner_v2.common.i18n import tr


def _sort_suffix(descending: bool) -> str:
    return "?" if descending else "?"


class BaseExchangeRuntimeWidget(QWidget):
    content_changed = Signal()
    wheel_scrolled = Signal(int)

    def __init__(
        self,
        runtime: BasePerpRuntime,
        exchange_id: str,
        starter_runtime: StarterPairsRuntime,
        comparison_runtime: BaseComparisonRuntime,
        workspace_runtime: WorkspaceRuntime,
        parent=None,
    ) -> None:
        super().__init__(parent)
        self.setStyleSheet(_runtime_widget_stylesheet())
        self._exchange_id = str(exchange_id or "").strip().lower()
        if not self._exchange_id:
            raise ValueError("BaseExchangeRuntimeWidget requires a non-empty exchange_id")
        self._starter_runtime = starter_runtime
        self._workspace_runtime = workspace_runtime
        self._starter_view_model = StarterPairsViewModel(starter_runtime)
        self._comparison_view_model = BaseComparisonViewModel(comparison_runtime)
        self._funding_sort_descending = True
        self._funding_sort_active = False
        self._sort_throttle_ms = 1000
        self._last_sort_push_at = 0.0
        self._sort_update_pending = False
        self._last_pushed_sort_values: dict[str, float] = {}
        self._active_sort_timer = QTimer(self)
        self._active_sort_timer.setSingleShot(True)
        self._active_sort_timer.timeout.connect(self._flush_pending_sort)

        initial_snapshot = runtime.snapshot() if hasattr(runtime, "snapshot") else None
        initial_title = str(getattr(initial_snapshot, "title", "") or self._exchange_id.title())

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        self.column = BaseExchangeColumnView(initial_title, self._exchange_id, self)
        layout.addWidget(self.column, 1)
        self._view_model = PerpColumnViewModel(runtime)
        self.column._canvas.wheel_scrolled.connect(self.wheel_scrolled.emit)
        self.column._funding_header.clicked.connect(self._on_funding_header_clicked)
        self._view_model.changed.connect(self._apply_snapshot)
        self._view_model.changed.connect(self._request_active_sort_refresh)
        self._starter_view_model.changed.connect(self._apply_snapshot)
        self._comparison_view_model.changed.connect(self._apply_snapshot)
        self._workspace_runtime.snapshot_changed.connect(self._sync_sort_state_from_workspace)
        self._sync_sort_state_from_workspace()
        self._apply_snapshot()
        if self._funding_sort_active:
            self._request_active_sort_refresh()

    def _apply_snapshot(self) -> None:
        snapshot: PerpSnapshot = self._view_model.snapshot()
        starter_snapshot = self._starter_view_model.snapshot()
        self.column._title_label.setText(snapshot.title)
        self.column.set_status_ok(bool(snapshot.status_ok))
        self.column.set_status_hint(getattr(snapshot, "status_hint", ""))
        funding_header_text = tr("scanner.subcol_funding")
        if self._funding_sort_active:
            funding_header_text = f'{funding_header_text} {"↓" if self._funding_sort_descending else "↑"}'
        self.column._funding_header.setText(funding_header_text)
        rows_by_canonical = {
            str(row.canonical or "").strip().upper(): row
            for row in snapshot.rows
            if getattr(row, "kind", "") == "row"
        }
        ordered_rows: list[BaseExchangeRowViewModel] = []
        for starter_row in starter_snapshot.rows:
            if starter_row.kind == "separator":
                ordered_rows.append(
                    BaseExchangeRowViewModel(
                        kind="separator",
                        canonical="",
                        volume_usdt=0,
                        price_text="-",
                        accent=None,
                        funding_text="-",
                        timer_text="-",
                    )
                )
                continue
            canonical = str(starter_row.canonical or "").strip().upper()
            row = rows_by_canonical.get(canonical)
            if row is None:
                continue
            row.accent = self._comparison_view_model.accent_for(self._exchange_id, canonical)
            ordered_rows.append(row)
        self.column.set_rows(ordered_rows)
        self.content_changed.emit()

    def set_scroll_offset(self, offset: int) -> None:
        self.column.set_scroll_offset(offset)

    def content_height(self) -> int:
        return self.column.content_height()

    def _on_funding_header_clicked(self) -> None:
        if self._funding_sort_active:
            self._funding_sort_descending = not self._funding_sort_descending
        else:
            self._funding_sort_active = True
            self._funding_sort_descending = True
        self._push_funding_sort()
        self._apply_snapshot()

    def _current_funding_sort_values(self) -> dict[str, float]:
        snapshot: PerpSnapshot = self._view_model.snapshot()
        return {
            str(row.canonical or "").strip().upper(): float(row.funding_sort_value)
            for row in snapshot.rows
            if getattr(row, "kind", "") == "row" and getattr(row, "funding_sort_value", None) is not None
        }

    def _push_funding_sort(self) -> None:
        sort_values = self._current_funding_sort_values()
        self._last_sort_push_at = time.monotonic()
        self._sort_update_pending = False
        self._last_pushed_sort_values = dict(sort_values)
        self._workspace_runtime.set_external_sort(
            "base",
            self._exchange_id,
            f"{self._exchange_id}_funding",
            sort_values,
            descending=self._funding_sort_descending,
        )

    def _sync_sort_state_from_workspace(self) -> None:
        workspace_snapshot = self._workspace_runtime.snapshot()
        is_active = (
            str(getattr(workspace_snapshot, "sort_role", "") or "").strip().lower() == "base"
            and str(getattr(workspace_snapshot, "sort_source_id", "") or "").strip().lower() == self._exchange_id
        )
        next_descending = bool(getattr(workspace_snapshot, "sort_descending", True))
        changed = (
            is_active != self._funding_sort_active
            or (is_active and next_descending != self._funding_sort_descending)
        )
        self._funding_sort_active = is_active
        if is_active:
            self._funding_sort_descending = next_descending
        else:
            self._sort_update_pending = False
            self._active_sort_timer.stop()
            self._last_pushed_sort_values = {}
        if changed:
            self._apply_snapshot()

    def _request_active_sort_refresh(self) -> None:
        if not self._funding_sort_active:
            return
        current_values = self._current_funding_sort_values()
        if current_values == self._last_pushed_sort_values:
            return
        elapsed_ms = int((time.monotonic() - self._last_sort_push_at) * 1000)
        if elapsed_ms >= self._sort_throttle_ms:
            self._push_funding_sort()
            return
        self._sort_update_pending = True
        remaining_ms = max(1, self._sort_throttle_ms - elapsed_ms)
        self._active_sort_timer.start(remaining_ms)

    def _flush_pending_sort(self) -> None:
        if not self._funding_sort_active or not self._sort_update_pending:
            return
        self._push_funding_sort()


__all__ = ["BaseExchangeRuntimeWidget", "BaseExchangeColumnView"]
