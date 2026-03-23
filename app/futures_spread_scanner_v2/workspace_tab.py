from __future__ import annotations

from PySide6.QtCore import QEvent, Qt, Signal
from PySide6.QtGui import QColor
from PySide6.QtWidgets import QHBoxLayout, QScrollBar, QSizePolicy, QVBoxLayout, QWidget

from app.futures_spread_scanner_v2.manager import WorkspaceManager
from app.futures_spread_scanner_v2.session import WorkspaceSession
from app.futures_spread_scanner_v2.views import (
    BaseExchangeRuntimeWidget,
    OutputRuntimeWidget,
    StarterRuntimeWidget,
    WorkspaceHeaderRuntimeWidget,
)
from app.futures_spread_scanner_v2.common.theme import THEMES


class FuturesSpreadWorkspaceTab(QWidget):
    edit_requested = Signal(str)

    def __init__(self, session: WorkspaceSession, parent=None, *, manager: WorkspaceManager | None = None) -> None:
        super().__init__(parent)
        self._session = session
        self._manager = manager
        self._runtime_widgets: list[QWidget] = []
        self._last_persisted_sort_state: tuple[str | None, str | None, str | None, bool] | None = None
        self._last_persisted_bookmarks_by_starter: dict[str, tuple[str, ...]] = {}
        self._last_persisted_top_volume_limit: int | None = None

        root = QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(12)

        self._header_widget = WorkspaceHeaderRuntimeWidget(
            self._session.header_runtime(),
            self,
            show_settings_button=True,
        )
        self._header_widget.settings_requested.connect(self._on_edit_requested)
        root.addWidget(self._header_widget, 0)

        self._content = QHBoxLayout()
        self._content.setContentsMargins(0, 0, 0, 0)
        self._content.setSpacing(12)

        for binding in self._session.column_bindings():
            widget = self._build_runtime_widget(binding.node_id, binding.role)
            if widget is None:
                continue
            widget.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
            self._content.addWidget(widget, 0)
            self._content.setStretch(
                self._content.count() - 1,
                self._session.workspace_runtime().stretch_for_role(binding.role),
            )
            self._runtime_widgets.append(widget)

        self._scroll_bar = QScrollBar(self)
        self._scroll_bar.setObjectName("scannerExternalScrollBar")
        self._scroll_bar.setOrientation(Qt.Orientation.Vertical)
        self._scroll_bar.setFixedWidth(12)
        self._scroll_bar.valueChanged.connect(self._apply_scroll_value)
        self._content.addWidget(self._scroll_bar, 0)
        root.addLayout(self._content, 1)

        for widget in [self._header_widget, *self._runtime_widgets, self._scroll_bar]:
            widget.installEventFilter(self)
        for runtime_widget in self._runtime_widgets:
            if hasattr(runtime_widget, "wheel_scrolled"):
                runtime_widget.wheel_scrolled.connect(self._on_runtime_wheel_scrolled)
            if hasattr(runtime_widget, "content_changed"):
                runtime_widget.content_changed.connect(self._refresh_scroll_metrics)
        self._session.workspace_runtime().snapshot_changed.connect(self._persist_sort_state_if_needed)
        self._session.header_runtime().snapshot_changed.connect(self._persist_top_volume_if_needed)
        for starter_node_id, starter_runtime in self._session.starter_runtimes().items():
            starter_runtime.snapshot_changed.connect(
                lambda starter_node_id=starter_node_id: self._persist_bookmarks_if_needed(starter_node_id)
            )

        self.apply_theme()
        self._refresh_scroll_metrics()
        self._persist_top_volume_if_needed()
        self._persist_sort_state_if_needed()
        for starter_node_id in self._session.starter_runtimes():
            self._persist_bookmarks_if_needed(starter_node_id)

    def definition(self):
        return self._session.definition()

    def workspace_id(self) -> str:
        return self._session.definition().workspace_id

    def retranslate_ui(self) -> None:
        self._header_widget.retranslate_ui()

    def apply_theme(self) -> None:
        self.setObjectName("scannerExperimentalTab")
        d = THEMES["dark"]
        surface_alt = d["surface_alt"]
        border = d["border"]
        accent_bg = d["accent_bg"]
        accent_bg_hover = d["accent_bg_hover"]
        self.setStyleSheet(
            f"""
            QWidget#scannerExperimentalTab {{
                background-color: {d["window_bg"]};
            }}
            QScrollBar#scannerExternalScrollBar:vertical {{
                background: {QColor(surface_alt).darker(120).name()};
                width: 12px;
                margin: 34px 0 0 0;
                border: 1px solid {border};
                border-radius: 6px;
            }}
            QScrollBar#scannerExternalScrollBar::handle:vertical {{
                background: {QColor(accent_bg).lighter(115).name()};
                min-height: 32px;
                border-radius: 5px;
            }}
            QScrollBar#scannerExternalScrollBar::handle:vertical:hover {{
                background: {QColor(accent_bg_hover).lighter(110).name()};
            }}
            QScrollBar#scannerExternalScrollBar::sub-line:vertical,
            QScrollBar#scannerExternalScrollBar::add-line:vertical {{
                height: 0px;
                background: transparent;
                border: none;
            }}
            QScrollBar#scannerExternalScrollBar::sub-page:vertical,
            QScrollBar#scannerExternalScrollBar::add-page:vertical {{
                background: transparent;
            }}
            """
        )

    def eventFilter(self, watched, event):  # type: ignore[override]
        watch_set = {self._header_widget, *self._runtime_widgets, self._scroll_bar}
        if event.type() == QEvent.Type.Wheel and watched in watch_set:
            delta = int(event.angleDelta().y())
            if delta:
                self._on_runtime_wheel_scrolled(delta)
                event.accept()
                return True
        if event.type() == QEvent.Type.Resize and watched in set(self._runtime_widgets):
            self._refresh_scroll_metrics()
        return super().eventFilter(watched, event)

    def resizeEvent(self, event) -> None:  # type: ignore[override]
        super().resizeEvent(event)
        self._refresh_scroll_metrics()

    def wheelEvent(self, event) -> None:  # type: ignore[override]
        delta = int(event.angleDelta().y())
        if delta:
            self._on_runtime_wheel_scrolled(delta)
            event.accept()
            return
        super().wheelEvent(event)

    def _build_runtime_widget(self, node_id: str, role: str) -> QWidget | None:
        runtime = self._session.runtime_for_node(node_id)
        if runtime is None:
            return None
        normalized_role = str(role or "").strip().lower()
        if normalized_role == "starter":
            return StarterRuntimeWidget(runtime, self)
        if normalized_role == "base":
            binding = next((item for item in self._session.column_bindings() if item.node_id == node_id), None)
            starter_runtime = self._session.runtime_for_node(binding.anchor_starter_id if binding is not None else "")
            if starter_runtime is None:
                return None
            return BaseExchangeRuntimeWidget(
                runtime,
                str(getattr(binding, "exchange_id", "") or getattr(runtime.snapshot(), "exchange_id", "")),
                starter_runtime,
                self._session.workspace_runtime().comparison_runtime(),
                self._session.workspace_runtime(),
                self,
            )
        if normalized_role == "output":
            widget = OutputRuntimeWidget(runtime, self._session.workspace_runtime(), self)
            runtime_id = str(getattr(runtime.snapshot(), "runtime_id", "") or "").strip().lower()
            if runtime_id.startswith("rate_delta::") or runtime_id == "rate_delta":
                widget.column.setMinimumWidth(66)
            return widget
        return None

    def _on_runtime_wheel_scrolled(self, delta: int) -> None:
        step = 52
        if delta > 0:
            self._scroll_bar.setValue(max(self._scroll_bar.minimum(), self._scroll_bar.value() - step))
        elif delta < 0:
            self._scroll_bar.setValue(min(self._scroll_bar.maximum(), self._scroll_bar.value() + step))

    def _apply_scroll_value(self, value: int) -> None:
        for widget in self._runtime_widgets:
            if hasattr(widget, "set_scroll_offset"):
                widget.set_scroll_offset(value)

    def _refresh_scroll_metrics(self) -> None:
        if not self._runtime_widgets:
            self._scroll_bar.setRange(0, 0)
            return
        viewport_height = max(int(widget.height()) for widget in self._runtime_widgets)
        content_height = max(
            int(widget.content_height()) for widget in self._runtime_widgets if hasattr(widget, "content_height")
        )
        maximum = max(0, content_height - viewport_height)
        self._scroll_bar.setPageStep(max(1, viewport_height))
        self._scroll_bar.setSingleStep(52)
        self._scroll_bar.setRange(0, maximum)
        self._apply_scroll_value(self._scroll_bar.value())

    def _on_edit_requested(self) -> None:
        self.edit_requested.emit(self.workspace_id())

    def _persist_sort_state_if_needed(self) -> None:
        workspace_runtime = self._session.workspace_runtime()
        snapshot = workspace_runtime.snapshot()
        next_state = (
            str(getattr(snapshot, "sort_role", "") or "").strip().lower() or None,
            str(getattr(snapshot, "sort_source_id", "") or "").strip().lower() or None,
            str(getattr(snapshot, "sort_key", "") or "").strip() or None,
            bool(getattr(snapshot, "sort_descending", True)),
        )
        if next_state == self._last_persisted_sort_state:
            return
        self._last_persisted_sort_state = next_state
        if self._manager is None:
            return
        self._manager.update_workspace_sort_state(
            self.workspace_id(),
            sort_role=next_state[0],
            sort_source_id=next_state[1],
            sort_key=next_state[2],
            sort_descending=next_state[3],
        )

    def _persist_bookmarks_if_needed(self, starter_node_id: str) -> None:
        starter_runtime = self._session.starter_runtimes().get(str(starter_node_id or "").strip())
        if starter_runtime is None:
            return
        bookmark_order = tuple(starter_runtime.bookmark_order())
        if self._last_persisted_bookmarks_by_starter.get(starter_node_id) == bookmark_order:
            return
        self._last_persisted_bookmarks_by_starter[starter_node_id] = bookmark_order
        if self._manager is None:
            return
        self._manager.update_workspace_bookmarks(
            self.workspace_id(),
            starter_node_id=starter_node_id,
            bookmark_order=bookmark_order,
        )

    def _persist_top_volume_if_needed(self) -> None:
        if self._manager is None:
            return
        next_top = int(self._session.header_runtime().top_volume_limit())
        if self._last_persisted_top_volume_limit == next_top:
            return
        self._last_persisted_top_volume_limit = next_top
        self._manager.update_workspace_top_volume_limit(
            self.workspace_id(),
            top_volume_limit=next_top,
        )


__all__ = ["FuturesSpreadWorkspaceTab"]
