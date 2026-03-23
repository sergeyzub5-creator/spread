from __future__ import annotations

from PySide6.QtCore import Signal
from PySide6.QtWidgets import QVBoxLayout, QWidget

from app.futures_spread_scanner_v2.runtime.header_runtime import WorkspaceHeaderRuntime
from app.futures_spread_scanner_v2.runtime.view_models import WorkspaceHeaderViewModel
from app.futures_spread_scanner_v2.views.common import _runtime_widget_stylesheet
from app.futures_spread_scanner_v2.common.i18n import tr
from app.futures_spread_scanner_v2.common.workspace_header import WorkspaceHeaderBar


class WorkspaceHeaderRuntimeWidget(QWidget):
    settings_requested = Signal()

    def __init__(self, runtime: WorkspaceHeaderRuntime, parent=None, *, show_settings_button: bool = False) -> None:
        super().__init__(parent)
        self.setStyleSheet(_runtime_widget_stylesheet())
        self._view_model = WorkspaceHeaderViewModel(runtime)
        self._edit = None
        self._daily_volume_threshold = None

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        self.bar = WorkspaceHeaderBar(self, edit_host=self)
        self.bar.settings_btn.setVisible(bool(show_settings_button))
        layout.addWidget(self.bar, 0)
        self._edit = self.bar.edit

        self.bar.apply_btn.clicked.connect(self._on_refresh_clicked)
        self.bar.add_notification_btn.clicked.connect(self._on_add_notification_clicked)
        self.bar.edit.editingFinished.connect(self._on_top_volume_edit_finished)
        self.bar.settings_btn.clicked.connect(self.settings_requested.emit)

        self.retranslate_ui()
        self._view_model.changed.connect(self._apply_snapshot)
        self._apply_snapshot()

    def retranslate_ui(self) -> None:
        self.bar.label.setText(tr("scanner.volume_filter_label"))
        self.bar.apply_btn.setText(tr("scanner.refresh"))
        self.bar.add_notification_btn.setText(tr("scanner.add_notification"))
        self.bar.edit.setPlaceholderText(tr("scanner.volume_filter_placeholder"))
        self._apply_snapshot()

    def _edit_style(self, *, readonly: bool) -> str:
        if readonly:
            bg, border = "#11161d", "#334155"
        else:
            bg, border = "#161d27", "#7aa2ff"
        return (
            "QLineEdit#scannerVolumeEdit {"
            f"background-color:{bg};"
            "color:#f1f5fb;"
            f"border:1px solid {border};"
            "border-radius:12px;"
            "padding:4px 10px;"
            "font-weight:600;"
            "font-size:11px;"
            "min-height:14px;"
            "}"
            "QLineEdit#scannerVolumeEdit::placeholder { color:#8ea0b6; }"
        )

    def _apply_readonly_style(self) -> None:
        self.bar.edit.setStyleSheet(self._edit_style(readonly=True))

    def _apply_editable_style(self) -> None:
        self.bar.edit.setStyleSheet(self._edit_style(readonly=False))

    def _on_volume_edit_focus_out(self) -> None:
        if self.bar.edit.isReadOnly():
            return
        self._on_top_volume_edit_finished()

    def _apply_snapshot(self) -> None:
        snapshot = self._view_model.snapshot()
        self._daily_volume_threshold = snapshot.top_volume_text
        if self.bar.edit.text().strip() != snapshot.top_volume_text:
            self.bar.edit.setText(snapshot.top_volume_text)
        self.bar.pairs_status_label.setText(snapshot.pairs_status_text)
        self.bar.apply_btn.setEnabled(bool(snapshot.refresh_enabled))
        self.bar.add_notification_btn.setEnabled(bool(snapshot.add_notification_enabled))
        self.bar.apply_btn.setText(tr("scanner.refreshing") if snapshot.loading else tr("scanner.refresh"))
        if self.bar.edit.isReadOnly():
            self._apply_readonly_style()

    def _on_top_volume_edit_finished(self) -> None:
        self._view_model.set_top_volume_text(self.bar.edit.text())
        self.bar.edit.setReadOnly(True)
        self._apply_readonly_style()
        self._apply_snapshot()

    def _on_refresh_clicked(self) -> None:
        self._view_model.set_top_volume_text(self.bar.edit.text())
        self._view_model.request_refresh()
        self._apply_snapshot()

    def _on_add_notification_clicked(self) -> None:
        self._view_model.request_notification()


__all__ = ["WorkspaceHeaderRuntimeWidget"]
