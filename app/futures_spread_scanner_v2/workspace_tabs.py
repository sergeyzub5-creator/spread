from __future__ import annotations

from pathlib import Path

from PySide6.QtCore import QTimer
from PySide6.QtGui import QIcon
from PySide6.QtWidgets import QTabWidget, QToolButton, QVBoxLayout, QWidget
from PySide6.QtCore import Qt, QSize

from app.futures_spread_scanner_v2.common.logger import get_logger
from app.futures_spread_scanner_v2.constructor_tab import FuturesSpreadConstructorTab
from app.futures_spread_scanner_v2.manager import WorkspaceManager
from app.futures_spread_scanner_v2.notifications_tab import FuturesSpreadNotificationsTab
from app.futures_spread_scanner_v2.workspace_tab import FuturesSpreadWorkspaceTab
from app.futures_spread_scanner_v2.common.i18n import tr


class FuturesSpreadWorkspaceTabs(QWidget):
    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self._logger = get_logger("scanner.v2.tabs")
        self._manager = WorkspaceManager()
        self._workspace_tabs_by_id: dict[str, FuturesSpreadWorkspaceTab] = {}
        self._editor_tabs_by_id: dict[str, FuturesSpreadConstructorTab] = {}
        self._suspended_workspace_tabs_by_id: dict[str, FuturesSpreadWorkspaceTab] = {}
        self._plus_tab = FuturesSpreadConstructorTab(self)
        self._notifications_tab = FuturesSpreadNotificationsTab(self)
        self._notifications_button: QToolButton | None = None
        self._plus_tab.apply_requested.connect(self._create_workspace_from_constructor)
        self._manager.workspaces_changed.connect(self._rebuild_tabs)
        self._manager.active_workspace_changed.connect(self._sync_active_tab_from_manager)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        self.tabs = QTabWidget(self)
        self.tabs.setTabsClosable(False)
        self.tabs.currentChanged.connect(self._on_current_tab_changed)
        layout.addWidget(self.tabs)

        self._build_notifications_corner_button()

        self._rebuild_tabs()
        self._logger.info("workspace tabs init complete")

    def retranslate_ui(self) -> None:
        self._refresh_tab_titles()
        self._plus_tab.retranslate_ui()
        self._notifications_tab.retranslate_ui()
        if self._notifications_button is not None:
            self._notifications_button.setText(tr("tab.notifications"))

    def apply_theme(self) -> None:
        for tab in self._workspace_tabs_by_id.values():
            tab.apply_theme()
        for tab in self._suspended_workspace_tabs_by_id.values():
            tab.apply_theme()
        for tab in self._editor_tabs_by_id.values():
            tab.apply_theme()
        self._plus_tab.apply_theme()
        self._notifications_tab.apply_theme()
        self._apply_notifications_button_style()

    def _clear_workspace_tabs(self) -> None:
        while self.tabs.count():
            widget = self.tabs.widget(0)
            self.tabs.removeTab(0)
            if widget is not None and widget is not self._plus_tab and widget is not self._notifications_tab:
                widget.deleteLater()
        self._workspace_tabs_by_id.clear()
        for tab in self._suspended_workspace_tabs_by_id.values():
            tab.deleteLater()
        self._suspended_workspace_tabs_by_id.clear()
        self._editor_tabs_by_id.clear()

    def _rebuild_tabs(self) -> None:
        current_workspace_id = self._manager.active_workspace_id()
        desired_ids = [workspace.workspace_id for workspace in self._manager.workspaces()]
        self._logger.info(
            "rebuild tabs | desired=%s | existing=%s | active=%s",
            desired_ids,
            list(self._workspace_tabs_by_id),
            current_workspace_id,
        )

        for workspace_id, tab in list(self._workspace_tabs_by_id.items()):
            if workspace_id in desired_ids:
                continue
            index = self.tabs.indexOf(tab)
            if index >= 0:
                self.tabs.removeTab(index)
            tab.deleteLater()
            self._workspace_tabs_by_id.pop(workspace_id, None)
            self._logger.info("tab removed from ui | workspace_id=%s", workspace_id)

        for workspace_id, tab in list(self._suspended_workspace_tabs_by_id.items()):
            if workspace_id in desired_ids:
                continue
            tab.deleteLater()
            self._suspended_workspace_tabs_by_id.pop(workspace_id, None)
            self._logger.info("suspended tab removed | workspace_id=%s", workspace_id)

        for workspace_id, tab in list(self._editor_tabs_by_id.items()):
            if workspace_id in desired_ids:
                continue
            index = self.tabs.indexOf(tab)
            if index >= 0:
                self.tabs.removeTab(index)
            tab.deleteLater()
            self._editor_tabs_by_id.pop(workspace_id, None)
            self._logger.info("editor tab removed from ui | workspace_id=%s", workspace_id)

        plus_index = self.tabs.indexOf(self._plus_tab)
        if plus_index >= 0:
            self.tabs.removeTab(plus_index)
        notifications_index = self.tabs.indexOf(self._notifications_tab)
        if notifications_index >= 0:
            self.tabs.removeTab(notifications_index)

        for workspace in self._manager.workspaces():
            tab = self._editor_tabs_by_id.get(workspace.workspace_id)
            if tab is None:
                tab = self._workspace_tabs_by_id.get(workspace.workspace_id)
            if tab is None:
                tab = self._suspended_workspace_tabs_by_id.get(workspace.workspace_id)
            if tab is None:
                session = self._manager.session_for(workspace.workspace_id)
                if session is None:
                    continue
                workspace_tab = FuturesSpreadWorkspaceTab(session, self, manager=self._manager)
                workspace_tab.edit_requested.connect(self._enter_edit_mode)
                self._workspace_tabs_by_id[workspace.workspace_id] = workspace_tab
                tab = workspace_tab
                self._logger.info("tab created in ui | workspace_id=%s | title=%s", workspace.workspace_id, workspace.title)
            index = self.tabs.indexOf(tab)
            if index < 0:
                self.tabs.addTab(tab, workspace.title)
                self._logger.info("tab inserted | workspace_id=%s | index=%s", workspace.workspace_id, self.tabs.indexOf(tab))

        self.tabs.addTab(self._plus_tab, "+")
        self.tabs.addTab(self._notifications_tab, tr("tab.notifications"))
        notifications_index = self.tabs.indexOf(self._notifications_tab)
        if notifications_index >= 0:
            self.tabs.tabBar().setTabVisible(notifications_index, False)
        self._sync_active_tab_from_manager(current_workspace_id or "")
        self._refresh_tab_titles()

    def _refresh_tab_titles(self) -> None:
        for workspace_id, tab in self._workspace_tabs_by_id.items():
            workspace = self._manager.workspace_by_id(workspace_id)
            if workspace is None:
                continue
            index = self.tabs.indexOf(tab)
            if index >= 0:
                self.tabs.setTabText(index, workspace.title)
            tab.retranslate_ui()
        for workspace_id, tab in self._editor_tabs_by_id.items():
            workspace = self._manager.workspace_by_id(workspace_id)
            if workspace is None:
                continue
            index = self.tabs.indexOf(tab)
            if index >= 0:
                self.tabs.setTabText(index, workspace.title)
            tab.retranslate_ui()
        plus_index = self.tabs.indexOf(self._plus_tab)
        if plus_index >= 0:
            self.tabs.setTabText(plus_index, "+")
        notifications_index = self.tabs.indexOf(self._notifications_tab)
        if notifications_index >= 0:
            self.tabs.setTabText(notifications_index, tr("tab.notifications"))
        if self._notifications_button is not None:
            self._notifications_button.setText(tr("tab.notifications"))

    def _sync_active_tab_from_manager(self, workspace_id: str) -> None:
        normalized = str(workspace_id or "").strip()
        if normalized:
            widget = self._editor_tabs_by_id.get(normalized) or self._workspace_tabs_by_id.get(normalized)
        else:
            widget = None
        if widget is not None:
            index = self.tabs.indexOf(widget)
            if index >= 0 and self.tabs.currentIndex() != index:
                self.tabs.setCurrentIndex(index)
                self._logger.info("active ui tab synced | workspace_id=%s | index=%s", normalized, index)
            if self._notifications_button is not None:
                self._notifications_button.setChecked(False)
            return
        if self.tabs.count() > 0:
            self.tabs.setCurrentIndex(max(0, self.tabs.count() - 1 if not self._workspace_tabs_by_id else 0))

    def _on_current_tab_changed(self, index: int) -> None:
        widget = self.tabs.widget(index)
        self._logger.info("current tab changed | index=%s | widget_type=%s", index, type(widget).__name__ if widget else "None")
        if self._notifications_button is not None:
            self._notifications_button.setChecked(widget is self._notifications_tab)
        if widget is self._notifications_tab:
            return
        for workspace_id, tab in self._workspace_tabs_by_id.items():
            if tab is widget:
                self._manager.set_active_workspace(workspace_id)
                return
        for workspace_id, tab in self._editor_tabs_by_id.items():
            if tab is widget:
                self._manager.set_active_workspace(workspace_id)
                return

    def _create_workspace_from_constructor(self) -> None:
        self._logger.info(
            "constructor apply requested | title=%s | top=%s | nodes=%s",
            self._plus_tab.draft().effective_title(),
            self._plus_tab.top_volume_limit(),
            len(self._plus_tab.draft().nodes()),
        )
        definition = self._manager.create_or_update_from_draft(
            self._plus_tab.draft(),
            top_volume_limit=self._plus_tab.top_volume_limit(),
            make_active=True,
        )
        self._plus_tab.reset_draft()
        self._sync_active_tab_from_manager(definition.workspace_id)
        self._logger.info("constructor apply complete | workspace_id=%s", definition.workspace_id)

    def _enter_edit_mode(self, workspace_id: str) -> None:
        normalized = str(workspace_id or "").strip()
        if not normalized:
            return
        if normalized in self._editor_tabs_by_id:
            self._sync_active_tab_from_manager(normalized)
            return
        QTimer.singleShot(0, lambda workspace_id=normalized: self._finish_enter_edit_mode(workspace_id))

    def _finish_enter_edit_mode(self, workspace_id: str) -> None:
        normalized = str(workspace_id or "").strip()
        if not normalized:
            return
        if normalized in self._editor_tabs_by_id:
            self._sync_active_tab_from_manager(normalized)
            return
        definition = self._manager.workspace_by_id(normalized)
        if definition is None:
            return
        old_tab = self._workspace_tabs_by_id.pop(normalized, None)
        insert_index = self.tabs.indexOf(old_tab) if old_tab is not None else -1
        if insert_index >= 0 and old_tab is not None:
            self.tabs.removeTab(insert_index)
            self._suspended_workspace_tabs_by_id[normalized] = old_tab
        editor_tab = FuturesSpreadConstructorTab(self)
        editor_tab.load_definition(definition)
        editor_tab.apply_requested.connect(lambda workspace_id=normalized: self._apply_workspace_edit(workspace_id))
        editor_tab.delete_requested.connect(self._delete_workspace_from_editor)
        editor_tab.cancel_requested.connect(self._cancel_workspace_edit)
        self._editor_tabs_by_id[normalized] = editor_tab
        if insert_index < 0:
            plus_index = self.tabs.indexOf(self._plus_tab)
            insert_index = plus_index if plus_index >= 0 else self.tabs.count()
        self.tabs.insertTab(insert_index, editor_tab, definition.title)
        self.tabs.setCurrentIndex(insert_index)
        self._logger.info("workspace entered edit mode | workspace_id=%s | index=%s", normalized, insert_index)

    def _apply_workspace_edit(self, workspace_id: str) -> None:
        normalized = str(workspace_id or "").strip()
        editor_tab = self._editor_tabs_by_id.get(normalized)
        if editor_tab is None:
            return
        if not editor_tab.draft().is_valid():
            return
        QTimer.singleShot(0, lambda workspace_id=normalized: self._finish_apply_workspace_edit(workspace_id))

    def _finish_apply_workspace_edit(self, workspace_id: str) -> None:
        normalized = str(workspace_id or "").strip()
        editor_tab = self._editor_tabs_by_id.get(normalized)
        if editor_tab is None:
            return
        index = self.tabs.indexOf(editor_tab)
        if index >= 0:
            self.tabs.removeTab(index)
        self._editor_tabs_by_id.pop(normalized, None)
        suspended_tab = self._suspended_workspace_tabs_by_id.pop(normalized, None)
        definition = self._manager.create_or_update_from_draft(
            editor_tab.draft(),
            top_volume_limit=editor_tab.top_volume_limit(),
            workspace_id=normalized,
            make_active=True,
        )
        editor_tab.deleteLater()
        if suspended_tab is not None:
            suspended_tab.deleteLater()
        self._sync_active_tab_from_manager(definition.workspace_id)
        self._logger.info("workspace edit applied | workspace_id=%s", normalized)

    def _delete_workspace_from_editor(self, workspace_id: str) -> None:
        normalized = str(workspace_id or "").strip()
        QTimer.singleShot(0, lambda workspace_id=normalized: self._finish_delete_workspace_from_editor(workspace_id))

    def _finish_delete_workspace_from_editor(self, workspace_id: str) -> None:
        normalized = str(workspace_id or "").strip()
        editor_tab = self._editor_tabs_by_id.pop(normalized, None)
        if editor_tab is not None:
            index = self.tabs.indexOf(editor_tab)
            if index >= 0:
                self.tabs.removeTab(index)
            editor_tab.deleteLater()
        suspended_tab = self._suspended_workspace_tabs_by_id.pop(normalized, None)
        if suspended_tab is not None:
            suspended_tab.deleteLater()
        self._manager.delete_workspace(normalized)
        self._logger.info("workspace deleted from editor | workspace_id=%s", normalized)

    def _cancel_workspace_edit(self, workspace_id: str) -> None:
        normalized = str(workspace_id or "").strip()
        if not normalized:
            return
        QTimer.singleShot(0, lambda workspace_id=normalized: self._finish_cancel_workspace_edit(workspace_id))

    def _finish_cancel_workspace_edit(self, workspace_id: str) -> None:
        normalized = str(workspace_id or "").strip()
        editor_tab = self._editor_tabs_by_id.pop(normalized, None)
        suspended_tab = self._suspended_workspace_tabs_by_id.pop(normalized, None)
        if editor_tab is None or suspended_tab is None:
            return
        index = self.tabs.indexOf(editor_tab)
        if index >= 0:
            self.tabs.removeTab(index)
        editor_tab.deleteLater()
        self._workspace_tabs_by_id[normalized] = suspended_tab
        if index < 0:
            plus_index = self.tabs.indexOf(self._plus_tab)
            index = plus_index if plus_index >= 0 else self.tabs.count()
        title = self._manager.workspace_by_id(normalized).title if self._manager.workspace_by_id(normalized) else normalized
        self.tabs.insertTab(index, suspended_tab, title)
        self.tabs.setCurrentIndex(index)
        self._logger.info("workspace edit canceled | workspace_id=%s | index=%s", normalized, index)

    def closeEvent(self, event) -> None:  # type: ignore[override]
        self._logger.info("workspace tabs close")
        self._manager.dispose()
        super().closeEvent(event)

    def _build_notifications_corner_button(self) -> None:
        self._notifications_button = QToolButton(self.tabs)
        self._notifications_button.setObjectName("v2NotificationsCornerBtn")
        self._notifications_button.setCheckable(True)
        self._notifications_button.setAutoRaise(True)
        self._notifications_button.setToolButtonStyle(Qt.ToolButtonStyle.ToolButtonTextBesideIcon)
        icon_path = Path(__file__).resolve().parent / "assets" / "logos" / "alarm.svg"
        self._notifications_button.setIcon(QIcon(str(icon_path)))
        self._notifications_button.setIconSize(QSize(16, 16))
        self._notifications_button.clicked.connect(self._open_notifications_tab)
        self.tabs.setCornerWidget(self._notifications_button, Qt.Corner.TopRightCorner)
        self._notifications_button.setText(tr("tab.notifications"))
        self._apply_notifications_button_style()

    def _apply_notifications_button_style(self) -> None:
        if self._notifications_button is None:
            return
        self._notifications_button.setStyleSheet(
            """
            QToolButton#v2NotificationsCornerBtn {
                color: #d8e1ea;
                background-color: transparent;
                border: 1px solid #334155;
                border-bottom: none;
                border-top-left-radius: 8px;
                border-top-right-radius: 8px;
                padding: 7px 12px;
                font-weight: 700;
            }
            QToolButton#v2NotificationsCornerBtn:hover {
                background-color: #11161d;
                border: 1px solid #415166;
                border-bottom: none;
            }
            QToolButton#v2NotificationsCornerBtn:checked {
                background-color: #161d27;
                border: 1px solid #7aa2ff;
                border-bottom: none;
                color: #f1f5fb;
            }
            """
        )

    def _open_notifications_tab(self) -> None:
        index = self.tabs.indexOf(self._notifications_tab)
        if index >= 0:
            self.tabs.setCurrentIndex(index)


__all__ = ["FuturesSpreadWorkspaceTabs"]
