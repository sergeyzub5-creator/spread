from __future__ import annotations

from PySide6.QtCore import QTimer, Qt
from PySide6.QtGui import QCloseEvent
from PySide6.QtWidgets import QApplication, QHBoxLayout, QLabel, QMainWindow, QMenu, QSizePolicy, QTabWidget, QToolButton, QVBoxLayout, QWidget

from ui.i18n import get_language_manager, tr
from ui.tabs.exchanges_mock_tab import ExchangesMockTab
from ui.tabs.placeholder_tab import PlaceholderTab
from ui.tabs.spread_mock_tab import SpreadMockTab
from ui.theme import build_app_stylesheet, get_theme_manager, theme_color
from ui.widgets.brand_header import NeonHeader, build_app_icon
from ui.widgets.startup_splash import ShutdownSplash
from ui.widgets.status_bar import NetworkStatusBar


class AppWindow(QMainWindow):
    def __init__(self, coordinator=None) -> None:
        super().__init__()
        self.coordinator = coordinator
        self.language_manager = get_language_manager()
        self.language_manager.language_changed.connect(self._retranslate_ui)
        self.theme_manager = get_theme_manager()
        self.theme_manager.theme_changed.connect(self._apply_theme)
        self._shutdown_splash: ShutdownSplash | None = None
        self._closing = False

        self.setWindowTitle(tr("app.window_title"))
        self.setWindowIcon(build_app_icon())
        self.resize(1240, 780)

        root = QWidget()
        self.setCentralWidget(root)
        layout = QVBoxLayout(root)
        layout.setContentsMargins(10, 8, 10, 10)
        layout.setSpacing(6)

        self._create_top_controls(layout)

        self.tabs = QTabWidget()
        self.exchanges_tab = ExchangesMockTab()
        self.spread_tab = SpreadMockTab(coordinator=self.coordinator)
        self.test_tab = PlaceholderTab(
            "placeholder.test_title",
            "placeholder.test_subtitle",
            ["placeholder.action.show_block", "placeholder.action.change_state", "placeholder.action.reset"],
        )
        self.tabs.addTab(self.exchanges_tab, tr("tab.exchanges"))
        self.tabs.addTab(self.spread_tab, tr("tab.spread"))
        self.tabs.addTab(self.test_tab, tr("tab.test"))
        self.tabs.currentChanged.connect(self._on_tab_changed)
        layout.addWidget(self.tabs)

        self.status_bar = NetworkStatusBar()
        layout.addWidget(self.status_bar)

        for tab in (self.exchanges_tab, self.spread_tab, self.test_tab):
            tab.action_triggered.connect(self._on_tab_action)

        self._apply_theme()
        self._on_tab_changed(0)

    def _create_top_controls(self, parent_layout: QVBoxLayout) -> None:
        top_row = QHBoxLayout()
        top_row.setContentsMargins(0, 0, 0, 0)
        top_row.setSpacing(10)

        self.left_slot = QWidget()
        self.left_slot.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        top_row.addWidget(self.left_slot, 1)

        self.header = NeonHeader()
        top_row.addWidget(self.header, 0, Qt.AlignmentFlag.AlignCenter)

        self.right_slot = QWidget()
        self.right_slot.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        self.top_controls = QHBoxLayout(self.right_slot)
        self.top_controls.setContentsMargins(0, 0, 0, 0)
        self.top_controls.setSpacing(8)

        self.language_group = QWidget()
        self.language_group.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
        self.language_group_layout = QHBoxLayout(self.language_group)
        self.language_group_layout.setContentsMargins(0, 0, 0, 0)
        self.language_group_layout.setSpacing(0)

        self.language_code_label = QLabel(self.language_manager.language().upper())
        self.language_code_label.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        self.language_code_label.setFixedWidth(16)
        self.language_group_layout.addWidget(self.language_code_label)

        self.language_menu = QMenu(self)
        self.language_btn = QToolButton()
        self.language_btn.setText("\U0001F310")
        self.language_btn.setPopupMode(QToolButton.ToolButtonPopupMode.InstantPopup)
        self.language_btn.setMenu(self.language_menu)
        self.language_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self.language_btn.setFixedSize(22, 22)
        self.language_group_layout.addWidget(self.language_btn)
        self.top_controls.addWidget(self.language_group, 0, Qt.AlignmentFlag.AlignRight)

        self.settings_menu = QMenu(self)
        self.settings_btn = QToolButton()
        self.settings_btn.setPopupMode(QToolButton.ToolButtonPopupMode.InstantPopup)
        self.settings_btn.setMenu(self.settings_menu)
        self.settings_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self.settings_btn.setMinimumWidth(130)
        self.settings_btn.setFixedHeight(24)
        self.top_controls.addWidget(self.settings_btn, 0, Qt.AlignmentFlag.AlignRight)

        top_row.addWidget(self.right_slot, 1)
        parent_layout.addLayout(top_row)

        self._build_language_menu()
        self._build_settings_menu()
        self._sync_header_side_widths()

    def _sync_header_side_widths(self) -> None:
        self.right_slot.adjustSize()
        self.left_slot.setMinimumWidth(self.right_slot.sizeHint().width())

    def _build_language_menu(self) -> None:
        self.language_menu.clear()
        current_language = self.language_manager.language()
        for code in self.language_manager.available_languages():
            action = self.language_menu.addAction(tr(f"language.{code}"))
            action.setCheckable(True)
            action.setChecked(code == current_language)
            action.triggered.connect(lambda _checked=False, selected=code: self.language_manager.set_language(selected))

    def _build_settings_menu(self) -> None:
        self.settings_menu.clear()
        themes_submenu = self.settings_menu.addMenu(tr("top.themes"))
        current_theme = self.theme_manager.theme_name
        for code, label_key in (("dark", "theme.dark"), ("steel", "theme.steel"), ("graphite_pro", "theme.graphite_pro")):
            action = themes_submenu.addAction(tr(label_key))
            action.setCheckable(True)
            action.setChecked(code == current_theme)
            action.triggered.connect(lambda _checked=False, theme_code=code: self.theme_manager.set_theme(theme_code))

        compact_menu_qss = f"""
            QMenu {{
                background-color: {theme_color('surface')};
                color: {theme_color('text_primary')};
                border: 1px solid {theme_color('border')};
                border-radius: 8px;
                padding: 3px;
                font-size: 11px;
                font-weight: 600;
            }}
            QMenu::item {{
                padding: 3px 8px;
                border-radius: 5px;
            }}
            QMenu::item:selected {{
                background-color: {theme_color('selection_bg_soft')};
                color: {theme_color('accent')};
            }}
        """
        self.settings_menu.setStyleSheet(compact_menu_qss)
        themes_submenu.setStyleSheet(compact_menu_qss)

    def _apply_theme(self, _theme_name: str | None = None) -> None:
        self.setStyleSheet(build_app_stylesheet())
        self._build_language_menu()
        self._build_settings_menu()
        self.header.set_palette(theme_color("glow_a"), theme_color("glow_b"))

        self.language_btn.setStyleSheet(
            f"""
            QToolButton {{
                background-color: {theme_color('surface')};
                color: {theme_color('text_primary')};
                border: 1px solid {theme_color('border')};
                border-radius: 8px;
                font-size: 12px;
                font-weight: 600;
                padding: 0;
                margin: 0;
            }}
            QToolButton::menu-indicator {{
                image: none;
                width: 0px;
            }}
            QToolButton:hover {{
                background-color: {theme_color('surface_alt')};
            }}
            """
        )

        self.language_code_label.setStyleSheet(
            f"""
            QLabel {{
                color: {theme_color('text_primary')};
                font-size: 12px;
                font-weight: 700;
                padding: 0;
                margin: 0;
            }}
            """
        )

        self.settings_btn.setStyleSheet(
            f"""
            QToolButton {{
                background-color: {theme_color('surface')};
                color: {theme_color('text_primary')};
                border: 1px solid {theme_color('border')};
                border-radius: 8px;
                font-size: 12px;
                font-weight: 600;
                padding: 1px 12px;
            }}
            QToolButton::menu-indicator {{
                image: none;
                width: 0px;
            }}
            QToolButton:hover {{
                background-color: {theme_color('surface_alt')};
            }}
            """
        )

        self._retranslate_ui()
        self._sync_header_side_widths()
        self.exchanges_tab.apply_theme()
        self.spread_tab.apply_theme()
        self.test_tab.apply_theme()
        self.status_bar.apply_theme()

    def _retranslate_ui(self, _language: str | None = None) -> None:
        self.setWindowTitle(tr("app.window_title"))
        self.language_code_label.setText(self.language_manager.language().upper())
        self.language_btn.setToolTip(tr("top.language_tooltip"))
        self.settings_btn.setText(tr("top.settings"))
        self.tabs.setTabText(0, tr("tab.exchanges"))
        self.tabs.setTabText(1, tr("tab.spread"))
        self.tabs.setTabText(2, tr("tab.test"))
        self._build_language_menu()
        self._build_settings_menu()
        self.exchanges_tab.retranslate_ui()
        self.spread_tab.retranslate_ui()
        self.test_tab.retranslate_ui()
        self.status_bar.retranslate_ui()
        current_index = self.tabs.currentIndex()
        if current_index >= 0:
            self.status_bar.show_message(tr("status.active_tab", name=self.tabs.tabText(current_index)), timeout_ms=1800)

    def _on_tab_changed(self, index: int) -> None:
        self.status_bar.show_message(tr("status.active_tab", name=self.tabs.tabText(index)), timeout_ms=1800)

    def _on_tab_action(self, action_name: str) -> None:
        self.status_bar.show_message(action_name, timeout_ms=1800)

    def resizeEvent(self, event) -> None:
        super().resizeEvent(event)
        self._sync_header_side_widths()

    def closeEvent(self, event: QCloseEvent) -> None:
        if self._closing:
            event.accept()
            return

        self._closing = True
        event.ignore()
        self.hide()

        if self._shutdown_splash is None:
            self._shutdown_splash = ShutdownSplash()
        self.status_bar.stop_background_tasks()
        self._shutdown_splash.start()
        self._shutdown_splash.finish()
        QTimer.singleShot(430, QApplication.instance().quit)
