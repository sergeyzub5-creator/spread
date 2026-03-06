from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QApplication,
    QHBoxLayout,
    QLabel,
    QMainWindow,
    QMenu,
    QTabWidget,
    QToolButton,
    QVBoxLayout,
    QWidget,
)

from ui.tabs.placeholder_tab import PlaceholderTab
from ui.theme import build_app_stylesheet, get_theme_manager, theme_color
from ui.widgets.brand_header import NeonHeader, build_app_icon


class AppWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.theme_manager = get_theme_manager()
        self.theme_manager.theme_changed.connect(self._apply_theme)

        self.setWindowTitle("Spread Sniper UI Shell")
        self.setWindowIcon(build_app_icon())
        self.resize(1240, 780)

        root = QWidget()
        self.setCentralWidget(root)
        layout = QVBoxLayout(root)
        layout.setContentsMargins(10, 8, 10, 10)
        layout.setSpacing(6)

        layout.addLayout(self._build_header())

        self.tabs = QTabWidget()
        self.exchanges_tab = PlaceholderTab(
            "Биржи",
            "Новая оболочка для будущего экрана подключений. Сейчас здесь оставлен только визуал, структура и интерактивные точки.",
            ["Добавить биржу", "Импорт", "Макет карточек"],
        )
        self.spread_tab = PlaceholderTab(
            "Снайпинг спреда",
            "Каркас будущего торгового экрана. Старое ядро не используется, но ритм интерфейса и характер вкладки сохранены.",
            ["Открыть макет", "Панель стратегии", "Лента котировок"],
        )
        self.test_tab = PlaceholderTab(
            "Тест",
            "Песочница для быстрых экспериментов с новыми блоками интерфейса.",
            ["Показать блок", "Сменить состояние", "Сбросить"],
        )

        self.tabs.addTab(self.exchanges_tab, "Биржи")
        self.tabs.addTab(self.spread_tab, "Снайпинг спреда")
        self.tabs.addTab(self.test_tab, "Тест")
        self.tabs.currentChanged.connect(self._on_tab_changed)
        layout.addWidget(self.tabs)

        self.footer = QLabel("UI shell mode")
        self.footer.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        layout.addWidget(self.footer)

        for tab in (self.exchanges_tab, self.spread_tab, self.test_tab):
            tab.action_triggered.connect(self._on_tab_action)

        self._apply_theme()
        self._on_tab_changed(0)

    def _build_header(self) -> QHBoxLayout:
        row = QHBoxLayout()
        row.setContentsMargins(0, 0, 0, 0)
        row.setSpacing(10)

        self.left_slot = QWidget()
        row.addWidget(self.left_slot, 1)

        self.header = NeonHeader()
        row.addWidget(self.header, 0, Qt.AlignmentFlag.AlignCenter)

        self.right_slot = QWidget()
        right = QHBoxLayout(self.right_slot)
        right.setContentsMargins(0, 0, 0, 0)
        right.setSpacing(8)

        self.mode_badge = QLabel("UI SHELL")
        self.mode_badge.setAlignment(Qt.AlignmentFlag.AlignCenter)
        right.addWidget(self.mode_badge)

        self.theme_menu = QMenu(self)
        self.theme_btn = QToolButton()
        self.theme_btn.setText("Theme")
        self.theme_btn.setPopupMode(QToolButton.ToolButtonPopupMode.InstantPopup)
        self.theme_btn.setMenu(self.theme_menu)
        right.addWidget(self.theme_btn)

        row.addWidget(self.right_slot, 1, Qt.AlignmentFlag.AlignRight)
        self._rebuild_theme_menu()
        return row

    def _rebuild_theme_menu(self) -> None:
        self.theme_menu.clear()
        current = self.theme_manager.theme_name
        labels = {
            "dark": "Dark",
            "steel": "Steel",
            "graphite_pro": "Graphite Pro",
        }
        for name in self.theme_manager.available_themes():
            action = self.theme_menu.addAction(labels.get(name, name))
            action.setCheckable(True)
            action.setChecked(name == current)
            action.triggered.connect(lambda _checked=False, theme_name=name: self.theme_manager.set_theme(theme_name))

    def _apply_theme(self, _theme_name: str | None = None) -> None:
        self.setStyleSheet(build_app_stylesheet())
        self._rebuild_theme_menu()
        self.header.set_palette(theme_color("glow_a"), theme_color("glow_b"))
        self.mode_badge.setStyleSheet(
            f"""
            QLabel {{
                color: {theme_color('accent')};
                background-color: {theme_color('selection_bg_soft')};
                border: 1px solid {theme_color('border')};
                border-radius: 10px;
                padding: 7px 12px;
                font-size: 11px;
                font-weight: 800;
                letter-spacing: 0.6px;
            }}
            """
        )
        self.theme_btn.setStyleSheet(
            f"""
            QToolButton {{
                background-color: {theme_color('surface')};
                color: {theme_color('text_primary')};
                border: 1px solid {theme_color('border')};
                border-radius: 10px;
                padding: 7px 12px;
                font-weight: 700;
            }}
            QToolButton:hover {{
                background-color: {theme_color('surface_alt')};
            }}
            """
        )
        self.footer.setStyleSheet(
            f"color: {theme_color('text_muted')}; padding: 4px 2px; font-size: 12px; font-weight: 600;"
        )
        self.exchanges_tab.apply_theme()
        self.spread_tab.apply_theme()
        self.test_tab.apply_theme()

    def _on_tab_changed(self, index: int) -> None:
        title = self.tabs.tabText(index)
        self.footer.setText(f"Active tab: {title} | Mode: visual shell")

    def _on_tab_action(self, action_name: str) -> None:
        current_title = self.tabs.tabText(self.tabs.currentIndex())
        self.footer.setText(f"Active tab: {current_title} | Last action: {action_name}")
