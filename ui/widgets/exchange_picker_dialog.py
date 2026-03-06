from __future__ import annotations

from PySide6.QtCore import QSize, Qt
from PySide6.QtWidgets import QDialog, QHBoxLayout, QLabel, QListWidget, QListWidgetItem, QPushButton, QVBoxLayout

from ui.exchange_catalog import EXCHANGE_ORDER, get_exchange_meta
from ui.i18n import tr
from ui.theme import button_style, theme_color
from ui.widgets.exchange_badge import build_exchange_icon


class ExchangePickerDialog(QDialog):
    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self.selected_code: str | None = None
        self.setWindowTitle(tr("exchange.picker_title"))
        self.setMinimumSize(500, 420)
        self.resize(540, 460)
        self._build_ui()

    def _build_ui(self) -> None:
        self.setStyleSheet(
            f"""
            QDialog {{
                background-color: {theme_color('surface')};
                color: {theme_color('text_primary')};
            }}
            QLabel {{
                color: {theme_color('text_primary')};
                font-size: 14px;
                font-weight: bold;
            }}
            QListWidget {{
                background-color: {theme_color('window_bg')};
                border: 1px solid {theme_color('border')};
                border-radius: 10px;
                padding: 6px;
                color: {theme_color('text_primary')};
                font-size: 13px;
                outline: none;
            }}
            QListWidget::item {{
                padding: 10px 12px;
                border-radius: 8px;
            }}
            QListWidget::item:hover {{
                background-color: {theme_color('surface_alt')};
            }}
            QListWidget::item:selected {{
                background-color: {theme_color('selection_bg_soft')};
                color: {theme_color('accent')};
            }}
            """
        )

        layout = QVBoxLayout(self)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(10)
        layout.addWidget(QLabel(tr("exchange.picker_subtitle")))

        self.list_widget = QListWidget()
        self.list_widget.itemDoubleClicked.connect(lambda _item: self._accept_selected())
        self.list_widget.setIconSize(QSize(31, 31))
        self.list_widget.setSpacing(4)
        for code in EXCHANGE_ORDER:
            meta = get_exchange_meta(code)
            item = QListWidgetItem(build_exchange_icon(code, size=31), meta["title"])
            item.setData(Qt.ItemDataRole.UserRole, code)
            self.list_widget.addItem(item)
        self.list_widget.setCurrentRow(0)
        layout.addWidget(self.list_widget)

        buttons = QHBoxLayout()
        buttons.addStretch()
        self.add_btn = QPushButton(tr("exchange.picker_add"))
        self.add_btn.setStyleSheet(button_style("primary"))
        self.add_btn.clicked.connect(self._accept_selected)
        buttons.addWidget(self.add_btn)

        cancel_btn = QPushButton(tr("common.cancel"))
        cancel_btn.setStyleSheet(button_style("secondary"))
        cancel_btn.clicked.connect(self.reject)
        buttons.addWidget(cancel_btn)
        layout.addLayout(buttons)

    def _accept_selected(self) -> None:
        item = self.list_widget.currentItem()
        if item is None:
            return
        self.selected_code = item.data(Qt.ItemDataRole.UserRole)
        self.accept()
