from __future__ import annotations

from PySide6.QtCore import QSize, Qt
from PySide6.QtGui import QFont
from PySide6.QtWidgets import QDialog, QFrame, QHBoxLayout, QLabel, QListWidget, QListWidgetItem, QPushButton, QVBoxLayout

from ui.exchange_catalog import EXCHANGE_ORDER, get_exchange_meta
from ui.i18n import tr
from ui.theme import button_style, theme_color
from ui.widgets.exchange_badge import build_exchange_icon


class ExchangePickerDialog(QDialog):
    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self.selected_code: str | None = None
        self.setWindowTitle(tr("exchange.picker_title"))
        self.setMinimumSize(350, 364)
        self.resize(371, 416)
        self._build_ui()

    @staticmethod
    def _rgba(hex_color: str, alpha: float) -> str:
        color = str(hex_color or "").strip()
        if color.startswith("#") and len(color) == 7:
            r = int(color[1:3], 16)
            g = int(color[3:5], 16)
            b = int(color[5:7], 16)
            a = max(0, min(255, int(round(max(0.0, min(1.0, alpha)) * 255))))
            return f"rgba({r}, {g}, {b}, {a})"
        return color

    def _build_ui(self) -> None:
        frame_top = self._rgba(theme_color("surface_alt"), 0.98)
        frame_bottom = self._rgba(theme_color("window_bg"), 0.98)
        frame_border = self._rgba(theme_color("border"), 0.78)
        soft_border = self._rgba(theme_color("border"), 0.40)
        hover_bg = self._rgba(theme_color("surface_alt"), 0.84)
        selected_bg = self._rgba(theme_color("accent"), 0.12)
        selected_border = self._rgba(theme_color("accent"), 0.48)
        separator = self._rgba(theme_color("border"), 0.28)

        self.setStyleSheet(
            f"""
            QDialog {{
                background-color: {theme_color('surface')};
                color: {theme_color('text_primary')};
            }}
            QFrame#pickerFrame {{
                background: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1, stop: 0 {frame_top}, stop: 1 {frame_bottom});
                border: 2px solid {frame_border};
                border-radius: 16px;
            }}
            QLabel#pickerInfo {{
                color: {theme_color('text_muted')};
                font-size: 11px;
                font-weight: 600;
            }}
            QFrame#pickerSeparator {{
                background-color: {separator};
                min-height: 1px;
                max-height: 1px;
                border: none;
            }}
            QListWidget {{
                background-color: {theme_color('window_bg')};
                border: 1px solid {soft_border};
                border-radius: 12px;
                padding: 6px;
                color: {theme_color('text_primary')};
                font-size: 13px;
                outline: none;
            }}
            QListWidget::item {{
                padding: 9px 12px;
                border-radius: 10px;
                margin: 2px 0px;
                border: 1px solid transparent;
            }}
            QListWidget::item:hover {{
                background-color: {hover_bg};
                border-color: {soft_border};
            }}
            QListWidget::item:selected {{
                background-color: {selected_bg};
                color: {theme_color('accent')};
                border-color: {selected_border};
            }}
            """
        )

        root = QVBoxLayout(self)
        root.setContentsMargins(6, 6, 6, 6)
        root.setSpacing(0)

        self.frame = QFrame()
        self.frame.setObjectName("pickerFrame")
        root.addWidget(self.frame)

        layout = QVBoxLayout(self.frame)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(8)

        info = QLabel(tr("exchange.picker_subtitle"))
        info.setObjectName("pickerInfo")
        layout.addWidget(info)

        self.list_widget = QListWidget()
        self.list_widget.itemDoubleClicked.connect(lambda _item: self._accept_selected())
        self.list_widget.setIconSize(QSize(31, 31))
        self.list_widget.setSpacing(2)
        list_font = QFont("Segoe UI")
        list_font.setPointSize(11)
        list_font.setWeight(QFont.Weight.DemiBold)
        self.list_widget.setFont(list_font)
        for code in EXCHANGE_ORDER:
            meta = get_exchange_meta(code)
            item = QListWidgetItem(build_exchange_icon(code, size=31), meta["title"])
            item.setData(Qt.ItemDataRole.UserRole, code)
            self.list_widget.addItem(item)
        self.list_widget.setCurrentRow(0)
        layout.addWidget(self.list_widget)

        separator_line = QFrame()
        separator_line.setObjectName("pickerSeparator")
        layout.addWidget(separator_line)

        buttons = QHBoxLayout()
        buttons.setContentsMargins(0, 2, 0, 0)
        buttons.setSpacing(6)
        self.add_btn = QPushButton(tr("exchange.picker_add"))
        self.add_btn.setStyleSheet(button_style("primary"))
        self.add_btn.setMinimumHeight(30)
        self.add_btn.clicked.connect(self._accept_selected)
        buttons.addWidget(self.add_btn)

        cancel_btn = QPushButton(tr("common.cancel"))
        cancel_btn.setStyleSheet(button_style("secondary"))
        cancel_btn.setMinimumHeight(30)
        cancel_btn.clicked.connect(self.reject)
        buttons.addWidget(cancel_btn)
        buttons.addStretch(1)
        layout.addLayout(buttons)

    def _accept_selected(self) -> None:
        item = self.list_widget.currentItem()
        if item is None:
            return
        self.selected_code = item.data(Qt.ItemDataRole.UserRole)
        self.accept()
