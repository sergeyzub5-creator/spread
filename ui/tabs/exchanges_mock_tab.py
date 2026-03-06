from __future__ import annotations

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import QFrame, QHBoxLayout, QPushButton, QScrollArea, QVBoxLayout, QWidget

from ui.exchange_catalog import get_exchange_meta
from ui.i18n import tr
from ui.theme import theme_color
from ui.widgets.add_exchange_dialog import AddExchangeDialog
from ui.widgets.exchange_card_mock import ExchangeCardMock
from ui.widgets.exchange_picker_dialog import ExchangePickerDialog


class ExchangesMockTab(QWidget):
    action_triggered = Signal(str)

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self.cards: list[ExchangeCardMock] = []
        self._build_ui()
        self.apply_theme()
        self.retranslate_ui()

    @staticmethod
    def _rgba(hex_color: str, alpha: float) -> str:
        color = str(hex_color or "").strip()
        if color.startswith("#") and len(color) == 7:
            r = int(color[1:3], 16)
            g = int(color[3:5], 16)
            b = int(color[5:7], 16)
            return f"rgba({r}, {g}, {b}, {max(0.0, min(1.0, alpha)):.3f})"
        return color

    def _soft_button_style(self, role: str, bold: bool = False) -> str:
        roles = {
            "primary": (
                self._rgba(theme_color("accent"), 0.14),
                self._rgba(theme_color("accent"), 0.56),
                theme_color("text_primary"),
                self._rgba(theme_color("accent"), 0.22),
            ),
            "success": (
                self._rgba(theme_color("success"), 0.14),
                self._rgba(theme_color("success"), 0.56),
                theme_color("text_primary"),
                self._rgba(theme_color("success"), 0.22),
            ),
            "danger": (
                self._rgba(theme_color("danger"), 0.14),
                self._rgba(theme_color("danger"), 0.56),
                theme_color("text_primary"),
                self._rgba(theme_color("danger"), 0.22),
            ),
            "warning": (
                self._rgba(theme_color("warning"), 0.14),
                self._rgba(theme_color("warning"), 0.58),
                theme_color("text_primary"),
                self._rgba(theme_color("warning"), 0.24),
            ),
        }
        bg, border, text, hover = roles[role]
        weight = "700" if bold else "600"
        return (
            f"QPushButton {{ background-color: {bg}; color: {text}; border: 1px solid {border}; border-radius: 11px; padding: 6px 12px; font-weight: {weight}; }}"
            f" QPushButton:hover {{ background-color: {hover}; border-color: {border}; }}"
        )

    def _build_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(4, 4, 4, 4)
        root.setSpacing(0)

        self.main_frame = QFrame()
        self.main_frame.setObjectName("exchangesMainFrame")
        root.addWidget(self.main_frame)

        layout = QVBoxLayout(self.main_frame)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(8)

        controls = QHBoxLayout()
        controls.setSpacing(10)
        self.add_btn = QPushButton()
        self.connect_all_btn = QPushButton()
        self.disconnect_all_btn = QPushButton()
        self.close_all_positions_btn = QPushButton()
        self.add_btn.clicked.connect(self._open_add_dialog)
        self.connect_all_btn.clicked.connect(self._connect_all_cards)
        self.disconnect_all_btn.clicked.connect(self._disconnect_all_cards)
        self.close_all_positions_btn.clicked.connect(lambda: self.action_triggered.emit("close_all_positions"))
        controls.addWidget(self.add_btn)
        controls.addStretch(1)
        controls.addWidget(self.connect_all_btn)
        controls.addWidget(self.disconnect_all_btn)
        controls.addWidget(self.close_all_positions_btn)
        layout.addLayout(controls)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        scroll.setObjectName("exchangesScroll")

        container = QWidget()
        self.panels_layout = QVBoxLayout(container)
        self.panels_layout.setContentsMargins(0, 0, 0, 0)
        self.panels_layout.setSpacing(8)
        self.panels_layout.setAlignment(Qt.AlignmentFlag.AlignTop)
        self.panels_layout.addStretch(1)
        scroll.setWidget(container)
        layout.addWidget(scroll)

        self._update_content_visibility()

    def _wire_card(self, card: ExchangeCardMock) -> None:
        card.connect_clicked.connect(lambda exchange_name, _params: self.action_triggered.emit(f"{exchange_name}:connect"))
        card.disconnect_clicked.connect(self._handle_disconnect)
        card.close_positions_clicked.connect(lambda exchange_name: self.action_triggered.emit(f"{exchange_name}:close_positions"))
        card.remove_clicked.connect(self._handle_remove)
        card.cancel_clicked.connect(lambda: self.action_triggered.emit("cancel_new_exchange"))
        card.edit_clicked.connect(lambda exchange_name: self.action_triggered.emit(f"{exchange_name}:edit"))

    def _create_card(self, exchange_code: str, payload: dict) -> None:
        meta = get_exchange_meta(exchange_code)
        card = ExchangeCardMock(meta["base_name"], exchange_code)
        card.load_saved_data(payload)
        card.mark_connected(True, demo=bool(payload.get("testnet", False)))
        self._wire_card(card)
        self.cards.append(card)
        self.panels_layout.insertWidget(self.panels_layout.count() - 1, card)
        self._update_content_visibility()
        card.retranslate_ui()

    def _open_add_dialog(self) -> None:
        picker = ExchangePickerDialog(self)
        if picker.exec() != picker.DialogCode.Accepted or not picker.selected_code:
            return

        dialog = AddExchangeDialog(picker.selected_code, self)
        if dialog.exec() != dialog.DialogCode.Accepted or not isinstance(dialog.payload, dict):
            return

        self._create_card(picker.selected_code, dialog.payload)
        self.action_triggered.emit(f"{picker.selected_code}:added")

    def _handle_disconnect(self, exchange_name: str) -> None:
        for card in self.cards:
            if card.exchange_name == exchange_name:
                card.mark_connected(False)
                break
        self.action_triggered.emit(f"{exchange_name}:disconnect")

    def _handle_remove(self, exchange_name: str) -> None:
        for index, card in enumerate(list(self.cards)):
            if card.exchange_name != exchange_name:
                continue
            self.cards.pop(index)
            card.setParent(None)
            card.deleteLater()
            self.action_triggered.emit(f"{exchange_name}:remove")
            break
        self._update_content_visibility()

    def _connect_all_cards(self) -> None:
        for card in self.cards:
            card.mark_connected(True, demo=card.testnet_check.isChecked())
        self.action_triggered.emit("connect_all")

    def _disconnect_all_cards(self) -> None:
        for card in self.cards:
            card.mark_connected(False)
        self.action_triggered.emit("disconnect_all")

    def _update_content_visibility(self) -> None:
        has_cards = bool(self.cards)
        self.connect_all_btn.setVisible(has_cards)
        self.disconnect_all_btn.setVisible(has_cards)
        self.close_all_positions_btn.setVisible(has_cards)

    def retranslate_ui(self) -> None:
        self.add_btn.setText(tr("exchanges.add"))
        self.connect_all_btn.setText(tr("exchanges.connect_all"))
        self.disconnect_all_btn.setText(tr("exchanges.disconnect_all"))
        self.close_all_positions_btn.setText(tr("exchanges.close_all_positions"))
        for card in self.cards:
            card.retranslate_ui()

    def apply_theme(self) -> None:
        frame_top = self._rgba(theme_color("surface_alt"), 0.96)
        frame_bottom = self._rgba(theme_color("window_bg"), 0.98)
        frame_border = self._rgba(theme_color("border"), 0.58)
        self.setStyleSheet(
            f"""
            QFrame#exchangesMainFrame {{
                background: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1, stop: 0 {frame_top}, stop: 1 {frame_bottom});
                border: 1px solid {frame_border};
                border-radius: 12px;
            }}
            QScrollArea#exchangesScroll {{
                border: none;
                background: transparent;
            }}
            """
        )
        self.add_btn.setStyleSheet(self._soft_button_style("primary", bold=True))
        self.connect_all_btn.setStyleSheet(self._soft_button_style("success"))
        self.disconnect_all_btn.setStyleSheet(self._soft_button_style("danger"))
        self.close_all_positions_btn.setStyleSheet(self._soft_button_style("warning", bold=True))
        for card in self.cards:
            card.apply_theme()
