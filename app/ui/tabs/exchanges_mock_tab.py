from __future__ import annotations

from uuid import uuid4

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import QFrame, QHBoxLayout, QPushButton, QScrollArea, QVBoxLayout, QWidget

from app.ui.exchange_catalog import get_exchange_meta
from app.ui.exchange_store import load_exchange_cards, save_exchange_cards
from app.ui.i18n import tr
from app.ui.theme import theme_color
from app.ui.widgets.add_exchange_dialog import AddExchangeDialog
from app.ui.widgets.exchange_card_mock import ExchangeCardMock
from app.ui.widgets.exchange_picker_dialog import ExchangePickerDialog


class ExchangesMockTab(QWidget):
    action_triggered = Signal(str)

    def __init__(self, coordinator=None, parent=None) -> None:
        super().__init__(parent)
        self.coordinator = coordinator
        self.cards: list[ExchangeCardMock] = []
        self._connect_requests: dict[str, ExchangeCardMock] = {}
        self._close_requests: dict[str, ExchangeCardMock] = {}
        self._build_ui()
        self.apply_theme()
        self.retranslate_ui()
        if self.coordinator is not None:
            self.coordinator.exchange_connect_succeeded.connect(self._on_exchange_connect_succeeded)
            self.coordinator.exchange_connect_failed.connect(self._on_exchange_connect_failed)
            self.coordinator.exchange_snapshot_updated.connect(self._on_exchange_snapshot_updated)
            self.coordinator.exchange_snapshot_update_failed.connect(self._on_exchange_snapshot_update_failed)
            self.coordinator.exchange_close_positions_succeeded.connect(self._on_exchange_close_positions_succeeded)
            self.coordinator.exchange_close_positions_failed.connect(self._on_exchange_close_positions_failed)
        self._restore_cards()

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
        self.close_all_positions_btn.clicked.connect(self._close_all_positions)
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
        card.connect_clicked.connect(lambda _exchange_name, params, target=card: self._connect_card(target, params))
        card.disconnect_clicked.connect(self._handle_disconnect)
        card.close_positions_clicked.connect(lambda _exchange_name, target=card: self._close_card_positions(target))
        card.remove_clicked.connect(self._handle_remove)
        card.cancel_clicked.connect(lambda: self.action_triggered.emit("cancel_new_exchange"))
        card.edit_clicked.connect(lambda exchange_name: self.action_triggered.emit(f"{exchange_name}:edit"))

    def _create_card(self, exchange_code: str, payload: dict) -> None:
        meta = get_exchange_meta(exchange_code)
        card = ExchangeCardMock(meta["base_name"], exchange_code)
        card._monitor_id = uuid4().hex
        card.load_saved_data(payload)
        card._stored_payload = dict(payload)
        snapshot = payload.get("account_snapshot")
        if isinstance(snapshot, dict):
            card.apply_account_snapshot(snapshot)
        card.mark_connected(bool(payload.get("connected", True)), demo=bool(payload.get("testnet", False)))
        self._wire_card(card)
        self.cards.append(card)
        self.panels_layout.insertWidget(self.panels_layout.count() - 1, card)
        self._update_content_visibility()
        card.retranslate_ui()

    def _open_add_dialog(self) -> None:
        if self.coordinator is not None:
            exchange_codes = [code for code, _title in self.coordinator.available_exchanges()]
        else:
            exchange_codes = ["binance", "bybit"]
        picker = ExchangePickerDialog(exchange_codes, self)
        if picker.exec() != picker.DialogCode.Accepted or not picker.selected_code:
            return

        dialog = AddExchangeDialog(picker.selected_code, coordinator=self.coordinator, parent=self)
        if dialog.exec() != dialog.DialogCode.Accepted or not isinstance(dialog.payload, dict):
            return

        self._create_card(picker.selected_code, dialog.payload)
        self._save_cards()
        self.action_triggered.emit(f"{picker.selected_code}:added")

    def _connect_card(self, card: ExchangeCardMock, params: dict) -> None:
        if self.coordinator is None:
            card.mark_connected(True)
            self.action_triggered.emit(f"{card.exchange_name}:connect")
            return
        request_id = uuid4().hex
        self._connect_requests[request_id] = card
        card.set_busy(True)
        self.coordinator.connect_exchange_async(request_id, card.exchange_type, params)

    @staticmethod
    def _card_credentials(card: ExchangeCardMock) -> dict:
        return {
            "api_key": card.api_key_input.text().strip(),
            "api_secret": card.api_secret_input.text().strip(),
            "api_passphrase": card.passphrase_input.text().strip(),
            "testnet": False,
        }

    def _close_card_positions(self, card: ExchangeCardMock) -> None:
        if self.coordinator is None or not card.is_connected:
            return
        params = self._card_credentials(card)
        if not params["api_key"] or not params["api_secret"]:
            card.show_operation_error(tr("exchange.error.key_secret_required"))
            return
        request_id = uuid4().hex
        self._close_requests[request_id] = card
        card.set_busy_mode("close_positions")
        self.coordinator.close_exchange_positions_async(request_id, card.exchange_type, params)

    def _handle_disconnect(self, exchange_name: str) -> None:
        for card in self.cards:
            if card.exchange_name == exchange_name:
                if self.coordinator is not None:
                    self.coordinator.stop_exchange_monitor(getattr(card, "_monitor_id", ""))
                card.mark_connected(False)
                stored = getattr(card, "_stored_payload", {})
                stored["connected"] = False
                card._stored_payload = stored
                break
        self._save_cards()
        self.action_triggered.emit(f"{exchange_name}:disconnect")

    def _handle_remove(self, exchange_name: str) -> None:
        for index, card in enumerate(list(self.cards)):
            if card.exchange_name != exchange_name:
                continue
            if self.coordinator is not None:
                self.coordinator.stop_exchange_monitor(getattr(card, "_monitor_id", ""))
            self.cards.pop(index)
            card.setParent(None)
            card.deleteLater()
            self.action_triggered.emit(f"{exchange_name}:remove")
            break
        self._update_content_visibility()
        self._save_cards()

    def _connect_all_cards(self) -> None:
        for card in self.cards:
            params = self._card_credentials(card)
            self._connect_card(card, params)
        self.action_triggered.emit("connect_all")

    def _disconnect_all_cards(self) -> None:
        for card in self.cards:
            if self.coordinator is not None:
                self.coordinator.stop_exchange_monitor(getattr(card, "_monitor_id", ""))
            card.mark_connected(False)
        self._save_cards()
        self.action_triggered.emit("disconnect_all")

    def _close_all_positions(self) -> None:
        for card in self.cards:
            if card.is_connected:
                self._close_card_positions(card)
        self.action_triggered.emit("close_all_positions")

    def _on_exchange_connect_succeeded(self, request_id: str, exchange_code: str, snapshot: object) -> None:
        card = self._connect_requests.pop(request_id, None)
        if card is None or card.exchange_type != exchange_code or not isinstance(snapshot, dict):
            return
        card.apply_account_snapshot(snapshot)
        card.mark_connected(True)
        stored = dict(getattr(card, "_stored_payload", {}))
        stored["exchange_code"] = card.exchange_type
        stored["exchange_title"] = card.exchange_meta["title"]
        stored["exchange_name"] = card.exchange_meta["base_name"]
        stored["api_key"] = card.api_key_input.text().strip()
        stored["api_secret"] = card.api_secret_input.text().strip()
        stored["api_passphrase"] = card.passphrase_input.text().strip()
        stored["testnet"] = False
        stored["connected"] = True
        stored["account_snapshot"] = snapshot
        card._stored_payload = stored
        if self.coordinator is not None:
            self.coordinator.start_exchange_monitor(
                getattr(card, "_monitor_id", ""),
                card.exchange_type,
                self._card_credentials(card),
            )
        self._save_cards()
        self.action_triggered.emit(f"{card.exchange_name}:connect")

    def _on_exchange_connect_failed(self, request_id: str, exchange_code: str, message: str) -> None:
        card = self._connect_requests.pop(request_id, None)
        if card is None or card.exchange_type != exchange_code:
            return
        card.show_connection_error(message)

    def _on_exchange_snapshot_updated(self, monitor_id: str, exchange_code: str, snapshot: object) -> None:
        if not isinstance(snapshot, dict):
            return
        for card in self.cards:
            if getattr(card, "_monitor_id", None) != monitor_id or card.exchange_type != exchange_code:
                continue
            card.apply_account_snapshot(snapshot)
            stored = dict(getattr(card, "_stored_payload", {}))
            stored["connected"] = True
            stored["account_snapshot"] = snapshot
            card._stored_payload = stored
            self._save_cards()
            break

    def _on_exchange_snapshot_update_failed(self, monitor_id: str, exchange_code: str, message: str) -> None:
        for card in self.cards:
            if getattr(card, "_monitor_id", None) != monitor_id or card.exchange_type != exchange_code:
                continue
            card.show_operation_error(message)
            break

    def _on_exchange_close_positions_succeeded(self, request_id: str, exchange_code: str, payload: object) -> None:
        card = self._close_requests.pop(request_id, None)
        if card is None or card.exchange_type != exchange_code or not isinstance(payload, dict):
            return
        snapshot = payload.get("account_snapshot")
        if isinstance(snapshot, dict):
            card.apply_account_snapshot(snapshot)
        card.mark_connected(True)
        stored = dict(getattr(card, "_stored_payload", {}))
        stored["connected"] = True
        if isinstance(snapshot, dict):
            stored["account_snapshot"] = snapshot
        card._stored_payload = stored
        self._save_cards()
        self.action_triggered.emit(f"{card.exchange_name}:close_positions")

    def _on_exchange_close_positions_failed(self, request_id: str, exchange_code: str, message: str) -> None:
        card = self._close_requests.pop(request_id, None)
        if card is None or card.exchange_type != exchange_code:
            return
        card.show_operation_error(message)

    def _serialize_card(self, card: ExchangeCardMock) -> dict:
        stored = dict(getattr(card, "_stored_payload", {}))
        stored["exchange_code"] = card.exchange_type
        stored["exchange_title"] = card.exchange_meta["title"]
        stored["exchange_name"] = card.exchange_meta["base_name"]
        stored["api_key"] = card.api_key_input.text().strip()
        stored["api_secret"] = card.api_secret_input.text().strip()
        stored["api_passphrase"] = card.passphrase_input.text().strip()
        stored["testnet"] = False
        stored["connected"] = bool(card.is_connected)
        if isinstance(card._snapshot, dict):
            stored["account_snapshot"] = dict(card._snapshot)
        return stored

    def _save_cards(self) -> None:
        save_exchange_cards([self._serialize_card(card) for card in self.cards])

    def _restore_cards(self) -> None:
        for payload in load_exchange_cards():
            exchange_code = str(payload.get("exchange_code", "")).strip().lower()
            if not exchange_code:
                continue
            self._create_card(exchange_code, payload)
            if bool(payload.get("connected")):
                card = self.cards[-1]
                params = {
                    "api_key": str(payload.get("api_key", "")).strip(),
                    "api_secret": str(payload.get("api_secret", "")).strip(),
                    "api_passphrase": str(payload.get("api_passphrase", "")).strip(),
                    "testnet": False,
                }
                if params["api_key"] and params["api_secret"]:
                    self._connect_card(card, params)

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

