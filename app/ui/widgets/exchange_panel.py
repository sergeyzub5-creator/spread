from __future__ import annotations

from PySide6.QtCore import Signal
from PySide6.QtWidgets import QFrame

from app.ui.exchange_catalog import get_exchange_meta, normalize_exchange_code, requires_passphrase
from app.ui.exchange_store import resolve_exchange_card_credentials
from app.ui.widgets.exchange_panel_parts import ExchangePanelPartsMixin
from app.ui.i18n import tr


class ExchangePanel(ExchangePanelPartsMixin, QFrame):
    connect_clicked = Signal(str, dict)
    disconnect_clicked = Signal(str)
    close_positions_clicked = Signal(str)
    remove_clicked = Signal(str)
    cancel_clicked = Signal()
    edit_clicked = Signal(str)

    def __init__(self, exchange_name: str, exchange_type: str, is_new: bool = False, parent=None) -> None:
        super().__init__(parent)
        self.exchange_name = exchange_name
        self.exchange_type = normalize_exchange_code(exchange_type)
        self.exchange_meta = get_exchange_meta(self.exchange_type)
        self.is_connected = False
        self.testnet = False
        self.is_new = is_new
        self.edit_mode = is_new
        self._busy = False
        self._busy_mode: str | None = None
        self._snapshot: dict | None = None
        self._last_error_message: str | None = None
        self._credentials_stored = False
        self._stored_api_key_masked = ""
        self._init_ui()
        self.apply_theme()
        self._update_ui_state()
        self.retranslate_ui()

    def _on_connect(self) -> None:
        api_key = self.api_key_input.text().strip()
        api_secret = self.api_secret_input.text().strip()
        passphrase = self.passphrase_input.text().strip()
        if (not api_key or not api_secret) and isinstance(getattr(self, "_stored_payload", None), dict):
            stored = resolve_exchange_card_credentials(getattr(self, "_stored_payload"))
            if stored is not None:
                api_key = str(stored.get("api_key", "")).strip()
                api_secret = str(stored.get("api_secret", "")).strip()
                passphrase = str(stored.get("api_passphrase", "")).strip()
        if not api_key or not api_secret:
            self._show_input_error(tr("exchange.error.key_secret_required"))
            return
        if not self._is_ascii(api_key) or not self._is_ascii(api_secret):
            self._show_input_error(tr("exchange.error.key_secret_ascii"))
            return
        if requires_passphrase(self.exchange_type):
            if not passphrase:
                self._show_input_error(tr("exchange.error.passphrase_required"))
                return
            if not self._is_ascii(passphrase):
                self._show_input_error(tr("exchange.error.passphrase_ascii"))
                return
        elif passphrase and not self._is_ascii(passphrase):
            self._show_input_error(tr("exchange.error.passphrase_ascii"))
            return
        params = {
            "api_key": api_key,
            "api_secret": api_secret,
            "api_passphrase": passphrase,
            "testnet": False,
        }
        self.connect_clicked.emit(self.exchange_name, params)

    def _on_edit_clicked(self) -> None:
        self.edit_mode = True
        self.edit_clicked.emit(self.exchange_name)
        self._update_ui_state()

    def _on_cancel_clicked(self) -> None:
        if self.is_new:
            self.cancel_clicked.emit()
            return
        self.edit_mode = False
        self._update_ui_state()

    def set_edit_mode(self, edit_mode: bool) -> None:
        self.edit_mode = edit_mode
        self._update_ui_state()

    def set_busy(self, busy: bool) -> None:
        self._busy = bool(busy)
        self._busy_mode = "connect" if self._busy else None
        self._update_ui_state()

    def set_busy_mode(self, mode: str | None) -> None:
        self._busy = mode is not None
        self._busy_mode = mode if self._busy else None
        self._update_ui_state()

    def apply_account_snapshot(self, snapshot: dict) -> None:
        self._snapshot = dict(snapshot or {})
        self._last_error_message = None
        self.balance_label.setText(str(self._snapshot.get("balance_text", tr("exchange.balance"))))
        self.positions_label.setText(str(self._snapshot.get("positions_text", tr("exchange.positions"))))
        self.pnl_label.setText(str(self._snapshot.get("pnl_text", tr("exchange.pnl"))))
        self._update_ui_state()

    def show_connection_error(self, message: str) -> None:
        self._busy = False
        self._busy_mode = None
        self.is_connected = False
        self._snapshot = None
        self._last_error_message = str(message or "")
        self._update_ui_state()

    def show_operation_error(self, message: str) -> None:
        self._busy = False
        self._busy_mode = None
        self._last_error_message = str(message or "")
        self._update_ui_state()

    def mark_connected(self, connected: bool = True, demo: bool = False) -> None:
        del demo
        self._busy = False
        self._busy_mode = None
        self.is_connected = connected
        self.testnet = False
        if not connected:
            self._snapshot = None
        self._last_error_message = None
        self._update_ui_state()

    def load_saved_data(self, params: dict) -> None:
        self._credentials_stored = bool(params.get("credentials_stored"))
        self._stored_api_key_masked = str(params.get("api_key_masked", "")).strip()
        self.api_key_input.clear()
        self.api_secret_input.clear()
        self.passphrase_input.clear()
        if self._credentials_stored:
            self.api_key_input.setPlaceholderText(self._stored_api_key_masked or tr("exchange.api_key"))
            self.api_secret_input.setPlaceholderText(tr("exchange.secret_stored"))
            self.passphrase_input.setPlaceholderText(tr("exchange.passphrase_stored") if requires_passphrase(self.exchange_type) else tr("exchange.passphrase_optional"))
        else:
            self.api_key_input.setText(str(params.get("api_key", "")))
            self.api_secret_input.setText(str(params.get("api_secret", "")))
            self.passphrase_input.setText(str(params.get("api_passphrase", "")))
        self.testnet = False
