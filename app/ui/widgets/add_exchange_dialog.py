from __future__ import annotations

from uuid import uuid4

from PySide6.QtWidgets import QDialog, QVBoxLayout

from app.ui.exchange_catalog import get_exchange_meta
from app.ui.i18n import tr
from app.ui.theme import theme_color
from app.ui.widgets.exchange_panel import ExchangePanel


class AddExchangeDialog(QDialog):
    def __init__(self, exchange_code: str, coordinator=None, parent=None) -> None:
        super().__init__(parent)
        meta = get_exchange_meta(exchange_code)
        self.coordinator = coordinator
        self.payload: dict | None = None
        self._request_id: str | None = None
        self.setWindowTitle(tr("exchange_dialog.title", exchange=meta["title"]))
        self.setMinimumSize(520, 190)
        self.resize(560, 222)
        self.setStyleSheet(
            f"""
            QDialog {{
                background-color: {theme_color('surface')};
                color: {theme_color('text_primary')};
            }}
            """
        )

        layout = QVBoxLayout(self)
        layout.setContentsMargins(6, 6, 6, 6)
        layout.setSpacing(4)

        self.panel = ExchangePanel(meta["base_name"], exchange_code, is_new=True)
        self.panel.connect_clicked.connect(self._start_connect)
        self.panel.cancel_clicked.connect(self.reject)
        layout.addWidget(self.panel)

        if self.coordinator is not None:
            self.coordinator.exchange_connect_succeeded.connect(self._on_connect_succeeded)
            self.coordinator.exchange_connect_failed.connect(self._on_connect_failed)

    def _start_connect(self, exchange_name: str, params: dict) -> None:
        del exchange_name
        if self.coordinator is None:
            self._accept_payload(params, snapshot=None)
            return
        self._request_id = uuid4().hex
        self.panel.set_busy(True)
        self.coordinator.connect_exchange_async(self._request_id, self.panel.exchange_type, params)

    def _accept_payload(self, params: dict, snapshot: dict | None) -> None:
        self.payload = {
            "exchange_code": self.panel.exchange_type,
            "exchange_title": self.panel.exchange_meta["title"],
            "exchange_name": self.panel.exchange_meta["base_name"],
            **params,
        }
        if isinstance(snapshot, dict):
            self.payload["account_snapshot"] = snapshot
        self.accept()

    def _on_connect_succeeded(self, request_id: str, exchange_code: str, snapshot: object) -> None:
        if request_id != self._request_id or exchange_code != self.panel.exchange_type or not isinstance(snapshot, dict):
            return
        params = {
            "api_key": self.panel.api_key_input.text().strip(),
            "api_secret": self.panel.api_secret_input.text().strip(),
            "api_passphrase": self.panel.passphrase_input.text().strip(),
            "testnet": False,
        }
        self.panel.set_busy(False)
        self._accept_payload(params, snapshot=snapshot)

    def _on_connect_failed(self, request_id: str, exchange_code: str, message: str) -> None:
        if request_id != self._request_id or exchange_code != self.panel.exchange_type:
            return
        self.panel.show_connection_error(message)

