from __future__ import annotations

from PySide6.QtWidgets import QDialog, QVBoxLayout

from ui.exchange_catalog import get_exchange_meta
from ui.i18n import tr
from ui.theme import theme_color
from ui.widgets.exchange_panel import ExchangePanel


class AddExchangeDialog(QDialog):
    def __init__(self, exchange_code: str, parent=None) -> None:
        super().__init__(parent)
        meta = get_exchange_meta(exchange_code)
        self.payload: dict | None = None
        self.setWindowTitle(tr("exchange_dialog.title", exchange=meta["title"]))
        self.setMinimumSize(900, 320)
        self.resize(980, 360)
        self.setStyleSheet(
            f"""
            QDialog {{
                background-color: {theme_color('surface')};
                color: {theme_color('text_primary')};
            }}
            """
        )

        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(6)

        self.panel = ExchangePanel(tr("exchange.new_connection"), exchange_code, is_new=True)
        self.panel.connect_clicked.connect(self._accept_payload)
        self.panel.cancel_clicked.connect(self.reject)
        layout.addWidget(self.panel)

    def _accept_payload(self, exchange_name: str, params: dict) -> None:
        del exchange_name
        self.payload = {
            "exchange_code": self.panel.exchange_type,
            "exchange_title": self.panel.exchange_meta["title"],
            "exchange_name": self.panel.exchange_meta["base_name"],
            **params,
        }
        self.accept()
