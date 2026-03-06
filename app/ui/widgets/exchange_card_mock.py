from __future__ import annotations

from app.ui.widgets.exchange_panel import ExchangePanel


class ExchangeCardMock(ExchangePanel):
    def __init__(self, exchange_name: str, exchange_code: str, parent=None) -> None:
        super().__init__(exchange_name, exchange_code, is_new=False, parent=parent)

