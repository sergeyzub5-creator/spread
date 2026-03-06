from __future__ import annotations

from collections import defaultdict

from app.core.models.instrument import InstrumentId
from app.core.models.instrument_types import UiInstrumentType, to_ui_instrument_type


class InstrumentRegistry:
    """Canonical storage and lookup for exchange instruments."""

    def __init__(self) -> None:
        self._instruments_by_exchange: dict[str, list[InstrumentId]] = defaultdict(list)

    def replace_exchange_instruments(self, exchange: str, instruments: list[InstrumentId]) -> None:
        self._instruments_by_exchange[exchange] = list(instruments)

    def list_by_exchange(self, exchange: str) -> list[InstrumentId]:
        return list(self._instruments_by_exchange.get(exchange, []))

    def list_ui_market_types(self, exchange: str) -> list[UiInstrumentType]:
        seen: list[UiInstrumentType] = []
        for instrument in self._instruments_by_exchange.get(exchange, []):
            ui_type = to_ui_instrument_type(instrument.market_type)
            if ui_type is None or ui_type in seen:
                continue
            seen.append(ui_type)
        return seen

    def list_by_ui_market_type(self, exchange: str, ui_market_type: str) -> list[InstrumentId]:
        normalized = str(ui_market_type or "").strip().lower()
        return [
            instrument
            for instrument in self._instruments_by_exchange.get(exchange, [])
            if ((to_ui_instrument_type(instrument.market_type).value) if to_ui_instrument_type(instrument.market_type) else "") == normalized
        ]

    def find(self, exchange: str, ws_symbol: str, market_type: str) -> InstrumentId | None:
        for instrument in self._instruments_by_exchange.get(exchange, []):
            if instrument.routing.ws_symbol == ws_symbol and instrument.market_type == market_type:
                return instrument
        return None

    def find_by_symbol(self, exchange: str, symbol: str, market_type: str) -> InstrumentId | None:
        normalized_symbol = str(symbol or "").strip().upper()
        for instrument in self._instruments_by_exchange.get(exchange, []):
            if instrument.symbol == normalized_symbol and instrument.market_type == market_type:
                return instrument
        return None

    def find_by_ui_symbol(self, exchange: str, symbol: str, ui_market_type: str) -> InstrumentId | None:
        normalized_symbol = str(symbol or "").strip().upper()
        normalized_ui_type = str(ui_market_type or "").strip().lower()
        for instrument in self._instruments_by_exchange.get(exchange, []):
            if instrument.symbol != normalized_symbol:
                continue
            ui_type = to_ui_instrument_type(instrument.market_type)
            if (ui_type.value if ui_type else "") != normalized_ui_type:
                continue
            return instrument
        return None
