from __future__ import annotations

from enum import StrEnum


class UiInstrumentType(StrEnum):
    SPOT = "spot"
    PERPETUAL = "perpetual"


UI_INSTRUMENT_TYPE_LABELS = {
    UiInstrumentType.SPOT: "Спот",
    UiInstrumentType.PERPETUAL: "Фьючерз бесср.",
}


ACTUAL_TO_UI_MARKET_TYPE = {
    "spot": UiInstrumentType.SPOT,
    "linear_perp": UiInstrumentType.PERPETUAL,
    "inverse_perp": UiInstrumentType.PERPETUAL,
}


def to_ui_instrument_type(actual_market_type: str) -> UiInstrumentType | None:
    return ACTUAL_TO_UI_MARKET_TYPE.get(str(actual_market_type or "").strip().lower())
