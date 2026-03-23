from __future__ import annotations

from enum import StrEnum


class UiInstrumentType(StrEnum):
    SPOT = "spot"
    PERPETUAL = "perpetual"
    # Срочные (доставочные) линейные фьючерсы USD-M и аналоги на других биржах
    FUTURES = "futures"


UI_INSTRUMENT_TYPE_LABELS = {
    UiInstrumentType.SPOT: "Спот",
    UiInstrumentType.PERPETUAL: "Фьючерз бесср.",
    UiInstrumentType.FUTURES: "Фьючерз срочн.",
}


ACTUAL_TO_UI_MARKET_TYPE = {
    "spot": UiInstrumentType.SPOT,
    "linear_perp": UiInstrumentType.PERPETUAL,
    "inverse_perp": UiInstrumentType.PERPETUAL,
    "linear_delivery": UiInstrumentType.FUTURES,
    # Bitget COIN-M delivery — в UI тот же тип «Фьючерз срочн.»
    "bitget_coin_delivery": UiInstrumentType.FUTURES,
}


def to_ui_instrument_type(actual_market_type: str) -> UiInstrumentType | None:
    return ACTUAL_TO_UI_MARKET_TYPE.get(str(actual_market_type or "").strip().lower())
