from __future__ import annotations

from enum import StrEnum


class ChartInstrumentType(StrEnum):
    SPOT = "spot"
    PERPETUAL = "perpetual"
    FUTURES = "futures"


CHART_INSTRUMENT_TYPE_LABELS = {
    ChartInstrumentType.SPOT: "Спот",
    ChartInstrumentType.PERPETUAL: "Фьючерз бесср.",
    ChartInstrumentType.FUTURES: "Фьючерз срочн.",
}


ACTUAL_TO_CHART_MARKET_TYPE = {
    "spot": ChartInstrumentType.SPOT,
    "linear_perp": ChartInstrumentType.PERPETUAL,
    "inverse_perp": ChartInstrumentType.PERPETUAL,
    "linear_delivery": ChartInstrumentType.FUTURES,
    "bitget_coin_delivery": ChartInstrumentType.FUTURES,
}


def to_chart_instrument_type(actual_market_type: str) -> ChartInstrumentType | None:
    return ACTUAL_TO_CHART_MARKET_TYPE.get(str(actual_market_type or "").strip().lower())


def chart_market_type_menu_items() -> list[tuple[str, str]]:
    return [(item.value, CHART_INSTRUMENT_TYPE_LABELS[item]) for item in ChartInstrumentType]
