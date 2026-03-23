from __future__ import annotations

from dataclasses import dataclass

from app.charts.market_types import ChartInstrumentType
from app.charts.exchanges.catalog import normalize_chart_exchange_code
from app.charts.exchanges.symbols import (
    binance_from_native,
    binance_to_native,
    bitget_from_native,
    bitget_to_native,
    bybit_from_native,
    bybit_to_native,
    mexc_canonical_symbol,
    okx_from_native,
    okx_to_native,
)


@dataclass(frozen=True)
class ChartExchangeIdentifier:
    exchange: str
    ui_market_types: tuple[str, ...]
    actual_market_types: dict[str, str]
    spot_symbol_style: str
    perpetual_symbol_style: str
    futures_symbol_style: str
    notes: str


CHART_EXCHANGE_IDENTIFIERS: dict[str, ChartExchangeIdentifier] = {
    "binance": ChartExchangeIdentifier(
        exchange="binance",
        ui_market_types=(
            ChartInstrumentType.SPOT.value,
            ChartInstrumentType.PERPETUAL.value,
            ChartInstrumentType.FUTURES.value,
        ),
        actual_market_types={
            ChartInstrumentType.SPOT.value: "spot",
            ChartInstrumentType.PERPETUAL.value: "linear_perp",
            ChartInstrumentType.FUTURES.value: "linear_delivery",
        },
        spot_symbol_style="BTCUSDT",
        perpetual_symbol_style="BTCUSDT",
        futures_symbol_style="BTCUSDT_YYMMDD",
        notes="Binance uses spot, USD-M perpetual, and USD-M delivery identifiers.",
    ),
    "bybit": ChartExchangeIdentifier(
        exchange="bybit",
        ui_market_types=(
            ChartInstrumentType.SPOT.value,
            ChartInstrumentType.PERPETUAL.value,
            ChartInstrumentType.FUTURES.value,
        ),
        actual_market_types={
            ChartInstrumentType.SPOT.value: "spot",
            ChartInstrumentType.PERPETUAL.value: "linear_perp",
            ChartInstrumentType.FUTURES.value: "linear_delivery",
        },
        spot_symbol_style="BTCUSDT",
        perpetual_symbol_style="BTCUSDT",
        futures_symbol_style="BTCUSDT delivery",
        notes="Bybit spot and linear contracts share symbol form; delivery stays in linear_delivery.",
    ),
    "bitget": ChartExchangeIdentifier(
        exchange="bitget",
        ui_market_types=(
            ChartInstrumentType.SPOT.value,
            ChartInstrumentType.PERPETUAL.value,
            ChartInstrumentType.FUTURES.value,
        ),
        actual_market_types={
            ChartInstrumentType.SPOT.value: "spot",
            ChartInstrumentType.PERPETUAL.value: "linear_perp",
            ChartInstrumentType.FUTURES.value: "bitget_coin_delivery",
        },
        spot_symbol_style="BTCUSDT",
        perpetual_symbol_style="BTCUSDT",
        futures_symbol_style="BTCUSD delivery",
        notes="Bitget delivery in our app is represented by bitget_coin_delivery.",
    ),
    "okx": ChartExchangeIdentifier(
        exchange="okx",
        ui_market_types=(
            ChartInstrumentType.SPOT.value,
            ChartInstrumentType.PERPETUAL.value,
        ),
        actual_market_types={
            ChartInstrumentType.SPOT.value: "spot",
            ChartInstrumentType.PERPETUAL.value: "linear_perp",
        },
        spot_symbol_style="BTC-USDT",
        perpetual_symbol_style="BTC-USDT-SWAP",
        futures_symbol_style="",
        notes="OKX perpetuals map to SWAP instId format.",
    ),
    "mexc": ChartExchangeIdentifier(
        exchange="mexc",
        ui_market_types=(
            ChartInstrumentType.SPOT.value,
            ChartInstrumentType.PERPETUAL.value,
        ),
        actual_market_types={
            ChartInstrumentType.SPOT.value: "spot",
            ChartInstrumentType.PERPETUAL.value: "linear_perp",
        },
        spot_symbol_style="BTCUSDT",
        perpetual_symbol_style="BTC_USDT",
        futures_symbol_style="",
        notes="MEXC perpetual symbols often arrive as BTC_USDT and are normalized by removing separators.",
    ),
    "kucoin": ChartExchangeIdentifier(
        exchange="kucoin",
        ui_market_types=(ChartInstrumentType.PERPETUAL.value,),
        actual_market_types={ChartInstrumentType.PERPETUAL.value: "linear_perp"},
        spot_symbol_style="",
        perpetual_symbol_style="XBTUSDTM",
        futures_symbol_style="",
        notes="Prepared as local identifier metadata only.",
    ),
    "gate": ChartExchangeIdentifier(
        exchange="gate",
        ui_market_types=(ChartInstrumentType.PERPETUAL.value,),
        actual_market_types={ChartInstrumentType.PERPETUAL.value: "linear_perp"},
        spot_symbol_style="",
        perpetual_symbol_style="BTC_USDT",
        futures_symbol_style="",
        notes="Prepared as local identifier metadata only.",
    ),
    "bingx": ChartExchangeIdentifier(
        exchange="bingx",
        ui_market_types=(ChartInstrumentType.PERPETUAL.value,),
        actual_market_types={ChartInstrumentType.PERPETUAL.value: "linear_perp"},
        spot_symbol_style="",
        perpetual_symbol_style="BTC-USDT",
        futures_symbol_style="",
        notes="Prepared as local identifier metadata only.",
    ),
}


def available_chart_market_types(exchange_code: str | None) -> list[str]:
    normalized = normalize_chart_exchange_code(exchange_code)
    identifier = CHART_EXCHANGE_IDENTIFIERS.get(normalized)
    return list(identifier.ui_market_types) if identifier else []


def chart_exchange_supports_market_type(exchange_code: str | None, ui_market_type: str | None) -> bool:
    normalized = normalize_chart_exchange_code(exchange_code)
    identifier = CHART_EXCHANGE_IDENTIFIERS.get(normalized)
    if not identifier:
        return False
    return str(ui_market_type or "").strip().lower() in identifier.ui_market_types


def to_chart_actual_market_type(exchange_code: str | None, ui_market_type: str | None) -> str | None:
    normalized = normalize_chart_exchange_code(exchange_code)
    identifier = CHART_EXCHANGE_IDENTIFIERS.get(normalized)
    if not identifier:
        return None
    return identifier.actual_market_types.get(str(ui_market_type or "").strip().lower())


def normalize_chart_symbol(exchange_code: str | None, ui_market_type: str | None, raw_symbol: str | object) -> str | None:
    exchange = normalize_chart_exchange_code(exchange_code)
    market_type = str(ui_market_type or "").strip().lower()
    raw = str(raw_symbol or "").strip()
    if not raw:
        return None

    if exchange == "binance":
        if market_type == ChartInstrumentType.SPOT.value:
            return binance_to_native(raw)
        if market_type == ChartInstrumentType.PERPETUAL.value:
            return binance_from_native(raw) or binance_to_native(raw)
        if market_type == ChartInstrumentType.FUTURES.value:
            return raw.strip().upper()

    if exchange == "bybit":
        if market_type == ChartInstrumentType.SPOT.value:
            return bybit_to_native(raw)
        if market_type in (ChartInstrumentType.PERPETUAL.value, ChartInstrumentType.FUTURES.value):
            return bybit_from_native(raw, quote_suffix="USDT") or bybit_to_native(raw)

    if exchange == "bitget":
        if market_type == ChartInstrumentType.SPOT.value:
            return bitget_to_native(raw)
        if market_type in (ChartInstrumentType.PERPETUAL.value, ChartInstrumentType.FUTURES.value):
            return bitget_from_native(raw) or bitget_to_native(raw)

    if exchange == "okx":
        if market_type == ChartInstrumentType.SPOT.value:
            return raw.strip().upper()
        if market_type == ChartInstrumentType.PERPETUAL.value:
            return okx_from_native(raw) or okx_to_native(raw)

    if exchange == "mexc":
        return mexc_canonical_symbol(raw)

    return raw.strip().upper()
