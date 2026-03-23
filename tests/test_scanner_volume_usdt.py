"""Нормализаторы объёма в USDT для сканера."""
from __future__ import annotations

from app.ui.scanner.exchanges.binance.volume_usdt import volume_usdt_from_24h_item
from app.ui.scanner.exchanges.bitget.volume_usdt import volume_usdt_from_ticker_item as bitget_vol
from app.ui.scanner.exchanges.bybit.volume_usdt import volume_usdt_from_ticker_item as bybit_vol
from app.ui.scanner.exchanges.okx.volume_usdt import volume_usdt_from_ticker_item as okx_vol
from app.ui.scanner.exchanges.volume_canonical import volume_usdt_for_exchange


def test_binance_quote_volume() -> None:
    assert volume_usdt_from_24h_item({"quoteVolume": "13807455635.52"}) == 13807455635
    assert volume_usdt_from_24h_item({}) is None


def test_bybit_turnover() -> None:
    assert bybit_vol({"turnover24h": "6579197131.0278"}) == 6579197131


def test_bitget_quote_volume() -> None:
    assert bitget_vol({"quoteVolume": "1000.9"}) == 1000
    assert bitget_vol({"usdtVolume": "2000"}) == 2000


def test_okx_vol_ccy_times_last() -> None:
    # volCcy24h base * last ≈ USDT notional
    item = {"instId": "BTC-USDT-SWAP", "volCcy24h": "112935.812", "last": "70444.3"}
    v = okx_vol(item)
    assert v is not None
    assert v > 7_000_000_000  # same order as Bybit/Binance


def test_volume_usdt_for_exchange_dispatch() -> None:
    assert volume_usdt_for_exchange("binance", {"quoteVolume": "100"}) == 100
    assert volume_usdt_for_exchange("bybit", {"turnover24h": "200"}) == 200
    assert volume_usdt_for_exchange("unknown", {}) is None
