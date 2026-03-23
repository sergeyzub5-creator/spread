from __future__ import annotations

import unittest
from decimal import Decimal
from unittest.mock import patch

from app.core.market_data.scanner_pair_list import (
    ScannerPairRow,
    _bitget_funding_by_symbol,
    _bybit_next_funding_ms,
    _format_timer_with_interval,
    compute_pairs_sorted_by_spread,
    fetch_exchange_vol_price_maps,
    ExchangeCell,
    refresh_cached_pair_rows,
)
from app.core.market_data.scanner_tradable_symbols import InstrumentIdentity


def _make_row() -> ScannerPairRow:
    return ScannerPairRow(
        canonical="BTCUSDT",
        spread_pct=10.0,
        min_price=Decimal("100"),
        max_price=Decimal("110"),
        price_by_exchange={"binance": Decimal("100"), "bybit": Decimal("110")},
        volume_by_exchange={"binance": 1000, "bybit": 1200},
        funding_rate_by_exchange={"binance": "0.001", "bybit": "0.002"},
        next_funding_ms_by_exchange={"binance": 1_000, "bybit": 2_000},
        funding_interval_hours_by_exchange={"binance": 8, "bybit": 4},
        funding_display_by_exchange={"binance": "+0.1000%", "bybit": "+0.2000%"},
        timer_display_by_exchange={"binance": "00:00:01 (8\u0447)", "bybit": "00:00:02 (4\u0447)"},
    )


class ScannerPairListRefreshTests(unittest.TestCase):
    def test_compute_pairs_sorted_by_spread_accepts_volume_on_any_exchange(self) -> None:
        maps = {
            "binance": {
                "BTCUSDT": ExchangeCell(volume_usdt=2_000_000, price=Decimal("100")),
            },
            "bybit": {
                "BTCUSDT": ExchangeCell(volume_usdt=50_000, price=Decimal("110")),
            },
            "bitget": {},
            "okx": {},
        }
        with patch(
            "app.core.market_data.scanner_pair_list.fetch_exchange_vol_price_maps",
            return_value=(maps, None),
        ):
            rows, err = compute_pairs_sorted_by_spread(
                volume_threshold_usdt=1_000_000,
                visible_exchange_ids=["binance", "bybit"],
                timeout=10.0,
            )

        self.assertIsNone(err)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].canonical, "BTCUSDT")
        self.assertEqual(rows[0].volume_by_exchange["bybit"], 50_000)

    def test_compute_pairs_sorted_by_spread_still_requires_two_prices(self) -> None:
        maps = {
            "binance": {
                "BTCUSDT": ExchangeCell(volume_usdt=2_000_000, price=Decimal("100")),
            },
            "bybit": {
                "BTCUSDT": ExchangeCell(volume_usdt=50_000, price=None),
            },
            "bitget": {},
            "okx": {},
        }
        with patch(
            "app.core.market_data.scanner_pair_list.fetch_exchange_vol_price_maps",
            return_value=(maps, None),
        ):
            rows, err = compute_pairs_sorted_by_spread(
                volume_threshold_usdt=1_000_000,
                visible_exchange_ids=["binance", "bybit"],
                timeout=10.0,
            )

        self.assertIsNone(err)
        self.assertEqual(rows, [])

    def test_fetch_exchange_vol_price_maps_can_skip_funding_requests(self) -> None:
        bybit_payload = {"retCode": 0, "result": {"list": []}}
        bitget_payload = {"code": "00000", "data": []}
        okx_payload = {"code": "0", "data": []}
        with (
            patch(
                "app.core.market_data.scanner_pair_list._http_get_json",
                side_effect=[
                    [],
                    bybit_payload,
                    bitget_payload,
                    okx_payload,
                ],
            ) as http_get,
            patch("app.core.market_data.scanner_pair_list._binance_funding_by_symbol") as binance_funding,
            patch("app.core.market_data.scanner_pair_list._bitget_funding_by_symbol") as bitget_funding,
        ):
            maps, err = fetch_exchange_vol_price_maps(
                timeout=5.0,
                tradable_only=False,
                include_funding=False,
            )

        self.assertEqual(maps, {"binance": {}, "bybit": {}, "bitget": {}, "okx": {}})
        self.assertIsNone(err)
        self.assertEqual(http_get.call_count, 4)
        binance_funding.assert_not_called()
        bitget_funding.assert_not_called()

    def test_compute_pairs_sorted_by_spread_requires_matching_identity(self) -> None:
        maps = {
            "binance": {
                "ABCUSDT": ExchangeCell(volume_usdt=2_000_000, price=Decimal("100")),
            },
            "bybit": {
                "ABCUSDT": ExchangeCell(volume_usdt=3_000_000, price=Decimal("110")),
            },
            "bitget": {},
            "okx": {},
        }
        identity_maps = {
            "binance": {
                "ABCUSDT": InstrumentIdentity(
                    canonical="ABCUSDT",
                    base="ABC",
                    quote="USDT",
                    settle="USDT",
                    underlying="ABCUSDT",
                )
            },
            "bybit": {
                "ABCUSDT": InstrumentIdentity(
                    canonical="ABCUSDT",
                    base="ABC2",
                    quote="USDT",
                    settle="USDT",
                    underlying="ABC2USDT",
                )
            },
            "bitget": {},
            "okx": {},
        }
        with (
            patch(
                "app.core.market_data.scanner_pair_list.fetch_exchange_vol_price_maps",
                return_value=(maps, None),
            ),
            patch(
                "app.core.market_data.scanner_pair_list.fetch_all_tradable_identity_maps",
                return_value=identity_maps,
            ),
        ):
            rows, err = compute_pairs_sorted_by_spread(
                volume_threshold_usdt=1_000_000,
                visible_exchange_ids=["binance", "bybit"],
                timeout=10.0,
            )

        self.assertIsNone(err)
        self.assertEqual(rows, [])

    def test_compute_pairs_sorted_by_spread_uses_matching_identity(self) -> None:
        maps = {
            "binance": {
                "BTCUSDT": ExchangeCell(volume_usdt=2_000_000, price=Decimal("100")),
            },
            "bybit": {
                "BTCUSDT": ExchangeCell(volume_usdt=50_000, price=Decimal("110")),
            },
            "bitget": {},
            "okx": {},
        }
        identity = InstrumentIdentity(
            canonical="BTCUSDT",
            base="BTC",
            quote="USDT",
            settle="USDT",
            underlying="BTCUSDT",
        )
        with (
            patch(
                "app.core.market_data.scanner_pair_list.fetch_exchange_vol_price_maps",
                return_value=(maps, None),
            ),
            patch(
                "app.core.market_data.scanner_pair_list.fetch_all_tradable_identity_maps",
                return_value={
                    "binance": {"BTCUSDT": identity},
                    "bybit": {"BTCUSDT": identity},
                    "bitget": {},
                    "okx": {},
                },
            ),
        ):
            rows, err = compute_pairs_sorted_by_spread(
                volume_threshold_usdt=1_000_000,
                visible_exchange_ids=["binance", "bybit"],
                timeout=10.0,
            )

        self.assertIsNone(err)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].canonical, "BTCUSDT")

    def test_compute_pairs_sorted_by_spread_filters_over_100_percent(self) -> None:
        maps = {
            "binance": {
                "BTCUSDT": ExchangeCell(volume_usdt=2_000_000, price=Decimal("100")),
            },
            "bybit": {
                "BTCUSDT": ExchangeCell(volume_usdt=2_000_000, price=Decimal("250")),
            },
            "bitget": {},
            "okx": {},
        }
        identity = InstrumentIdentity(
            canonical="BTCUSDT",
            base="BTC",
            quote="USDT",
            settle="USDT",
            underlying="BTCUSDT",
        )
        with (
            patch(
                "app.core.market_data.scanner_pair_list.fetch_exchange_vol_price_maps",
                return_value=(maps, None),
            ),
            patch(
                "app.core.market_data.scanner_pair_list.fetch_all_tradable_identity_maps",
                return_value={
                    "binance": {"BTCUSDT": identity},
                    "bybit": {"BTCUSDT": identity},
                    "bitget": {},
                    "okx": {},
                },
            ),
        ):
            rows, err = compute_pairs_sorted_by_spread(
                volume_threshold_usdt=1_000_000,
                visible_exchange_ids=["binance", "bybit"],
                timeout=10.0,
            )

        self.assertIsNone(err)
        self.assertEqual(rows, [])

    def test_bybit_next_funding_uses_direct_timestamp_or_interval(self) -> None:
        self.assertEqual(
            _bybit_next_funding_ms({"nextFundingTime": "1700000000000", "fundingIntervalHour": "8"}),
            1_700_000_000_000,
        )
        self.assertEqual(
            _bybit_next_funding_ms({"fundingIntervalHour": "4"}, now_ms=3_600_000),
            14_400_000,
        )
        self.assertEqual(
            _bybit_next_funding_ms({"fundingIntervalHour": "1"}, now_ms=3_600_000),
            7_200_000,
        )

    def test_bitget_funding_by_symbol_uses_next_update_and_interval(self) -> None:
        payload = {
            "code": "00000",
            "data": [
                {"symbol": "BTCUSDT", "fundingRate": "0.001", "nextUpdate": "1700000000000"},
                {"symbol": "ETHUSDT", "fundingRate": "0.002", "fundingRateInterval": "4"},
            ],
        }
        with (
            patch("app.core.market_data.scanner_pair_list._http_get_json", return_value=payload),
            patch("app.core.market_data.scanner_pair_list.time.time", return_value=3600),
        ):
            result = _bitget_funding_by_symbol(10.0)

        self.assertEqual(result["BTCUSDT"], ("0.001", 1_700_000_000_000, None))
        self.assertEqual(result["ETHUSDT"], ("0.002", 14_400_000, 4))

    def test_format_timer_with_interval_appends_hours(self) -> None:
        with patch("app.core.market_data.scanner_pair_list.ms_until_next_funding", return_value=4_000):
            self.assertEqual(_format_timer_with_interval(9_000, 4), "00:00:04 (4\u0447)")
            self.assertEqual(_format_timer_with_interval(9_000, None), "00:00:04")

    def test_refresh_cached_pair_rows_updates_prices_funding_and_spread(self) -> None:
        row = _make_row()

        with (
            patch(
                "app.core.market_data.scanner_pair_list._fetch_price_maps",
                return_value=(
                    {
                        "binance": {"BTCUSDT": Decimal("101")},
                        "bybit": {"BTCUSDT": Decimal("121")},
                    },
                    [],
                ),
            ),
            patch(
                "app.core.market_data.scanner_pair_list._fetch_funding_maps",
                return_value=(
                    {
                        "binance": {"BTCUSDT": ("0.003", 3_000, 8)},
                        "bybit": {"BTCUSDT": ("0.004", 4_000, 1)},
                    },
                    [],
                ),
            ),
        ):
            refreshed, err = refresh_cached_pair_rows(
                rows=[row],
                visible_exchange_ids=["binance", "bybit"],
                refresh_prices=True,
                refresh_funding=True,
            )

        self.assertIsNone(err)
        self.assertEqual(row.price_by_exchange["binance"], Decimal("100"))
        self.assertEqual(len(refreshed), 1)
        updated = refreshed[0]
        self.assertEqual(updated.price_by_exchange["binance"], Decimal("101"))
        self.assertEqual(updated.price_by_exchange["bybit"], Decimal("121"))
        self.assertEqual(updated.funding_rate_by_exchange["binance"], "0.003")
        self.assertEqual(updated.next_funding_ms_by_exchange["bybit"], 4_000)
        self.assertEqual(updated.funding_interval_hours_by_exchange["binance"], 8)
        self.assertEqual(updated.funding_interval_hours_by_exchange["bybit"], 1)
        self.assertAlmostEqual(updated.spread_pct, 19.8019801980, places=6)
        self.assertEqual(updated.min_price, Decimal("101"))
        self.assertEqual(updated.max_price, Decimal("121"))

    def test_refresh_cached_pair_rows_keeps_previous_values_when_symbol_missing(self) -> None:
        row = _make_row()

        with patch(
            "app.core.market_data.scanner_pair_list._fetch_price_maps",
            return_value=({"binance": {}, "bybit": {"BTCUSDT": Decimal("115")}}, []),
        ):
            refreshed, err = refresh_cached_pair_rows(
                rows=[row],
                visible_exchange_ids=["binance", "bybit"],
                refresh_prices=True,
                refresh_funding=False,
            )

        self.assertIsNone(err)
        updated = refreshed[0]
        self.assertEqual(updated.price_by_exchange["binance"], Decimal("100"))
        self.assertEqual(updated.price_by_exchange["bybit"], Decimal("115"))

    def test_refresh_cached_pair_rows_filters_over_100_percent(self) -> None:
        row = _make_row()

        with patch(
            "app.core.market_data.scanner_pair_list._fetch_price_maps",
            return_value=(
                {
                    "binance": {"BTCUSDT": Decimal("100")},
                    "bybit": {"BTCUSDT": Decimal("250")},
                },
                [],
            ),
        ):
            refreshed, err = refresh_cached_pair_rows(
                rows=[row],
                visible_exchange_ids=["binance", "bybit"],
                refresh_prices=True,
                refresh_funding=False,
            )

        self.assertIsNone(err)
        self.assertEqual(refreshed, [])


if __name__ == "__main__":
    unittest.main()
