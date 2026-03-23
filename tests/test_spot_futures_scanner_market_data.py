from __future__ import annotations

import unittest
from decimal import Decimal
from unittest.mock import patch

from app.scanners.spot_futures_scanner.market_data import (
    compute_binance_spot_futures_rows,
    compute_mexc_spot_futures_rows,
    compute_spot_futures_rows,
)


class SpotFuturesScannerMarketDataTests(unittest.TestCase):
    def test_compute_binance_rows_intersects_spot_and_futures(self) -> None:
        with (
            patch(
                "app.scanners.spot_futures_scanner.market_data._fetch_binance_spot_book",
                return_value={
                    "BTCUSDT": (Decimal("100"), Decimal("102")),
                    "ETHUSDT": (Decimal("50"), Decimal("51")),
                },
            ),
            patch(
                "app.scanners.spot_futures_scanner.market_data._fetch_binance_spot_quote_volume",
                return_value={"BTCUSDT": 1_500_000},
            ),
            patch(
                "app.scanners.spot_futures_scanner.market_data._fetch_binance_futures_book",
                return_value={
                    "BTCUSDT": (Decimal("103"), Decimal("105")),
                    "XRPUSDT": (Decimal("1"), Decimal("1.1")),
                },
            ),
            patch(
                "app.scanners.spot_futures_scanner.market_data._fetch_binance_futures_quote_volume",
                return_value={"BTCUSDT": 2_000_000},
            ),
            patch(
                "app.scanners.spot_futures_scanner.market_data._fetch_binance_funding",
                return_value={"BTCUSDT": "0.0001"},
            ),
        ):
            rows = compute_binance_spot_futures_rows()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].symbol, "BTCUSDT")
        self.assertEqual(rows[0].exchange, "Binance")
        self.assertEqual(rows[0].funding_display, "+0.0100%")
        self.assertEqual(rows[0].spread_pct, Decimal("0.9803921568627450980392156863"))

    def test_compute_mexc_rows_intersects_spot_and_futures(self) -> None:
        with (
            patch(
                "app.scanners.spot_futures_scanner.market_data._fetch_mexc_spot_book",
                return_value={
                    "BTCUSDT": (Decimal("100"), Decimal("101")),
                    "ETHUSDT": (Decimal("50"), Decimal("51")),
                },
            ),
            patch(
                "app.scanners.spot_futures_scanner.market_data._fetch_mexc_spot_quote_volume",
                return_value={"BTCUSDT": 800_000},
            ),
            patch(
                "app.scanners.spot_futures_scanner.market_data._fetch_mexc_futures_book_and_funding",
                return_value=(
                    {
                        "BTCUSDT": (Decimal("104"), Decimal("105")),
                        "XRPUSDT": (Decimal("1"), Decimal("1.1")),
                    },
                    {
                        "BTCUSDT": 900_000,
                    },
                    {
                        "BTCUSDT": ("0.0002", 1_700_000_000_000, 8),
                    },
                ),
            ),
        ):
            rows = compute_mexc_spot_futures_rows()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].symbol, "BTCUSDT")
        self.assertEqual(rows[0].exchange, "MEXC")
        self.assertEqual(rows[0].funding_display, "+0.0200%")
        self.assertEqual(rows[0].spread_pct, Decimal("2.970297029702970297029702970"))

    def test_compute_rows_dispatches_by_exchange(self) -> None:
        with patch(
            "app.scanners.spot_futures_scanner.market_data.compute_mexc_spot_futures_rows",
            return_value=[],
        ) as mexc_compute:
            compute_spot_futures_rows("mexc", symbols={"BTCUSDT"}, refresh_funding=False, priority_symbols=["BTCUSDT"])

        mexc_compute.assert_called_once_with(
            symbols={"BTCUSDT"},
            refresh_funding=False,
            priority_symbols=["BTCUSDT"],
            volume_threshold=None,
        )

    def test_rows_with_spread_over_100_percent_are_filtered(self) -> None:
        with (
            patch(
                "app.scanners.spot_futures_scanner.market_data._fetch_binance_spot_book",
                return_value={"BTCUSDT": (Decimal("99"), Decimal("100"))},
            ),
            patch(
                "app.scanners.spot_futures_scanner.market_data._fetch_binance_spot_quote_volume",
                return_value={"BTCUSDT": 1_000_000},
            ),
            patch(
                "app.scanners.spot_futures_scanner.market_data._fetch_binance_futures_book",
                return_value={"BTCUSDT": (Decimal("250"), Decimal("251"))},
            ),
            patch(
                "app.scanners.spot_futures_scanner.market_data._fetch_binance_futures_quote_volume",
                return_value={"BTCUSDT": 1_200_000},
            ),
            patch(
                "app.scanners.spot_futures_scanner.market_data._fetch_binance_funding",
                return_value={},
            ),
        ):
            rows = compute_binance_spot_futures_rows()

        self.assertEqual(rows, [])

    def test_compute_mexc_rows_merges_funding_detail_timer(self) -> None:
        with (
            patch(
                "app.scanners.spot_futures_scanner.market_data._fetch_mexc_spot_book",
                return_value={"BTCUSDT": (Decimal("100"), Decimal("101"))},
            ),
            patch(
                "app.scanners.spot_futures_scanner.market_data._fetch_mexc_spot_quote_volume",
                return_value={"BTCUSDT": 800_000},
            ),
            patch(
                "app.scanners.spot_futures_scanner.market_data._fetch_mexc_futures_book_and_funding",
                return_value=(
                    {"BTCUSDT": (Decimal("104"), Decimal("105"))},
                    {"BTCUSDT": 900_000},
                    {"BTCUSDT": ("0.0002", None, None)},
                ),
            ),
            patch(
                "app.scanners.spot_futures_scanner.market_data._get_mexc_funding_details",
                return_value={"BTCUSDT": ("0.0003", 1_700_000_000_000, 8)},
            ),
        ):
            rows = compute_mexc_spot_futures_rows()

        self.assertEqual(rows[0].funding_rate, "0.0003")
        self.assertEqual(rows[0].next_funding_ms, 1_700_000_000_000)
        self.assertEqual(rows[0].funding_interval_hours, 8)

    def test_volume_filter_keeps_row_if_one_side_meets_threshold(self) -> None:
        with (
            patch(
                "app.scanners.spot_futures_scanner.market_data._fetch_binance_spot_book",
                return_value={"BTCUSDT": (Decimal("100"), Decimal("102"))},
            ),
            patch(
                "app.scanners.spot_futures_scanner.market_data._fetch_binance_spot_quote_volume",
                return_value={"BTCUSDT": 100_000},
            ),
            patch(
                "app.scanners.spot_futures_scanner.market_data._fetch_binance_futures_book",
                return_value={"BTCUSDT": (Decimal("103"), Decimal("105"))},
            ),
            patch(
                "app.scanners.spot_futures_scanner.market_data._fetch_binance_futures_quote_volume",
                return_value={"BTCUSDT": 2_500_000},
            ),
            patch(
                "app.scanners.spot_futures_scanner.market_data._fetch_binance_funding",
                return_value={},
            ),
        ):
            rows = compute_binance_spot_futures_rows(volume_threshold=1_000_000)

        self.assertEqual(len(rows), 1)


if __name__ == "__main__":
    unittest.main()
