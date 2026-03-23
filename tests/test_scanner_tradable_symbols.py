from __future__ import annotations

import unittest
from unittest.mock import patch

from app.core.market_data import scanner_tradable_symbols as sts


class ScannerTradableSymbolsTests(unittest.TestCase):
    def setUp(self) -> None:
        sts._TRADABLE_CACHE.clear()

    def test_fetch_all_tradable_canonical_sets_uses_cache(self) -> None:
        with (
            patch.object(sts, "fetch_binance_tradable_usdt_perpetual_canonical", return_value={"BTCUSDT"}) as binance,
            patch.object(sts, "fetch_bybit_tradable_linear_perpetual_canonical", return_value={"BTCUSDT"}) as bybit,
            patch.object(sts, "fetch_bitget_tradable_usdt_futures_canonical", return_value={"BTCUSDT"}) as bitget,
            patch.object(sts, "fetch_okx_tradable_usdt_swap_canonical", return_value={"BTCUSDT"}) as okx,
        ):
            first = sts.fetch_all_tradable_canonical_sets(timeout=5.0)
            second = sts.fetch_all_tradable_canonical_sets(timeout=5.0)

        self.assertEqual(first["binance"], {"BTCUSDT"})
        self.assertEqual(second["okx"], {"BTCUSDT"})
        self.assertEqual(binance.call_count, 1)
        self.assertEqual(bybit.call_count, 1)
        self.assertEqual(bitget.call_count, 1)
        self.assertEqual(okx.call_count, 1)

    def test_fetch_all_tradable_canonical_sets_falls_back_to_stale_cache(self) -> None:
        sts._TRADABLE_CACHE["binance"] = (999999.0, {"BTCUSDT"})
        with patch("app.core.market_data.scanner_tradable_symbols.time.monotonic", return_value=1_000_000.0):
            with patch.object(
                sts,
                "fetch_binance_tradable_usdt_perpetual_canonical",
                side_effect=RuntimeError("boom"),
            ):
                result = sts.fetch_all_tradable_canonical_sets(timeout=5.0)

        self.assertEqual(result["binance"], {"BTCUSDT"})


if __name__ == "__main__":
    unittest.main()
