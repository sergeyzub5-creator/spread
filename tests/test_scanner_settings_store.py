from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import patch

from app.ui.scanner.settings_store import load_scanner_settings, save_scanner_settings


class ScannerSettingsStoreTests(unittest.TestCase):
    def test_save_and_load_cached_rows(self) -> None:
        state_path = Path("tests") / "_tmp" / "scanner_settings_test.json"
        state_path.parent.mkdir(parents=True, exist_ok=True)
        if state_path.exists():
            state_path.unlink()
        try:
            with patch("app.ui.scanner.settings_store._STATE_PATH", state_path):
                save_scanner_settings(
                    daily_volume_text="1M",
                    daily_volume_threshold=1_000_000,
                    exchanges_visible=["binance", "bybit"],
                    bookmarked_pairs=["BTCUSDT", "ETHUSDT"],
                    cached_rows=[
                        {
                            "canonical": "BTCUSDT",
                            "spread_pct": 1.23,
                            "price_by_exchange": {"binance": "100"},
                            "funding_interval_hours_by_exchange": {"binance": 8},
                        }
                    ],
                )
                data = load_scanner_settings()
        finally:
            if state_path.exists():
                state_path.unlink()

        self.assertEqual(data["daily_volume_text"], "1M")
        self.assertEqual(data["daily_volume_threshold"], 1_000_000)
        self.assertEqual(data["exchanges_visible"], ["binance", "bybit"])
        self.assertEqual(data["bookmarked_pairs"], ["BTCUSDT", "ETHUSDT"])
        self.assertEqual(data["cached_rows"][0]["canonical"], "BTCUSDT")
        self.assertEqual(data["cached_rows"][0]["funding_interval_hours_by_exchange"]["binance"], 8)


if __name__ == "__main__":
    unittest.main()
