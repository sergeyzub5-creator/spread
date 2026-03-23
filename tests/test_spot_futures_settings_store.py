from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.scanners.spot_futures_scanner.settings_store import load_spot_futures_settings, save_spot_futures_settings


class SpotFuturesSettingsStoreTests(unittest.TestCase):
    def test_save_and_load_cached_rows(self) -> None:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as tmp_file:
            state_path = Path(tmp_file.name)
        try:
            with patch("app.scanners.spot_futures_scanner.settings_store._STATE_PATH", state_path):
                save_spot_futures_settings(
                    cached_rows=[{"symbol": "BTCUSDT", "spread_pct": "1.23"}],
                    cached_rows_by_exchange={
                        "binance": [{"symbol": "BTCUSDT", "spread_pct": "1.23"}],
                        "mexc": [{"symbol": "ETHUSDT", "spread_pct": "0.45"}],
                    },
                    selected_exchange="mexc",
                    daily_volume_text="10M",
                    daily_volume_threshold=10_000_000,
                )
                payload = load_spot_futures_settings()
        finally:
            try:
                state_path.unlink(missing_ok=True)
            except Exception:
                pass

        self.assertEqual(payload.get("cached_rows"), [{"symbol": "BTCUSDT", "spread_pct": "1.23"}])
        self.assertEqual(
            payload.get("cached_rows_by_exchange"),
            {
                "binance": [{"symbol": "BTCUSDT", "spread_pct": "1.23"}],
                "mexc": [{"symbol": "ETHUSDT", "spread_pct": "0.45"}],
            },
        )
        self.assertEqual(payload.get("selected_exchange"), "mexc")
        self.assertEqual(payload.get("daily_volume_text"), "10M")
        self.assertEqual(payload.get("daily_volume_threshold"), 10_000_000)


if __name__ == "__main__":
    unittest.main()
