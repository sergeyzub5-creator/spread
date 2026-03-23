import unittest
from unittest.mock import patch

from app.core.accounts.binance_account_connector_impl import BinanceAccountConnector, BinanceApiError
from app.core.models.account import ExchangeCredentials


class BinanceAccountConnectorTests(unittest.TestCase):
    def test_connect_falls_back_to_portfolio_margin_snapshot(self) -> None:
        connector = BinanceAccountConnector()
        credentials = ExchangeCredentials(exchange="binance", api_key="key", api_secret="secret")

        def fake_signed_get(*, base_url: str, path: str, **kwargs):
            if path == connector.SPOT_ACCOUNT_PATH:
                return {
                    "canTrade": True,
                    "balances": [
                        {"asset": "USDT", "free": "12.5", "locked": "0"},
                    ],
                }
            if path == connector.FUTURES_ACCOUNT_PATH:
                raise BinanceApiError("[-2015] Invalid API-key, IP, or permissions for action")
            if path == connector.PAPI_ACCOUNT_PATH:
                return {"accountEquity": "250.5"}
            if path == connector.PAPI_UM_ACCOUNT_PATH:
                return {
                    "canTrade": True,
                    "totalWalletBalance": "240.0",
                    "positions": [
                        {
                            "symbol": "BTCUSDT",
                            "positionAmt": "0.01",
                            "positionSide": "LONG",
                            "unrealizedProfit": "5.25",
                        }
                    ],
                }
            raise AssertionError(f"unexpected path: {path}")

        with patch.object(connector, "_signed_get", side_effect=fake_signed_get):
            snapshot = connector.connect(credentials)

        self.assertTrue(snapshot.spot_enabled)
        self.assertTrue(snapshot.futures_enabled)
        self.assertEqual(snapshot.account_profile.get("account_type"), "portfolio_margin")
        self.assertEqual(snapshot.account_profile.get("preferred_execution_route"), "binance_usdm_trade_ws")
        self.assertIn("250.50", snapshot.balance_text)
        self.assertIn("1", snapshot.positions_text)

    def test_connect_uses_portfolio_margin_path_when_profile_hints_it(self) -> None:
        connector = BinanceAccountConnector()
        credentials = ExchangeCredentials(
            exchange="binance",
            api_key="key",
            api_secret="secret",
            account_profile={"account_type": "portfolio_margin"},
        )

        requested_paths: list[str] = []

        def fake_signed_get(*, path: str, **kwargs):
            requested_paths.append(path)
            if path == connector.SPOT_ACCOUNT_PATH:
                return {"canTrade": True, "balances": []}
            if path == connector.PAPI_ACCOUNT_PATH:
                return {"accountEquity": "100"}
            if path == connector.PAPI_UM_ACCOUNT_PATH:
                return {"canTrade": True, "positions": [], "totalWalletBalance": "90"}
            if path == connector.FUTURES_ACCOUNT_PATH:
                raise AssertionError("futures path should not be used when account type is portfolio_margin")
            raise AssertionError(f"unexpected path: {path}")

        with patch.object(connector, "_signed_get", side_effect=fake_signed_get):
            snapshot = connector.connect(credentials)

        self.assertEqual(snapshot.account_profile.get("account_type"), "portfolio_margin")
        self.assertIn(connector.PAPI_ACCOUNT_PATH, requested_paths)
        self.assertIn(connector.PAPI_UM_ACCOUNT_PATH, requested_paths)
        self.assertNotIn(connector.FUTURES_ACCOUNT_PATH, requested_paths)


if __name__ == "__main__":
    unittest.main()
