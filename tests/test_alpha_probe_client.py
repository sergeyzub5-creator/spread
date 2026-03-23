import unittest

from app.alpha_probe.client import AlphaProbeHttpClient


class AlphaProbeClientTests(unittest.TestCase):
    def test_build_signed_params_adds_signature_and_timestamp(self) -> None:
        client = AlphaProbeHttpClient()
        client._get_server_time = lambda _base_url: 1234567890  # type: ignore[method-assign]

        params = client._build_signed_params(
            params={"symbol": "BTCUSDT", "side": "BUY"},
            api_secret="secret",
            time_base_url="https://api.binance.com",
        )

        self.assertEqual(params["timestamp"], "1234567890")
        self.assertEqual(params["recvWindow"], "5000")
        self.assertIn("signature", params)
        self.assertTrue(str(params["signature"]))


if __name__ == "__main__":
    unittest.main()
