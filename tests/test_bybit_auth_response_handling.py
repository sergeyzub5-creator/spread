from __future__ import annotations

import unittest

from app.core.execution.bybit_private_stream import BybitPrivateExecutionStream
from app.core.execution.bybit_trade_ws import BybitLinearTradeWebSocketTransport
from app.core.models.account import ExchangeCredentials


def _credentials() -> ExchangeCredentials:
    return ExchangeCredentials(exchange="bybit", api_key="key", api_secret="secret", api_passphrase="pass")


class BybitAuthResponseHandlingTests(unittest.TestCase):
    def test_trade_ws_auth_accepts_retcode_zero_without_success_flag(self) -> None:
        transport = BybitLinearTradeWebSocketTransport(_credentials())

        transport._on_message('{"op":"auth","retCode":0,"retMsg":"OK"}')

        self.assertTrue(transport._authenticated)
        self.assertIsNone(transport._connect_error)
        self.assertTrue(transport._auth_event.is_set())

    def test_private_stream_auth_accepts_retcode_zero_without_success_flag(self) -> None:
        stream = BybitPrivateExecutionStream(_credentials())
        subscribed: list[bool] = []
        stream._subscribe_topics = lambda: subscribed.append(True)  # type: ignore[method-assign]

        stream._on_message('{"op":"auth","retCode":0,"retMsg":"OK"}')

        self.assertTrue(stream._authenticated)
        self.assertIsNone(stream._connect_error)
        self.assertTrue(stream._auth_event.is_set())
        self.assertEqual(subscribed, [True])


if __name__ == "__main__":
    unittest.main()
