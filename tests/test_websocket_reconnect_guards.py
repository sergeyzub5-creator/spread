from __future__ import annotations

import unittest
from unittest.mock import patch

from app.core.execution.binance_usdm_trade_ws import BinanceUsdmTradeWebSocketTransport, ExecutionTransportError
from app.core.market_data.bitget_linear_connector import BitgetLinearPublicConnector
from app.core.market_data.bybit_linear_connector import BybitLinearPublicConnector
from app.core.models.account import ExchangeCredentials


class _AliveThread:
    def is_alive(self) -> bool:
        return True

    def join(self, timeout: float | None = None) -> None:
        return


class WebsocketReconnectGuardTests(unittest.TestCase):
    def test_binance_trade_connect_does_not_start_duplicate_thread_during_active_attempt(self) -> None:
        transport = BinanceUsdmTradeWebSocketTransport(
            ExchangeCredentials(exchange="binance", api_key="key", api_secret="secret"),
        )
        transport._thread = _AliveThread()
        transport._connected = False
        transport._last_disconnect_ts_ms = 0
        transport._connect_error = ExecutionTransportError("connect failed")
        transport._opened_event.set()

        with (
            patch("app.core.execution.binance_usdm_trade_ws.threading.Thread", side_effect=AssertionError("should not create new thread")),
            patch.object(transport, "_sync_time_offset", return_value=None),
        ):
            with self.assertRaisesRegex(ExecutionTransportError, "connect failed"):
                transport.connect()

    def test_bybit_public_error_marks_disconnect_state(self) -> None:
        connector = BybitLinearPublicConnector()
        connector._connected = True

        connector._on_error(RuntimeError("Connection to remote host was lost."))

        self.assertFalse(connector._connected)
        self.assertGreater(connector._last_disconnect_ts_ms, 0)

    def test_bitget_public_error_marks_disconnect_state(self) -> None:
        connector = BitgetLinearPublicConnector()
        connector._connected = True

        connector._on_error(RuntimeError("Connection to remote host was lost."))

        self.assertFalse(connector._connected)
        self.assertGreater(connector._last_disconnect_ts_ms, 0)


if __name__ == "__main__":
    unittest.main()
