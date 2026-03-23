from __future__ import annotations

import types
import unittest
from unittest.mock import patch

from app.core.execution import binance_usdm_user_data_stream as binance_stream_module
from app.core.execution import bitget_linear_private_stream as bitget_stream_module
from app.core.execution import bitget_linear_trade_ws as bitget_trade_module
from app.core.execution import bybit_private_stream as bybit_stream_module
from app.core.execution import bybit_trade_ws as bybit_trade_module
from app.core.execution.binance_usdm_user_data_stream import BinanceUsdmUserDataStream
from app.core.execution.bitget_linear_private_stream import BitgetLinearPrivateExecutionStream
from app.core.execution.bitget_linear_trade_ws import BitgetLinearTradeWebSocketTransport
from app.core.execution.bybit_private_stream import BybitPrivateExecutionStream
from app.core.execution.bybit_trade_ws import BybitLinearTradeWebSocketTransport
from app.core.models.account import ExchangeCredentials


class _FakeLoopWebSocketApp:
    calls = 0

    def __init__(self, *_args, **_kwargs) -> None:
        _FakeLoopWebSocketApp.calls += 1

    def run_forever(self) -> None:
        return

    def close(self) -> None:
        return

    def send(self, _payload: str) -> None:
        return


class ExecutionStreamDiagnosticsTests(unittest.TestCase):
    def _credentials(self, exchange: str) -> ExchangeCredentials:
        return ExchangeCredentials(
            exchange=exchange,
            api_key="key",
            api_secret="secret",
            api_passphrase="pass",
        )

    def test_bybit_diagnostics_updates_on_error_and_close(self) -> None:
        stream = BybitPrivateExecutionStream(self._credentials("bybit"))
        stream._on_error(RuntimeError("boom"))
        stream._on_close(1006, "closed")
        diagnostics = stream.diagnostics()

        self.assertEqual(diagnostics["last_error_text"], "boom")
        self.assertEqual(diagnostics["last_disconnect_code"], "1006")
        self.assertEqual(diagnostics["last_disconnect_message"], "closed")

    def test_bitget_diagnostics_updates_on_error_and_close(self) -> None:
        stream = BitgetLinearPrivateExecutionStream(self._credentials("bitget"))
        stream._on_error(RuntimeError("broken"))
        stream._on_close(1001, "bye")
        diagnostics = stream.diagnostics()

        self.assertEqual(diagnostics["last_error_text"], "broken")
        self.assertEqual(diagnostics["last_disconnect_code"], "1001")
        self.assertEqual(diagnostics["last_disconnect_message"], "bye")

    def test_binance_diagnostics_updates_on_error_and_close(self) -> None:
        stream = BinanceUsdmUserDataStream(self._credentials("binance"))
        stream._on_error(RuntimeError("drop"))
        stream._on_close(1000, "normal")
        diagnostics = stream.diagnostics()

        self.assertEqual(diagnostics["last_error_text"], "drop")
        self.assertEqual(diagnostics["last_disconnect_code"], "1000")
        self.assertEqual(diagnostics["last_disconnect_message"], "normal")

    def test_bybit_trade_diagnostics_updates_on_error_and_close(self) -> None:
        transport = BybitLinearTradeWebSocketTransport(self._credentials("bybit"))
        transport._on_error(RuntimeError("boom"))
        transport._on_close(1006, "closed")
        diagnostics = transport.diagnostics()

        self.assertEqual(diagnostics["last_error_text"], "boom")
        self.assertEqual(diagnostics["last_disconnect_code"], "1006")
        self.assertEqual(diagnostics["last_disconnect_message"], "closed")

    def test_bybit_trade_diagnostics_track_ping_pong(self) -> None:
        transport = BybitLinearTradeWebSocketTransport(self._credentials("bybit"))

        transport._on_message('{"op":"pong"}')

        diagnostics = transport.diagnostics()

        self.assertIsNotNone(diagnostics["last_pong_ts_ms"])
        self.assertIn("ping_fail_count", diagnostics)

    def test_bitget_trade_diagnostics_updates_on_error_and_close(self) -> None:
        transport = BitgetLinearTradeWebSocketTransport(self._credentials("bitget"))
        transport._on_error(RuntimeError("broken"))
        transport._on_close(1001, "bye")
        diagnostics = transport.diagnostics()

        self.assertEqual(diagnostics["last_error_text"], "broken")
        self.assertEqual(diagnostics["last_disconnect_code"], "1001")
        self.assertEqual(diagnostics["last_disconnect_message"], "bye")

    def test_reconnect_attempt_counters_increment(self) -> None:
        bybit_stream = BybitPrivateExecutionStream(self._credentials("bybit"))
        bitget_stream = BitgetLinearPrivateExecutionStream(self._credentials("bitget"))
        binance_stream = BinanceUsdmUserDataStream(self._credentials("binance"))
        bybit_trade = BybitLinearTradeWebSocketTransport(self._credentials("bybit"))
        bitget_trade = BitgetLinearTradeWebSocketTransport(self._credentials("bitget"))
        binance_stream._listen_key = "listen-key"

        _FakeLoopWebSocketApp.calls = 0

        def bybit_sleep(_seconds: float) -> None:
            bybit_stream._closing = True

        def bitget_sleep(_seconds: float) -> None:
            bitget_stream._closing = True

        def binance_sleep(_seconds: float) -> None:
            binance_stream._closing = True

        def bybit_trade_sleep(_seconds: float) -> None:
            bybit_trade._closing = True

        def bitget_trade_sleep(_seconds: float) -> None:
            bitget_trade._closing = True

        with (
            patch.object(bybit_stream_module, "websocket", types.SimpleNamespace(WebSocketApp=_FakeLoopWebSocketApp)),
            patch.object(bybit_stream_module.random, "uniform", return_value=0.0),
            patch.object(bybit_stream_module.time, "sleep", side_effect=bybit_sleep),
        ):
            bybit_stream._run_forever()
        with (
            patch.object(bitget_stream_module, "websocket", types.SimpleNamespace(WebSocketApp=_FakeLoopWebSocketApp)),
            patch.object(bitget_stream_module.random, "uniform", return_value=0.0),
            patch.object(bitget_stream_module.time, "sleep", side_effect=bitget_sleep),
        ):
            bitget_stream._run_forever()
        with (
            patch.object(binance_stream_module, "websocket", types.SimpleNamespace(WebSocketApp=_FakeLoopWebSocketApp)),
            patch.object(binance_stream_module.random, "uniform", return_value=0.0),
            patch.object(binance_stream_module.time, "sleep", side_effect=binance_sleep),
        ):
            binance_stream._run_forever()
        with (
            patch.object(bybit_trade_module, "websocket", types.SimpleNamespace(WebSocketApp=_FakeLoopWebSocketApp)),
            patch.object(bybit_trade_module.random, "uniform", return_value=0.0),
            patch.object(bybit_trade_module.time, "sleep", side_effect=bybit_trade_sleep),
        ):
            bybit_trade._run_forever()
        with (
            patch.object(bitget_trade_module, "websocket", types.SimpleNamespace(WebSocketApp=_FakeLoopWebSocketApp)),
            patch.object(bitget_trade_module.random, "uniform", return_value=0.0),
            patch.object(bitget_trade_module.time, "sleep", side_effect=bitget_trade_sleep),
        ):
            bitget_trade._run_forever()

        self.assertGreaterEqual(bybit_stream.diagnostics()["reconnect_attempts_total"], 1)
        self.assertGreaterEqual(bitget_stream.diagnostics()["reconnect_attempts_total"], 1)
        self.assertGreaterEqual(binance_stream.diagnostics()["reconnect_attempts_total"], 1)
        self.assertGreaterEqual(bybit_trade.diagnostics()["reconnect_attempts_total"], 1)
        self.assertGreaterEqual(bitget_trade.diagnostics()["reconnect_attempts_total"], 1)


if __name__ == "__main__":
    unittest.main()
