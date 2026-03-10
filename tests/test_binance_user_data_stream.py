from __future__ import annotations

import types
import unittest
from unittest.mock import MagicMock, patch

from app.core.execution import binance_usdm_user_data_stream as stream_module
from app.core.execution.binance_usdm_user_data_stream import BinanceUsdmUserDataStream
from app.core.models.account import ExchangeCredentials


class _FakeWebSocketApp:
    calls = 0

    def __init__(self, *_args, **_kwargs) -> None:
        _FakeWebSocketApp.calls += 1

    def run_forever(self) -> None:
        return

    def close(self) -> None:
        return


class BinanceUserDataStreamTests(unittest.TestCase):
    def _make_stream(self) -> BinanceUsdmUserDataStream:
        credentials = ExchangeCredentials(
            exchange="binance",
            api_key="key",
            api_secret="secret",
            api_passphrase="",
        )
        return BinanceUsdmUserDataStream(credentials=credentials, keepalive_interval_seconds=0.01)

    def test_keepalive_rotates_listen_key_on_http_4xx(self) -> None:
        stream = self._make_stream()
        stream._listen_key = "old-key"
        wait_results = iter([False, True])

        def fake_wait(_timeout: float) -> bool:
            return next(wait_results)

        with (
            patch.object(stream._keepalive_stop_event, "wait", side_effect=fake_wait),
            patch.object(stream, "_keepalive_listen_key", side_effect=RuntimeError("http 401")),
            patch.object(stream, "_create_listen_key", return_value="new-key"),
        ):
            stream._keepalive_loop()

        self.assertEqual(stream._listen_key, "new-key")

    def test_run_forever_schedules_reconnect_loop(self) -> None:
        stream = self._make_stream()
        stream._listen_key = "test-key"
        _FakeWebSocketApp.calls = 0
        sleep_calls = {"count": 0}

        def fake_sleep(_seconds: float) -> None:
            sleep_calls["count"] += 1
            if sleep_calls["count"] >= 2:
                stream._closing = True

        fake_websocket_module = types.SimpleNamespace(WebSocketApp=_FakeWebSocketApp)
        with (
            patch.object(stream_module, "websocket", fake_websocket_module),
            patch.object(stream_module.random, "uniform", return_value=0.0),
            patch.object(stream_module.time, "sleep", side_effect=fake_sleep),
        ):
            stream._run_forever()

        self.assertGreaterEqual(_FakeWebSocketApp.calls, 2)
        self.assertGreaterEqual(sleep_calls["count"], 2)

    def test_connect_does_not_spawn_duplicate_thread_while_reconnecting(self) -> None:
        stream = self._make_stream()
        stream._thread = MagicMock()
        stream._thread.is_alive.return_value = True
        stream._connected = False

        def fake_wait(*, timeout: float) -> bool:
            stream._connected = True
            return True

        with (
            patch.object(stream._opened_event, "wait", side_effect=fake_wait),
            patch.object(stream_module.threading, "Thread") as thread_ctor,
        ):
            stream.connect()

        for call_item in thread_ctor.call_args_list:
            target = call_item.kwargs.get("target")
            self.assertIsNot(target, stream._run_forever)


if __name__ == "__main__":
    unittest.main()
