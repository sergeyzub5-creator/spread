from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable
from typing import Any

from app.core.logging.logger_factory import get_logger
from app.core.market_data.connector import PublicMarketDataConnector
from app.core.models.instrument import InstrumentId

try:
    import websocket
except ImportError:  # pragma: no cover
    websocket = None


class BitgetLinearPublicConnector(PublicMarketDataConnector):
    WS_URL = "wss://ws.bitget.com/v2/ws/public"

    def __init__(self) -> None:
        self.logger = get_logger("market_data.bitget_linear")
        self._callbacks: list[Callable[[object], None]] = []
        self._subscriptions: dict[tuple[str, str, str], InstrumentId] = {}
        self._connected = False
        self._closing = False
        self._ws_app = None
        self._thread: threading.Thread | None = None
        self._ping_thread: threading.Thread | None = None
        self._lock = threading.RLock()

    def connect(self) -> None:
        if websocket is None:
            raise RuntimeError("websocket-client is required for BitgetLinearPublicConnector")
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._closing = False
            self._thread = threading.Thread(target=self._run_forever, name="bitget-linear-public-ws", daemon=True)
            self._thread.start()

    def subscribe_l1(self, instrument: InstrumentId) -> None:
        key = self._subscription_key(instrument)
        with self._lock:
            self._subscriptions[key] = instrument
        if self._ws_app is not None and self._connected:
            self._send_subscriptions("subscribe", [self._subscription_arg(instrument)])

    def unsubscribe_l1(self, instrument: InstrumentId) -> None:
        key = self._subscription_key(instrument)
        with self._lock:
            self._subscriptions.pop(key, None)
        if self._ws_app is not None and self._connected:
            self._send_subscriptions("unsubscribe", [self._subscription_arg(instrument)])

    def on_quote(self, callback: Callable[[object], None]) -> None:
        self._callbacks.append(callback)

    def close(self) -> None:
        with self._lock:
            self._closing = True
            ws_app = self._ws_app
        if ws_app is not None:
            ws_app.close()

    def _run_forever(self) -> None:
        backoff_seconds = 1.0
        while not self._closing:
            self._ws_app = websocket.WebSocketApp(
                self.WS_URL,
                on_open=lambda ws: self._on_open(),
                on_message=lambda ws, message: self._on_message(message),
                on_error=lambda ws, error: self._on_error(error),
                on_close=lambda ws, code, message: self._on_close(code, message),
            )
            try:
                self._ws_app.run_forever()
            except Exception as exc:  # pragma: no cover
                self.logger.error("bitget linear public ws loop crashed: %s", exc)
            if self._closing:
                break
            time.sleep(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2.0, 15.0)

    def _on_open(self) -> None:
        self.logger.info("bitget linear public ws connected")
        self._connected = True
        self._start_ping_loop()
        with self._lock:
            args = [self._subscription_arg(instrument) for instrument in self._subscriptions.values()]
        if args:
            self._send_subscriptions("subscribe", args)

    def _on_message(self, message: str) -> None:
        if str(message).strip().lower() == "pong":
            return
        payload = json.loads(message)
        if not isinstance(payload, dict):
            return
        if str(payload.get("event", "")).strip().lower() == "error":
            self.logger.error("bitget linear public ws subscribe error: %s", payload)
            return
        arg = payload.get("arg", {}) if isinstance(payload.get("arg"), dict) else {}
        channel = str(arg.get("channel", "")).strip()
        inst_type = str(arg.get("instType", "")).strip().upper()
        inst_id = str(arg.get("instId", "")).strip().upper()
        if channel != "books1" or inst_type != "USDT-FUTURES" or not inst_id:
            return
        data = payload.get("data")
        if not isinstance(data, list) or not data:
            return
        book = data[0]
        if not isinstance(book, dict):
            return
        with self._lock:
            instrument = self._subscriptions.get((inst_type, channel, inst_id))
        if instrument is None:
            return
        event = {
            "instrument": instrument,
            "payload": book,
            "ts_local": int(time.time() * 1000),
        }
        for callback in list(self._callbacks):
            callback(event)

    def _on_error(self, error: Any) -> None:
        self.logger.error("bitget linear public ws error: %s", error)

    def _on_close(self, code: Any, message: Any) -> None:
        self._connected = False
        self.logger.info("bitget linear public ws closed | code=%s | message=%s", code, message)

    def _start_ping_loop(self) -> None:
        with self._lock:
            if self._ping_thread is not None and self._ping_thread.is_alive():
                return
            self._ping_thread = threading.Thread(target=self._ping_loop, name="bitget-linear-public-ping", daemon=True)
            self._ping_thread.start()

    def _ping_loop(self) -> None:
        while not self._closing and self._connected:
            time.sleep(30.0)
            if self._closing or not self._connected:
                return
            ws_app = self._ws_app
            if ws_app is not None:
                try:
                    ws_app.send("ping")
                except Exception:
                    return

    def _send_subscriptions(self, op: str, args: list[dict[str, str]]) -> None:
        ws_app = self._ws_app
        if ws_app is None or not args:
            return
        ws_app.send(json.dumps({"op": op, "args": args}))

    @staticmethod
    def _subscription_arg(instrument: InstrumentId) -> dict[str, str]:
        return {
            "instType": "USDT-FUTURES",
            "channel": instrument.routing.ws_channel,
            "instId": instrument.routing.ws_symbol,
        }

    @classmethod
    def _subscription_key(cls, instrument: InstrumentId) -> tuple[str, str, str]:
        arg = cls._subscription_arg(instrument)
        return (arg["instType"], arg["channel"], arg["instId"])
