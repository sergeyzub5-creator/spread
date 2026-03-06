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


class BybitLinearPublicConnector(PublicMarketDataConnector):
    WS_URL = "wss://stream.bybit.com/v5/public/linear"

    def __init__(self) -> None:
        self.logger = get_logger("market_data.bybit_linear")
        self._callbacks: list[Callable[[object], None]] = []
        self._topic_to_instrument: dict[str, InstrumentId] = {}
        self._connected = False
        self._closing = False
        self._ws_app = None
        self._thread: threading.Thread | None = None
        self._lock = threading.RLock()

    def connect(self) -> None:
        if websocket is None:
            raise RuntimeError("websocket-client is required for BybitLinearPublicConnector")
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._closing = False
            self._thread = threading.Thread(target=self._run_forever, name="bybit-linear-public-ws", daemon=True)
            self._thread.start()

    def subscribe_l1(self, instrument: InstrumentId) -> None:
        topic = self._topic_name(instrument)
        with self._lock:
            self._topic_to_instrument[topic] = instrument
        if self._ws_app is not None and self._connected:
            self._send({"op": "subscribe", "args": [topic]})

    def unsubscribe_l1(self, instrument: InstrumentId) -> None:
        topic = self._topic_name(instrument)
        with self._lock:
            self._topic_to_instrument.pop(topic, None)
        if self._ws_app is not None and self._connected:
            self._send({"op": "unsubscribe", "args": [topic]})

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
                self.logger.error("bybit public ws loop crashed: %s", exc)
            if self._closing:
                break
            time.sleep(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2.0, 15.0)

    def _on_open(self) -> None:
        self._connected = True
        self.logger.info("bybit public ws connected")
        with self._lock:
            topics = list(self._topic_to_instrument.keys())
        if topics:
            self._send({"op": "subscribe", "args": topics})

    def _on_message(self, message: str) -> None:
        payload = json.loads(message)
        if not isinstance(payload, dict):
            return
        topic = str(payload.get("topic", "")).strip()
        if not topic:
            return
        with self._lock:
            instrument = self._topic_to_instrument.get(topic)
        if instrument is None:
            return
        data = payload.get("data", {})
        if not isinstance(data, dict):
            return
        event = {"instrument": instrument, "payload": data, "ts_local": int(time.time() * 1000)}
        for callback in list(self._callbacks):
            callback(event)

    def _on_error(self, error: Any) -> None:
        self.logger.error("bybit public ws error: %s", error)

    def _on_close(self, code: Any, message: Any) -> None:
        self._connected = False
        self.logger.info("bybit public ws closed | code=%s | message=%s", code, message)

    def _send(self, payload: dict[str, Any]) -> None:
        ws_app = self._ws_app
        if ws_app is None:
            return
        ws_app.send(json.dumps(payload))

    @staticmethod
    def _topic_name(instrument: InstrumentId) -> str:
        return f"{instrument.routing.ws_channel}.{instrument.routing.ws_symbol}"
