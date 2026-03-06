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


class BinanceUsdmPublicConnector(PublicMarketDataConnector):
    """Public WS connector for Binance USD-M futures L1 bookTicker stream."""

    WS_URL = "wss://fstream.binance.com/ws"

    def __init__(self) -> None:
        self.logger = get_logger("market_data.binance_usdm")
        self._callbacks: list[Callable[[object], None]] = []
        self._stream_to_instrument: dict[str, InstrumentId] = {}
        self._connected = False
        self._closing = False
        self._id_seq = 0
        self._ws_app = None
        self._thread: threading.Thread | None = None
        self._lock = threading.RLock()

    def connect(self) -> None:
        if websocket is None:
            raise RuntimeError("websocket-client is required for BinanceUsdmPublicConnector")

        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._closing = False
            self._thread = threading.Thread(target=self._run_forever, name="binance-usdm-public-ws", daemon=True)
            self._thread.start()

    def subscribe_l1(self, instrument: InstrumentId) -> None:
        stream_name = self._stream_name(instrument)
        with self._lock:
            self._stream_to_instrument[stream_name] = instrument
            ws_app = self._ws_app
        if ws_app is not None and self._connected:
            self._send({"method": "SUBSCRIBE", "params": [stream_name], "id": self._next_id()})

    def unsubscribe_l1(self, instrument: InstrumentId) -> None:
        stream_name = self._stream_name(instrument)
        with self._lock:
            self._stream_to_instrument.pop(stream_name, None)
        if self._ws_app is not None and self._connected:
            self._send({"method": "UNSUBSCRIBE", "params": [stream_name], "id": self._next_id()})

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
                on_close=lambda ws, status_code, message: self._on_close(status_code, message),
            )
            try:
                self._ws_app.run_forever()
            except Exception as exc:  # pragma: no cover
                self.logger.error("binance public ws loop crashed: %s", exc)

            if self._closing:
                break
            time.sleep(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2.0, 15.0)

    def _on_open(self) -> None:
        self.logger.info("binance public ws connected")
        self._connected = True
        with self._lock:
            streams = list(self._stream_to_instrument.keys())
        if streams:
            self._send({"method": "SUBSCRIBE", "params": streams, "id": self._next_id()})

    def _on_message(self, message: str) -> None:
        data = json.loads(message)
        if not isinstance(data, dict):
            return
        if "result" in data:
            return

        payload = data.get("data", data)
        symbol = str(payload.get("s", "")).lower()
        if not symbol:
            return

        stream_name = f"{symbol}@bookTicker"
        with self._lock:
            instrument = self._stream_to_instrument.get(stream_name)
        if instrument is None:
            return

        event = {
            "instrument": instrument,
            "payload": payload,
            "ts_local": int(time.time() * 1000),
        }
        for callback in list(self._callbacks):
            callback(event)

    def _on_error(self, error: Any) -> None:
        self.logger.error("binance public ws error: %s", error)

    def _on_close(self, status_code: Any, message: Any) -> None:
        self._connected = False
        self.logger.info("binance public ws closed | code=%s | message=%s", status_code, message)

    def _send(self, payload: dict[str, Any]) -> None:
        ws_app = self._ws_app
        if ws_app is None:
            return
        ws_app.send(json.dumps(payload))

    def _next_id(self) -> int:
        with self._lock:
            self._id_seq += 1
            return self._id_seq

    @staticmethod
    def _stream_name(instrument: InstrumentId) -> str:
        return f"{instrument.routing.ws_symbol}@{instrument.routing.ws_channel}"
