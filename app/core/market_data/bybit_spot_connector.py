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


class BybitSpotPublicConnector(PublicMarketDataConnector):
    WS_URL = "wss://stream.bybit.com/v5/public/spot"
    STALE_STREAM_TIMEOUT_MS = 30000
    WATCHDOG_INTERVAL_SECONDS = 5.0
    PING_INTERVAL_SECONDS = 20.0

    def __init__(self) -> None:
        self.logger = get_logger("market_data.bybit_spot")
        self._callbacks: list[Callable[[object], None]] = []
        self._topic_to_instrument: dict[str, InstrumentId] = {}
        self._connected = False
        self._closing = False
        self._ws_app = None
        self._thread: threading.Thread | None = None
        self._watchdog_thread: threading.Thread | None = None
        self._lock = threading.RLock()
        self._last_message_ts_ms = 0

    def connect(self) -> None:
        if websocket is None:
            raise RuntimeError("websocket-client is required for BybitSpotPublicConnector")
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._closing = False
            self._thread = threading.Thread(target=self._run_forever, name="bybit-spot-public-ws", daemon=True)
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
                self.logger.error("bybit spot public ws loop crashed: %s", exc)
            if self._closing:
                break
            time.sleep(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2.0, 15.0)

    def _on_open(self) -> None:
        self.logger.info("bybit spot public ws connected")
        self._connected = True
        self._last_message_ts_ms = int(time.time() * 1000)
        self._start_watchdog_loop()
        with self._lock:
            topics = list(self._topic_to_instrument.keys())
        if topics:
            self._send({"op": "subscribe", "args": topics})

    def _on_message(self, message: str) -> None:
        payload = json.loads(message)
        if not isinstance(payload, dict):
            return
        if self._is_pong_message(payload):
            self._last_message_ts_ms = int(time.time() * 1000)
            return
        topic = str(payload.get("topic", ""))
        if not topic.startswith("orderbook.1."):
            return
        with self._lock:
            instrument = self._topic_to_instrument.get(topic)
        if instrument is None:
            return
        self._last_message_ts_ms = int(time.time() * 1000)
        event = {
            "instrument": instrument,
            "payload": payload.get("data", {}),
            "ts_local": int(time.time() * 1000),
        }
        for callback in list(self._callbacks):
            try:
                callback(event)
            except Exception as exc:
                self.logger.error("bybit spot quote callback failed: %s", exc)

    def _on_error(self, error: Any) -> None:
        self.logger.error("bybit spot public ws error: %s", error)

    def _on_close(self, code: Any, message: Any) -> None:
        self._connected = False
        self.logger.info("bybit spot public ws closed | code=%s | message=%s", code, message)

    def _start_watchdog_loop(self) -> None:
        with self._lock:
            if self._watchdog_thread is not None and self._watchdog_thread.is_alive():
                return
            self._watchdog_thread = threading.Thread(
                target=self._watchdog_loop,
                name="bybit-spot-public-watchdog",
                daemon=True,
            )
            self._watchdog_thread.start()

    def _watchdog_loop(self) -> None:
        ping_elapsed_seconds = 0.0
        while not self._closing and self._connected:
            time.sleep(self.WATCHDOG_INTERVAL_SECONDS)
            if self._closing or not self._connected:
                return
            with self._lock:
                has_subscriptions = bool(self._topic_to_instrument)
            if not has_subscriptions:
                continue
            now_ms = int(time.time() * 1000)
            last_message_ts_ms = int(self._last_message_ts_ms or 0)
            ws_app = self._ws_app
            if last_message_ts_ms > 0 and (now_ms - last_message_ts_ms) >= self.STALE_STREAM_TIMEOUT_MS:
                self.logger.warning(
                    "bybit spot public ws stale stream detected | silence_ms=%s | action=restart",
                    now_ms - last_message_ts_ms,
                )
                with self._lock:
                    self._connected = False
                try:
                    if ws_app is not None:
                        ws_app.close()
                except Exception:
                    pass
                return
            ping_elapsed_seconds += self.WATCHDOG_INTERVAL_SECONDS
            if ping_elapsed_seconds < self.PING_INTERVAL_SECONDS:
                continue
            ping_elapsed_seconds = 0.0
            self._send({"op": "ping"})

    def _send(self, payload: dict[str, Any]) -> None:
        with self._lock:
            ws_app = self._ws_app
            connected = self._connected
        if ws_app is None or not connected:
            return
        try:
            ws_app.send(json.dumps(payload))
        except Exception as exc:
            self.logger.warning("bybit spot public ws send failed: %s", exc)
            with self._lock:
                self._connected = False
            try:
                ws_app.close()
            except Exception:
                pass

    def _is_pong_message(self, payload: dict[str, Any]) -> bool:
        op = str(payload.get("op", "")).strip().lower()
        ret_msg = str(payload.get("ret_msg", "")).strip().lower()
        if op == "pong":
            return True
        if op == "ping" and ret_msg == "pong":
            return True
        return False

    @staticmethod
    def _topic_name(instrument: InstrumentId) -> str:
        return f"{instrument.routing.ws_channel}.{instrument.routing.ws_symbol}"
