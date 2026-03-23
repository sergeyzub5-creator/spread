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


class BinanceSpotPublicConnector(PublicMarketDataConnector):
    """Public WS connector for Binance spot L1 bookTicker stream."""

    WS_URL = "wss://stream.binance.com:9443/ws"
    STALE_STREAM_TIMEOUT_MS = 10000
    WATCHDOG_INTERVAL_SECONDS = 5.0

    def __init__(self) -> None:
        self.logger = get_logger("market_data.binance_spot")
        self._callbacks: list[Callable[[object], None]] = []
        self._stream_to_instrument: dict[str, InstrumentId] = {}
        self._connected = False
        self._closing = False
        self._id_seq = 0
        self._ws_app = None
        self._thread: threading.Thread | None = None
        self._watchdog_thread: threading.Thread | None = None
        self._lock = threading.RLock()
        self._last_disconnect_ts_ms = 0
        self._last_message_ts_ms = 0

    def connect(self) -> None:
        if websocket is None:
            raise RuntimeError("websocket-client is required for BinanceSpotPublicConnector")
        stale_thread: threading.Thread | None = None
        stale_ws_app = None
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                now_ms = int(time.time() * 1000)
                disconnected_for_ms = now_ms - int(self._last_disconnect_ts_ms or 0)
                if self._connected or self._last_disconnect_ts_ms <= 0 or disconnected_for_ms < 2000:
                    return
                self.logger.warning(
                    "binance spot public ws hard reconnect | disconnected_for_ms=%s",
                    disconnected_for_ms,
                )
                self._closing = True
                stale_thread = self._thread
                stale_ws_app = self._ws_app
            else:
                self._closing = False
                self._thread = threading.Thread(target=self._run_forever, name="binance-spot-public-ws", daemon=True)
                self._thread.start()
                return

        if stale_ws_app is not None:
            try:
                stale_ws_app.close()
            except Exception:
                pass
        if stale_thread is not None and stale_thread is not threading.current_thread():
            stale_thread.join(timeout=1.0)
        with self._lock:
            self._closing = False
            self._thread = threading.Thread(target=self._run_forever, name="binance-spot-public-ws", daemon=True)
            self._thread.start()

    def subscribe_l1(self, instrument: InstrumentId) -> None:
        stream_name = self._stream_name(instrument)
        with self._lock:
            self._stream_to_instrument[stream_name] = instrument
        if self._ws_app is not None and self._connected:
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
                self._ws_app.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as exc:  # pragma: no cover
                self.logger.error("binance spot public ws loop crashed: %s", exc)
            finally:
                with self._lock:
                    self._ws_app = None
            if self._closing:
                break
            time.sleep(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2.0, 15.0)

    def _on_open(self) -> None:
        self.logger.info("binance spot public ws connected")
        self._connected = True
        self._last_disconnect_ts_ms = 0
        self._last_message_ts_ms = int(time.time() * 1000)
        self._start_watchdog_loop()
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
        self._last_message_ts_ms = int(time.time() * 1000)
        stream_name = f"{symbol}@bookTicker"
        with self._lock:
            instrument = self._stream_to_instrument.get(stream_name)
        if instrument is None:
            return
        event = {"instrument": instrument, "payload": payload, "ts_local": int(time.time() * 1000)}
        for callback in list(self._callbacks):
            try:
                callback(event)
            except Exception as exc:
                self.logger.error("binance spot quote callback failed: %s", exc)

    def _on_error(self, error: Any) -> None:
        error_text = str(error or "").strip()
        self._connected = False
        self._last_disconnect_ts_ms = int(time.time() * 1000)
        if self._closing:
            self.logger.info("binance spot public ws closing | error=%s", error_text)
            return
        if error_text in {"Connection to remote host was lost.", "socket is already closed.", "'NoneType' object has no attribute 'sock'"}:
            self.logger.warning("binance spot public ws disconnected: %s", error_text)
            return
        self.logger.error("binance spot public ws error: %s", error)

    def _on_close(self, status_code: Any, message: Any) -> None:
        self._connected = False
        self._last_disconnect_ts_ms = int(time.time() * 1000)
        self.logger.info("binance spot public ws closed | code=%s | message=%s", status_code, message)

    def _start_watchdog_loop(self) -> None:
        with self._lock:
            if self._watchdog_thread is not None and self._watchdog_thread.is_alive():
                return
            self._watchdog_thread = threading.Thread(target=self._watchdog_loop, name="binance-spot-public-watchdog", daemon=True)
            self._watchdog_thread.start()

    def _watchdog_loop(self) -> None:
        while not self._closing and self._connected:
            time.sleep(self.WATCHDOG_INTERVAL_SECONDS)
            if self._closing or not self._connected:
                return
            with self._lock:
                has_subscriptions = bool(self._stream_to_instrument)
                ws_app = self._ws_app
            if not has_subscriptions:
                continue
            now_ms = int(time.time() * 1000)
            last_message_ts_ms = int(self._last_message_ts_ms or 0)
            if last_message_ts_ms > 0 and (now_ms - last_message_ts_ms) >= self.STALE_STREAM_TIMEOUT_MS:
                self.logger.warning(
                    "binance spot public ws stale stream detected | silence_ms=%s | action=restart",
                    now_ms - last_message_ts_ms,
                )
                with self._lock:
                    self._connected = False
                    self._last_disconnect_ts_ms = int(time.time() * 1000)
                try:
                    if ws_app is not None:
                        ws_app.close()
                except Exception:
                    pass
                return

    def _send(self, payload: dict[str, Any]) -> None:
        with self._lock:
            ws_app = self._ws_app
            connected = self._connected
        if ws_app is None or not connected:
            return
        try:
            ws_app.send(json.dumps(payload))
        except Exception as exc:
            self.logger.warning("binance spot public ws send failed: %s", exc)
            with self._lock:
                self._connected = False
                self._last_disconnect_ts_ms = int(time.time() * 1000)
            try:
                ws_app.close()
            except Exception:
                pass

    def _next_id(self) -> int:
        with self._lock:
            self._id_seq += 1
            return self._id_seq

    @staticmethod
    def _stream_name(instrument: InstrumentId) -> str:
        return f"{instrument.routing.ws_symbol}@{instrument.routing.ws_channel}"
