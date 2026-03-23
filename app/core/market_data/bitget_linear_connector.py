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
    STALE_STREAM_TIMEOUT_MS = 30000
    PING_INTERVAL_SECONDS = 30.0
    WATCHDOG_INTERVAL_SECONDS = 5.0
    MAX_SUBSCRIPTION_ARGS_PER_MESSAGE = 20
    MIN_SECONDS_BETWEEN_CONTROL_MESSAGES = 0.15
    SUBSCRIPTION_DEBOUNCE_SECONDS = 0.25

    def __init__(self) -> None:
        self.logger = get_logger("market_data.bitget_linear")
        self._callbacks: list[Callable[[object], None]] = []
        self._subscriptions: dict[tuple[str, str, str], InstrumentId] = {}
        self._connected = False
        self._closing = False
        self._ws_app = None
        self._thread: threading.Thread | None = None
        self._ping_thread: threading.Thread | None = None
        self._flush_thread: threading.Thread | None = None
        self._lock = threading.RLock()
        self._last_message_ts_ms = 0
        self._last_disconnect_ts_ms = 0
        self._last_control_message_ts = 0.0
        self._pending_subscriptions: dict[tuple[str, str, str], dict[str, str]] = {}
        self._pending_unsubscriptions: dict[tuple[str, str, str], dict[str, str]] = {}
        self._flush_requested = False

    def connect(self) -> None:
        if websocket is None:
            raise RuntimeError("websocket-client is required for BitgetLinearPublicConnector")
        stale_thread: threading.Thread | None = None
        stale_ws_app = None
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                now_ms = int(time.time() * 1000)
                disconnected_for_ms = now_ms - int(self._last_disconnect_ts_ms or 0)
                if self._connected or self._last_disconnect_ts_ms <= 0 or disconnected_for_ms < 2000:
                    return
                self.logger.warning(
                    "bitget linear public ws hard reconnect | disconnected_for_ms=%s",
                    disconnected_for_ms,
                )
                self._closing = True
                stale_thread = self._thread
                stale_ws_app = self._ws_app
            else:
                self._closing = False
                self._thread = threading.Thread(target=self._run_forever, name="bitget-linear-public-ws", daemon=True)
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
            self._thread = threading.Thread(target=self._run_forever, name="bitget-linear-public-ws", daemon=True)
            self._thread.start()

    def subscribe_l1(self, instrument: InstrumentId) -> None:
        key = self._subscription_key(instrument)
        arg = self._subscription_arg(instrument)
        with self._lock:
            self._subscriptions[key] = instrument
            self._pending_unsubscriptions.pop(key, None)
            self._pending_subscriptions[key] = arg
        self._schedule_subscription_flush()

    def unsubscribe_l1(self, instrument: InstrumentId) -> None:
        key = self._subscription_key(instrument)
        arg = self._subscription_arg(instrument)
        with self._lock:
            self._subscriptions.pop(key, None)
            self._pending_subscriptions.pop(key, None)
            self._pending_unsubscriptions[key] = arg
        self._schedule_subscription_flush()

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
            finally:
                with self._lock:
                    self._ws_app = None
            if self._closing:
                break
            time.sleep(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2.0, 15.0)

    def _on_open(self) -> None:
        self.logger.info("bitget linear public ws connected")
        self._connected = True
        self._last_disconnect_ts_ms = 0
        self._last_message_ts_ms = int(time.time() * 1000)
        self._start_ping_loop()
        with self._lock:
            self._pending_subscriptions = {
                self._subscription_key(instrument): self._subscription_arg(instrument)
                for instrument in self._subscriptions.values()
            }
            self._pending_unsubscriptions.clear()
        self._schedule_subscription_flush()

    def _on_message(self, message: str) -> None:
        if str(message).strip().lower() == "pong":
            self._last_message_ts_ms = int(time.time() * 1000)
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
        if channel not in ("books1", "ticker") or inst_type not in ("USDT-FUTURES", "COIN-FUTURES") or not inst_id:
            return
        data = payload.get("data")
        if not isinstance(data, list) or not data:
            return
        item = data[0]
        if not isinstance(item, dict):
            return
        with self._lock:
            instrument = self._subscriptions.get((inst_type, channel, inst_id))
        if instrument is None:
            return
        event = {
            "instrument": instrument,
            "payload": item,
            "ts_local": int(time.time() * 1000),
        }
        self._last_message_ts_ms = int(event["ts_local"])
        for callback in list(self._callbacks):
            try:
                callback(event)
            except Exception as exc:
                self.logger.error("bitget linear quote callback failed: %s", exc)

    def _on_error(self, error: Any) -> None:
        error_text = str(error or "").strip()
        self._connected = False
        self._last_disconnect_ts_ms = int(time.time() * 1000)
        if self._closing:
            self.logger.info("bitget linear public ws closing | error=%s", error_text)
            return
        if error_text in {"Connection to remote host was lost.", "socket is already closed."}:
            self.logger.warning("bitget linear public ws disconnected: %s", error_text)
            return
        self.logger.error("bitget linear public ws error: %s", error)

    def _on_close(self, code: Any, message: Any) -> None:
        self._connected = False
        self._last_disconnect_ts_ms = int(time.time() * 1000)
        self.logger.info("bitget linear public ws closed | code=%s | message=%s", code, message)

    def _start_ping_loop(self) -> None:
        with self._lock:
            if self._ping_thread is not None and self._ping_thread.is_alive():
                return
            self._ping_thread = threading.Thread(target=self._ping_loop, name="bitget-linear-public-ping", daemon=True)
            self._ping_thread.start()

    def _ping_loop(self) -> None:
        ping_elapsed_seconds = 0.0
        while not self._closing and self._connected:
            time.sleep(self.WATCHDOG_INTERVAL_SECONDS)
            if self._closing or not self._connected:
                return
            with self._lock:
                has_subscriptions = bool(self._subscriptions)
            if not has_subscriptions:
                continue
            now_ms = int(time.time() * 1000)
            last_message_ts_ms = int(self._last_message_ts_ms or 0)
            ws_app = self._ws_app
            if last_message_ts_ms > 0 and (now_ms - last_message_ts_ms) >= self.STALE_STREAM_TIMEOUT_MS:
                self.logger.warning(
                    "bitget linear public ws stale stream detected | silence_ms=%s | action=restart",
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
            ping_elapsed_seconds += self.WATCHDOG_INTERVAL_SECONDS
            if ping_elapsed_seconds < self.PING_INTERVAL_SECONDS:
                continue
            ping_elapsed_seconds = 0.0
            if ws_app is not None:
                try:
                    ws_app.send("ping")
                except Exception:
                    with self._lock:
                        self._connected = False
                    try:
                        ws_app.close()
                    except Exception:
                        pass
                    return

    def _send_subscriptions(self, op: str, args: list[dict[str, str]]) -> None:
        if not args:
            return
        for index in range(0, len(args), self.MAX_SUBSCRIPTION_ARGS_PER_MESSAGE):
            chunk = args[index : index + self.MAX_SUBSCRIPTION_ARGS_PER_MESSAGE]
            self._send_subscription_chunk(op, chunk)

    def _send_subscription_chunk(self, op: str, args: list[dict[str, str]]) -> None:
        with self._lock:
            ws_app = self._ws_app
            connected = self._connected
            elapsed = time.monotonic() - self._last_control_message_ts
        if ws_app is None or not connected or not args:
            return
        if elapsed < self.MIN_SECONDS_BETWEEN_CONTROL_MESSAGES:
            time.sleep(self.MIN_SECONDS_BETWEEN_CONTROL_MESSAGES - elapsed)
        try:
            ws_app.send(json.dumps({"op": op, "args": args}))
            with self._lock:
                self._last_control_message_ts = time.monotonic()
        except Exception as exc:
            self.logger.warning("bitget linear public ws send failed: %s", exc)
            with self._lock:
                self._connected = False
                self._last_disconnect_ts_ms = int(time.time() * 1000)
            try:
                ws_app.close()
            except Exception:
                pass

    def _schedule_subscription_flush(self) -> None:
        with self._lock:
            if self._flush_thread is not None and self._flush_thread.is_alive():
                self._flush_requested = True
                return
            self._flush_requested = True
            self._flush_thread = threading.Thread(target=self._flush_subscription_loop, name="bitget-linear-public-sub-flush", daemon=True)
            self._flush_thread.start()

    def _flush_subscription_loop(self) -> None:
        while True:
            time.sleep(self.SUBSCRIPTION_DEBOUNCE_SECONDS)
            with self._lock:
                requested = self._flush_requested
                self._flush_requested = False
            if not requested:
                return
            self._flush_subscription_ops()
            with self._lock:
                if not self._flush_requested:
                    return

    def _flush_subscription_ops(self) -> None:
        with self._lock:
            if not self._connected or self._ws_app is None:
                return
            sub_args = list(self._pending_subscriptions.values())
            unsub_args = list(self._pending_unsubscriptions.values())
            self._pending_subscriptions.clear()
            self._pending_unsubscriptions.clear()
        if unsub_args:
            self._send_subscriptions("unsubscribe", unsub_args)
        if sub_args:
            self._send_subscriptions("subscribe", sub_args)

    @staticmethod
    def _subscription_arg(instrument: InstrumentId) -> dict[str, str]:
        # COIN-M delivery — другой instType в Bitget v2 ws
        if instrument.market_type == "bitget_coin_delivery":
            inst_type = "COIN-FUTURES"
        else:
            inst_type = "USDT-FUTURES"
        return {
            "instType": inst_type,
            "channel": instrument.routing.ws_channel,
            "instId": str(instrument.routing.ws_symbol or instrument.symbol).strip().upper(),
        }

    @classmethod
    def _subscription_key(cls, instrument: InstrumentId) -> tuple[str, str, str]:
        arg = cls._subscription_arg(instrument)
        return (arg["instType"], arg["channel"], arg["instId"])
