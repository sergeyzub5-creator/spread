from __future__ import annotations

import base64
import hashlib
import hmac
import json
import random
import threading
import time
from collections.abc import Callable
from typing import Any

from app.core.logging.logger_factory import get_logger
from app.core.models.account import ExchangeCredentials
from app.core.models.execution import ExecutionStreamEvent

try:
    import websocket
except ImportError:  # pragma: no cover
    websocket = None


class BitgetLinearPrivateExecutionStream:
    WS_URL = "wss://ws.bitget.com/v2/ws/private"
    VERIFY_PATH = "/user/verify"
    PRODUCT_TYPE = "USDT-FUTURES"
    CLOSE_JOIN_TIMEOUT_SECONDS = 2.0

    def __init__(
        self,
        credentials: ExchangeCredentials,
        *,
        timeout_seconds: float = 10.0,
        ping_interval_seconds: float = 30.0,
    ) -> None:
        self._credentials = credentials
        self._timeout_seconds = float(timeout_seconds)
        self._ping_interval_seconds = float(ping_interval_seconds)
        self._logger = get_logger("execution.bitget_linear_private_stream")
        self._callbacks: list[Callable[[ExecutionStreamEvent], None]] = []
        self._lock = threading.RLock()
        self._ws_app = None
        self._thread: threading.Thread | None = None
        self._ping_thread: threading.Thread | None = None
        self._connected = False
        self._authenticated = False
        self._closing = False
        self._opened_event = threading.Event()
        self._auth_event = threading.Event()
        self._connect_error: Exception | None = None
        self._session_ready = False
        self._reconnect_attempts_total = 0
        self._last_disconnect_code: str | None = None
        self._last_disconnect_message: str | None = None
        self._last_error_text: str | None = None
        self._last_ping_ts_ms = 0
        self._last_pong_ts_ms = 0
        self._ping_fail_count = 0

    def connect(self) -> None:
        if websocket is None:
            raise RuntimeError("websocket-client is required for BitgetLinearPrivateExecutionStream")
        with self._lock:
            if self._thread is not None and self._thread.is_alive() and self._connected and self._authenticated:
                self._logger.info("bitget private stream reuse existing connection")
                return
            if self._thread is not None and self._thread.is_alive():
                self._logger.info("bitget private stream waiting for active connection attempt")
            else:
                self._closing = False
                self._connected = False
                self._authenticated = False
                self._connect_error = None
                self._opened_event.clear()
                self._auth_event.clear()
                self._thread = threading.Thread(target=self._run_forever, name="bitget-linear-private-stream", daemon=True)
                self._thread.start()
            if self._ping_thread is None or not self._ping_thread.is_alive():
                self._ping_thread = threading.Thread(target=self._ping_loop, name="bitget-linear-private-ping", daemon=True)
                self._ping_thread.start()
            if self._connected and self._authenticated:
                self._logger.info("bitget private stream reuse existing connection")
            else:
                self._logger.info("bitget private stream starting/awaiting connection | execution_stack=classic_v2_private_ws")

        if not self._opened_event.wait(timeout=self._timeout_seconds):
            raise RuntimeError("bitget private stream connect timeout")
        if not self._auth_event.wait(timeout=self._timeout_seconds):
            raise RuntimeError("bitget private stream auth timeout")
        if self._connect_error is not None:
            raise RuntimeError(str(self._connect_error))
        if not self._connected or not self._authenticated:
            raise RuntimeError("bitget private stream failed to connect")

    def on_execution_event(self, callback: Callable[[ExecutionStreamEvent], None]) -> None:
        self._callbacks.append(callback)

    def close(self) -> None:
        with self._lock:
            self._closing = True
            ws_app = self._ws_app
            thread = self._thread
            ping_thread = self._ping_thread
        if ws_app is not None:
            ws_app.close()
        if thread is not None and thread.is_alive() and thread is not threading.current_thread():
            thread.join(timeout=self.CLOSE_JOIN_TIMEOUT_SECONDS)
        if ping_thread is not None and ping_thread.is_alive() and ping_thread is not threading.current_thread():
            ping_thread.join(timeout=self.CLOSE_JOIN_TIMEOUT_SECONDS)
        with self._lock:
            self._connected = False
            self._authenticated = False
            self._ws_app = None
            self._thread = None
            self._ping_thread = None

    def _run_forever(self) -> None:
        backoff_seconds = 1.0
        while not self._closing:
            with self._lock:
                self._opened_event.clear()
                self._auth_event.clear()
                self._session_ready = False
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
                self._connect_error = exc
                self._opened_event.set()
                self._auth_event.set()
                self._logger.error("bitget private stream loop crashed: %s", exc)
            finally:
                with self._lock:
                    self._ws_app = None
            if self._closing:
                break
            delay = min(backoff_seconds, 15.0)
            jitter = random.uniform(0.0, delay * 0.3)
            self._reconnect_attempts_total += 1
            self._logger.warning("bitget private stream reconnect scheduled | delay_seconds=%.2f", delay + jitter)
            time.sleep(delay + jitter)
            if self._session_ready:
                backoff_seconds = 1.0
            else:
                backoff_seconds = min(backoff_seconds * 2.0, 15.0)

    def _ping_loop(self) -> None:
        while not self._closing:
            time.sleep(self._ping_interval_seconds)
            if self._closing:
                return
            with self._lock:
                connected = self._connected
                authenticated = self._authenticated
                ws_app = self._ws_app
            if not connected or not authenticated or ws_app is None:
                continue
            try:
                self._last_ping_ts_ms = int(time.time() * 1000)
                self._send_raw("ping")
            except Exception:
                self._ping_fail_count += 1
                with self._lock:
                    self._connected = False
                try:
                    ws_app.close()
                except Exception:
                    pass

    def _on_open(self) -> None:
        self._connected = True
        self._opened_event.set()
        self._send(self._login_payload())
        self._logger.info("bitget private stream connected")

    def _on_message(self, message: str) -> None:
        if str(message).strip().lower() == "pong":
            self._last_pong_ts_ms = int(time.time() * 1000)
            return
        payload = json.loads(message)
        if not isinstance(payload, dict):
            return
        event = str(payload.get("event", "")).strip().lower()
        if event == "login":
            code = str(payload.get("code", "")).strip()
            if code == "0":
                self._authenticated = True
                self._session_ready = True
                self._auth_event.set()
                self._subscribe_topics()
                self._logger.info("bitget private stream authenticated")
            else:
                self._connect_error = RuntimeError(self._error_message(payload))
                self._auth_event.set()
            return
        if event == "error" and not self._authenticated:
            self._connect_error = RuntimeError(self._error_message(payload))
            self._auth_event.set()
            return
        if event in {"subscribe", "unsubscribe"}:
            return
        if event == "error":
            self._logger.warning("bitget private stream error event: %s", payload)
            return

        arg = payload.get("arg", {}) if isinstance(payload.get("arg"), dict) else {}
        channel = str(arg.get("channel", "")).strip().lower()
        inst_type = str(arg.get("instType", "")).strip().upper()
        if inst_type != self.PRODUCT_TYPE or channel not in {"orders", "fill"}:
            return
        for event_item in self._normalize_events(channel, payload):
            self._logger.debug(
                "bitget private event received | channel=%s | symbol=%s | order_id=%s | status=%s | exec_type=%s",
                channel,
                event_item.symbol,
                event_item.order_id,
                event_item.order_status,
                event_item.execution_type,
            )
            for callback in list(self._callbacks):
                callback(event_item)

    def _subscribe_topics(self) -> None:
        self._send(
            {
                "op": "subscribe",
                "args": [
                    {"instType": self.PRODUCT_TYPE, "channel": "orders", "instId": "default"},
                    {"instType": self.PRODUCT_TYPE, "channel": "fill", "instId": "default"},
                ],
            }
        )

    def _normalize_events(self, channel: str, payload: dict[str, Any]) -> list[ExecutionStreamEvent]:
        items = payload.get("data", [])
        if not isinstance(items, list):
            return []
        events: list[ExecutionStreamEvent] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            symbol = self._str_or_none(item.get("instId") or item.get("symbol"))
            order_id = self._str_or_none(item.get("ordId") or item.get("orderId"))
            client_order_id = self._str_or_none(item.get("clOrdId") or item.get("clientOid"))
            order_status = self._str_or_none(item.get("status") or item.get("state") or item.get("orderStatus"))
            side = self._str_or_none(item.get("side"))
            order_type = self._str_or_none(item.get("ordType") or item.get("orderType"))
            position_side = self._str_or_none(item.get("posSide") or item.get("holdSide") or item.get("tradeSide"))
            if channel == "orders":
                events.append(
                    ExecutionStreamEvent(
                        exchange="bitget",
                        event_type="order",
                        event_time=self._int_or_none(payload.get("ts")),
                        transaction_time=self._int_or_none(item.get("uTime") or item.get("fillTime") or item.get("cTime")),
                        symbol=symbol,
                        order_id=order_id,
                        client_order_id=client_order_id,
                        order_status=order_status,
                        execution_type=self._str_or_none(item.get("execType")) or "ORDER_UPDATE",
                        side=side,
                        order_type=order_type,
                        position_side=position_side,
                        last_fill_qty=self._str_or_none(item.get("fillSz") or item.get("fillQty") or item.get("baseVolume")),
                        cumulative_fill_qty=self._str_or_none(item.get("accFillSz") or item.get("cumExecQty") or item.get("baseVolume")),
                        last_fill_price=self._str_or_none(item.get("fillPx") or item.get("fillPrice")),
                        average_price=self._str_or_none(item.get("avgPx") or item.get("priceAvg") or item.get("avgPrice")),
                        realized_pnl=self._str_or_none(item.get("fillPnl") or item.get("pnl") or item.get("totalProfits")),
                        raw=item,
                    )
                )
            else:
                events.append(
                    ExecutionStreamEvent(
                        exchange="bitget",
                        event_type="fill",
                        event_time=self._int_or_none(payload.get("ts")),
                        transaction_time=self._int_or_none(item.get("fillTime") or item.get("cTime") or item.get("uTime")),
                        symbol=symbol,
                        order_id=order_id,
                        client_order_id=client_order_id,
                        order_status=order_status or "filled",
                        execution_type=self._str_or_none(item.get("execType")) or "TRADE",
                        side=side,
                        order_type=order_type,
                        position_side=position_side,
                        last_fill_qty=self._str_or_none(item.get("fillSz") or item.get("fillQty") or item.get("baseVolume")),
                        cumulative_fill_qty=self._str_or_none(item.get("accFillSz") or item.get("cumExecQty") or item.get("baseVolume")),
                        last_fill_price=self._str_or_none(item.get("fillPx") or item.get("fillPrice") or item.get("price")),
                        average_price=self._str_or_none(item.get("avgPx") or item.get("priceAvg") or item.get("avgPrice") or item.get("price")),
                        realized_pnl=self._str_or_none(item.get("fillPnl") or item.get("profit") or item.get("pnl") or item.get("totalProfits")),
                        raw=item,
                    )
                )
        return events

    def _login_payload(self) -> dict[str, Any]:
        timestamp = str(int(time.time() * 1000))
        signature_payload = f"{timestamp}GET{self.VERIFY_PATH}"
        signature = base64.b64encode(
            hmac.new(
                self._credentials.api_secret.encode("utf-8"),
                signature_payload.encode("utf-8"),
                hashlib.sha256,
            ).digest()
        ).decode("utf-8")
        return {
            "op": "login",
            "args": [
                {
                    "apiKey": self._credentials.api_key,
                    "passphrase": self._credentials.api_passphrase,
                    "timestamp": timestamp,
                    "sign": signature,
                }
            ],
        }

    def _on_error(self, error: Any) -> None:
        self._logger.error("bitget private stream error: %s", error)
        self._last_error_text = str(error)
        if not self._connected and self._connect_error is None:
            self._connect_error = error if isinstance(error, Exception) else RuntimeError(str(error))
            self._opened_event.set()
            self._auth_event.set()

    def _on_close(self, code: Any, message: Any) -> None:
        self._connected = False
        self._authenticated = False
        self._last_disconnect_code = None if code is None else str(code)
        self._last_disconnect_message = None if message is None else str(message)
        self._opened_event.set()
        self._auth_event.set()
        self._logger.info("bitget private stream closed | code=%s | message=%s", code, message)

    def diagnostics(self) -> dict[str, Any]:
        with self._lock:
            return {
                "connected": self._connected,
                "authenticated": self._authenticated,
                "closing": self._closing,
                "reconnect_attempts_total": self._reconnect_attempts_total,
                "last_disconnect_code": self._last_disconnect_code,
                "last_disconnect_message": self._last_disconnect_message,
                "last_error_text": self._last_error_text,
                "last_ping_ts_ms": self._last_ping_ts_ms or None,
                "last_pong_ts_ms": self._last_pong_ts_ms or None,
                "ping_fail_count": self._ping_fail_count,
            }

    def _send(self, payload: dict[str, Any]) -> None:
        self._send_raw(json.dumps(payload))

    def _send_raw(self, payload: str) -> None:
        with self._lock:
            ws_app = self._ws_app
        if ws_app is None:
            raise RuntimeError("bitget private stream is not connected")
        try:
            ws_app.send(payload)
        except Exception as exc:
            self._logger.warning("bitget private stream send failed: %s", exc)
            raise

    @staticmethod
    def _error_message(payload: dict[str, Any]) -> str:
        code = payload.get("code")
        msg = payload.get("msg")
        if code is not None and msg:
            return f"[{code}] {msg}"
        return "bitget private stream request failed"

    @staticmethod
    def _str_or_none(value: Any) -> str | None:
        if value is None:
            return None
        text = str(value)
        return text if text else None

    @staticmethod
    def _int_or_none(value: Any) -> int | None:
        if value is None or value == "":
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
