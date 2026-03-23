from __future__ import annotations

import hashlib
import hmac
import json
import random
import threading
import time
from collections.abc import Callable
from typing import Any

from app.core.bybit.http_client import BybitV5HttpClient
from app.core.logging.logger_factory import get_logger
from app.core.models.account import ExchangeCredentials
from app.core.models.execution import ExecutionStreamEvent

try:
    import websocket
except ImportError:  # pragma: no cover
    websocket = None


class BybitPrivateExecutionStream:
    WS_URL = "wss://stream.bybit.com/v5/private"
    CLOSE_JOIN_TIMEOUT_SECONDS = 2.0

    def __init__(
        self,
        credentials: ExchangeCredentials,
        *,
        timeout_seconds: float = 10.0,
        ping_interval_seconds: float = 20.0,
    ) -> None:
        self._credentials = credentials
        self._timeout_seconds = float(timeout_seconds)
        self._ping_interval_seconds = float(ping_interval_seconds)
        self._logger = get_logger("execution.bybit_private_stream")
        self._client = BybitV5HttpClient(credentials)
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
            raise RuntimeError("websocket-client is required for BybitPrivateExecutionStream")
        with self._lock:
            if self._thread is not None and self._thread.is_alive() and self._connected and self._authenticated:
                self._logger.info("bybit private stream reuse existing connection")
                return
            if self._thread is not None and self._thread.is_alive():
                self._logger.info("bybit private stream waiting for active connection attempt")
            else:
                self._closing = False
                self._connected = False
                self._authenticated = False
                self._connect_error = None
                self._opened_event.clear()
                self._auth_event.clear()
        self._client.sync_time_offset()
        with self._lock:
            if self._thread is None or not self._thread.is_alive():
                self._thread = threading.Thread(target=self._run_forever, name="bybit-private-stream", daemon=True)
                self._thread.start()
                self._logger.info("bybit private stream starting new connection")
            if self._ping_thread is None or not self._ping_thread.is_alive():
                self._ping_thread = threading.Thread(target=self._ping_loop, name="bybit-private-ping", daemon=True)
                self._ping_thread.start()

        if not self._opened_event.wait(timeout=self._timeout_seconds):
            raise RuntimeError("bybit private stream connect timeout")
        if not self._auth_event.wait(timeout=self._timeout_seconds):
            raise RuntimeError("bybit private stream auth timeout")
        if self._connect_error is not None:
            raise RuntimeError(str(self._connect_error))
        if not self._connected or not self._authenticated:
            raise RuntimeError("bybit private stream failed to connect")

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
                self._logger.error("bybit private stream loop crashed: %s", exc)
            finally:
                with self._lock:
                    self._ws_app = None
            if self._closing:
                break
            delay = min(backoff_seconds, 15.0)
            jitter = random.uniform(0.0, delay * 0.3)
            self._reconnect_attempts_total += 1
            self._logger.warning("bybit private stream reconnect scheduled | delay_seconds=%.2f", delay + jitter)
            time.sleep(delay + jitter)
            if self._session_ready:
                backoff_seconds = 1.0
            else:
                backoff_seconds = min(backoff_seconds * 2.0, 15.0)

    def _ping_loop(self) -> None:
        while not self._closing:
            time.sleep(self._ping_interval_seconds)
            if self._closing:
                break
            try:
                self._last_ping_ts_ms = int(time.time() * 1000)
                self._send({"op": "ping"})
            except Exception:
                self._ping_fail_count += 1

    def _on_open(self) -> None:
        self._connected = True
        self._opened_event.set()
        self._send(self._auth_payload())
        self._logger.info("bybit private stream connected")

    def _on_message(self, message: str) -> None:
        payload = json.loads(message)
        if not isinstance(payload, dict):
            return
        op = str(payload.get("op", "")).strip().lower()
        ret_msg = str(payload.get("ret_msg", "")).strip().lower()
        if op == "pong" or (op == "ping" and ret_msg == "pong"):
            self._last_pong_ts_ms = int(time.time() * 1000)
            return
        if op == "auth":
            success = self._is_auth_success(payload)
            if success:
                self._authenticated = True
                self._session_ready = True
                self._auth_event.set()
                self._connect_error = None
                self._subscribe_topics()
                self._logger.info("bybit private stream authenticated")
            else:
                error_text = str(payload.get("retMsg") or payload.get("ret_msg") or "auth failed")
                self._connect_error = RuntimeError(error_text)
                self._auth_event.set()
            return
        topic = str(payload.get("topic", "")).strip().lower()
        if topic in {"order", "execution"}:
            for event in self._normalize_events(payload):
                self._logger.debug(
                    "bybit private event received | topic=%s | symbol=%s | order_id=%s | status=%s | exec_type=%s",
                    event.event_type,
                    event.symbol,
                    event.order_id,
                    event.order_status,
                    event.execution_type,
                )
                for callback in list(self._callbacks):
                    callback(event)

    def _subscribe_topics(self) -> None:
        self._send({"op": "subscribe", "args": ["order", "execution"]})

    @staticmethod
    def _is_auth_success(payload: dict[str, Any]) -> bool:
        if bool(payload.get("success", False)):
            return True
        try:
            ret_code = int(payload.get("retCode", 0) or 0)
        except (TypeError, ValueError):
            ret_code = -1
        if ret_code == 0:
            return True
        return False

    def _normalize_events(self, payload: dict[str, Any]) -> list[ExecutionStreamEvent]:
        topic = str(payload.get("topic", "")).strip().lower()
        creation_time = self._int_or_none(payload.get("creationTime"))
        items = payload.get("data", [])
        if not isinstance(items, list):
            return []
        events: list[ExecutionStreamEvent] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            if str(item.get("category", "")).strip().lower() != "linear":
                continue
            if topic == "order":
                events.append(
                    ExecutionStreamEvent(
                        exchange="bybit",
                        event_type="order",
                        event_time=creation_time,
                        transaction_time=self._int_or_none(item.get("updatedTime")) or creation_time,
                        symbol=self._str_or_none(item.get("symbol")),
                        order_id=self._str_or_none(item.get("orderId")),
                        client_order_id=self._str_or_none(item.get("orderLinkId")),
                        order_status=self._str_or_none(item.get("orderStatus")),
                        execution_type="ORDER_UPDATE",
                        side=self._str_or_none(item.get("side")),
                        order_type=self._str_or_none(item.get("orderType")),
                        position_side=self._str_or_none(item.get("positionIdx")),
                        last_fill_qty=None,
                        cumulative_fill_qty=self._str_or_none(item.get("cumExecQty")),
                        last_fill_price=None,
                        average_price=self._str_or_none(item.get("avgPrice")),
                        realized_pnl=self._str_or_none(item.get("closedPnl")),
                        raw=item,
                    )
                )
                continue
            exec_qty = self._str_or_none(item.get("execQty"))
            leaves_qty = self._str_or_none(item.get("leavesQty"))
            order_status = None
            if leaves_qty is not None:
                try:
                    order_status = "FILLED" if float(leaves_qty) == 0.0 else "PARTIALLY_FILLED"
                except ValueError:
                    order_status = None
            events.append(
                ExecutionStreamEvent(
                    exchange="bybit",
                    event_type="execution",
                    event_time=creation_time,
                    transaction_time=self._int_or_none(item.get("execTime")) or creation_time,
                    symbol=self._str_or_none(item.get("symbol")),
                    order_id=self._str_or_none(item.get("orderId")),
                    client_order_id=self._str_or_none(item.get("orderLinkId")),
                    order_status=order_status,
                    execution_type=self._str_or_none(item.get("execType")) or "TRADE",
                    side=self._str_or_none(item.get("side")),
                    order_type=self._str_or_none(item.get("orderType")),
                    position_side=self._str_or_none(item.get("positionIdx")),
                    last_fill_qty=exec_qty,
                    cumulative_fill_qty=self._str_or_none(item.get("execQty")),
                    last_fill_price=self._str_or_none(item.get("execPrice")),
                    average_price=self._str_or_none(item.get("execPrice")),
                    realized_pnl=self._str_or_none(item.get("execPnl")),
                    raw=item,
                )
            )
        return events

    def _auth_payload(self) -> dict[str, Any]:
        expires = str(self._client.current_timestamp_ms() + 10_000)
        signature = hmac.new(
            self._credentials.api_secret.encode("utf-8"),
            f"GET/realtime{expires}".encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return {"op": "auth", "args": [self._credentials.api_key, expires, signature]}

    def _on_error(self, error: Any) -> None:
        self._last_error_text = str(error)
        error_text = str(error or "").strip()
        self._connected = False
        self._authenticated = False
        if self._closing:
            self._logger.info("bybit private stream closing | error=%s", error_text)
        elif error_text in {"Connection to remote host was lost.", "socket is already closed.", "'NoneType' object has no attribute 'sock'"}:
            self._logger.warning("bybit private stream disconnected: %s", error_text)
        else:
            self._logger.error("bybit private stream error: %s", error)
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
        self._logger.info("bybit private stream closed | code=%s | message=%s", code, message)

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
        with self._lock:
            ws_app = self._ws_app
        if ws_app is None:
            raise RuntimeError("bybit private stream is not connected")
        try:
            ws_app.send(json.dumps(payload))
        except Exception as exc:
            self._logger.warning("bybit private stream send failed: %s", exc)
            with self._lock:
                self._connected = False
                self._authenticated = False
            try:
                ws_app.close()
            except Exception:
                pass
            raise

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
