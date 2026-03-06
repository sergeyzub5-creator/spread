from __future__ import annotations

import hashlib
import hmac
import json
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

    def connect(self) -> None:
        if websocket is None:
            raise RuntimeError("websocket-client is required for BybitPrivateExecutionStream")
        with self._lock:
            if self._thread is not None and self._thread.is_alive() and self._connected and self._authenticated:
                self._logger.info("bybit private stream reuse existing connection")
                return
            self._closing = False
            self._connected = False
            self._authenticated = False
            self._connect_error = None
            self._opened_event.clear()
            self._auth_event.clear()
        self._client.sync_time_offset()
        with self._lock:
            self._thread = threading.Thread(target=self._run_forever, name="bybit-private-stream", daemon=True)
            self._thread.start()
            if self._ping_thread is None or not self._ping_thread.is_alive():
                self._ping_thread = threading.Thread(target=self._ping_loop, name="bybit-private-ping", daemon=True)
                self._ping_thread.start()
            self._logger.info("bybit private stream starting new connection")

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
        if ws_app is not None:
            ws_app.close()

    def _run_forever(self) -> None:
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

    def _ping_loop(self) -> None:
        while not self._closing:
            time.sleep(self._ping_interval_seconds)
            if self._closing:
                break
            try:
                self._send({"op": "ping"})
            except Exception:
                pass

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
        if op == "auth":
            success = bool(payload.get("success", False))
            if success:
                self._authenticated = True
                self._auth_event.set()
                self._subscribe_topics()
                self._logger.info("bybit private stream authenticated")
            else:
                self._connect_error = RuntimeError(str(payload.get("retMsg", "auth failed")))
                self._auth_event.set()
            return
        topic = str(payload.get("topic", "")).strip().lower()
        if topic in {"order", "execution"}:
            for event in self._normalize_events(payload):
                self._logger.info(
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
        self._logger.error("bybit private stream error: %s", error)
        if not self._connected and self._connect_error is None:
            self._connect_error = error if isinstance(error, Exception) else RuntimeError(str(error))
            self._opened_event.set()
            self._auth_event.set()

    def _on_close(self, code: Any, message: Any) -> None:
        self._connected = False
        self._authenticated = False
        self._opened_event.set()
        self._auth_event.set()
        self._logger.info("bybit private stream closed | code=%s | message=%s", code, message)

    def _send(self, payload: dict[str, Any]) -> None:
        with self._lock:
            ws_app = self._ws_app
        if ws_app is None:
            raise RuntimeError("bybit private stream is not connected")
        ws_app.send(json.dumps(payload))

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

