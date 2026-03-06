from __future__ import annotations

import hashlib
import hmac
import json
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from app.core.bybit.http_client import BybitApiError, BybitV5HttpClient
from app.core.logging.logger_factory import get_logger
from app.core.models.account import ExchangeCredentials

try:
    import websocket
except ImportError:  # pragma: no cover
    websocket = None


class BybitTradeWebSocketError(RuntimeError):
    pass


@dataclass(slots=True)
class _PendingRequest:
    event: threading.Event
    response: dict[str, Any] | None = None
    error: Exception | None = None


class BybitLinearTradeWebSocketTransport:
    WS_URL = "wss://stream.bybit.com/v5/trade"

    def __init__(
        self,
        credentials: ExchangeCredentials,
        *,
        connect_timeout_seconds: float = 10.0,
        request_timeout_seconds: float = 10.0,
        recv_window_ms: int = 5000,
    ) -> None:
        self._credentials = credentials
        self._connect_timeout_seconds = float(connect_timeout_seconds)
        self._request_timeout_seconds = float(request_timeout_seconds)
        self._recv_window_ms = int(recv_window_ms)
        self._logger = get_logger("execution.bybit_linear_ws")
        self._client = BybitV5HttpClient(credentials, recv_window_ms=recv_window_ms)
        self._callbacks: list[Callable[[dict[str, Any]], None]] = []
        self._lock = threading.RLock()
        self._pending: dict[str, _PendingRequest] = {}
        self._ws_app = None
        self._thread: threading.Thread | None = None
        self._connected = False
        self._authenticated = False
        self._closing = False
        self._opened_event = threading.Event()
        self._auth_event = threading.Event()
        self._connect_error: Exception | None = None

    def connect(self) -> bool:
        if websocket is None:
            raise BybitTradeWebSocketError("websocket-client is required for BybitLinearTradeWebSocketTransport")
        with self._lock:
            if self._thread is not None and self._thread.is_alive() and self._connected and self._authenticated:
                self._logger.info("bybit trade ws reuse existing connection")
                return True
            self._closing = False
            self._connected = False
            self._authenticated = False
            self._connect_error = None
            self._opened_event.clear()
            self._auth_event.clear()
        self._client.sync_time_offset()
        with self._lock:
            self._thread = threading.Thread(target=self._run_forever, name="bybit-linear-trade-ws", daemon=True)
            self._thread.start()
            self._logger.info("bybit trade ws starting new connection thread")

        if not self._opened_event.wait(timeout=self._connect_timeout_seconds):
            raise BybitTradeWebSocketError("bybit trade ws connect timeout")
        if not self._auth_event.wait(timeout=self._connect_timeout_seconds):
            raise BybitTradeWebSocketError("bybit trade ws auth timeout")
        if self._connect_error is not None:
            raise BybitTradeWebSocketError(str(self._connect_error))
        if not self._connected or not self._authenticated:
            raise BybitTradeWebSocketError("bybit trade ws failed to connect")
        return False

    def close(self) -> None:
        with self._lock:
            self._closing = True
            ws_app = self._ws_app
        if ws_app is not None:
            ws_app.close()
        self._fail_all_pending(BybitTradeWebSocketError("bybit trade ws closed"))

    def on_event(self, callback: Callable[[dict[str, Any]], None]) -> None:
        self._callbacks.append(callback)

    def request(
        self,
        op: str,
        payload: dict[str, Any],
        on_request_sent: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        connection_reused = self.connect()
        return self._request_once(op, payload, connection_reused=connection_reused, on_request_sent=on_request_sent)

    def _request_once(
        self,
        op: str,
        payload: dict[str, Any],
        *,
        connection_reused: bool,
        on_request_sent: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        request_id = str(uuid.uuid4())
        request_payload = {
            "reqId": request_id,
            "header": {
                "X-BAPI-TIMESTAMP": str(self._client.current_timestamp_ms()),
                "X-BAPI-RECV-WINDOW": str(self._recv_window_ms),
            },
            "op": op,
            "args": [payload],
        }
        pending = _PendingRequest(event=threading.Event())
        with self._lock:
            self._pending[request_id] = pending

        try:
            self._send(request_payload)
            sent_at_ms = int(time.time() * 1000)
            meta = {
                "request_id": request_id,
                "op": op,
                "sent_at_ms": sent_at_ms,
                "connection_reused": connection_reused,
                "time_offset_ms": self._client.time_offset_ms(),
            }
            if on_request_sent is not None:
                on_request_sent(dict(meta))
            self._logger.info(
                "bybit trade ws request sent | req_id=%s | op=%s | sent_at_ms=%s | reused=%s | args=%s",
                request_id,
                op,
                sent_at_ms,
                connection_reused,
                payload,
            )
        except Exception as exc:
            with self._lock:
                self._pending.pop(request_id, None)
            raise BybitTradeWebSocketError(str(exc)) from exc

        if not pending.event.wait(timeout=self._request_timeout_seconds):
            with self._lock:
                self._pending.pop(request_id, None)
            raise BybitTradeWebSocketError(f"bybit trade ws request timeout: {op}")
        if pending.error is not None:
            raise BybitTradeWebSocketError(str(pending.error)) from pending.error
        if pending.response is None:
            raise BybitTradeWebSocketError(f"bybit trade ws empty response: {op}")

        response_at_ms = int(time.time() * 1000)
        transport_meta = {
            "request_id": request_id,
            "op": op,
            "sent_at_ms": sent_at_ms,
            "response_at_ms": response_at_ms,
            "latency_ms": max(0, response_at_ms - sent_at_ms),
            "connection_reused": connection_reused,
            "time_offset_ms": self._client.time_offset_ms(),
        }
        pending.response["_transport_meta"] = transport_meta
        self._logger.info(
            "bybit trade ws response received | req_id=%s | op=%s | response_at_ms=%s | latency_ms=%s | reused=%s | summary=%s",
            request_id,
            op,
            response_at_ms,
            transport_meta["latency_ms"],
            connection_reused,
            self._summarize_response(pending.response),
        )
        return pending.response

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
            self._logger.error("bybit trade ws loop crashed: %s", exc)

    def _on_open(self) -> None:
        self._connected = True
        self._opened_event.set()
        self._send(self._auth_payload())
        self._logger.info("bybit trade ws connected")

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
                self._connect_error = None
                self._logger.info("bybit trade ws authenticated")
            else:
                self._connect_error = BybitTradeWebSocketError(str(payload.get("retMsg", "auth failed")))
                self._auth_event.set()
            return

        request_id = str(payload.get("reqId", "")).strip()
        if request_id:
            with self._lock:
                pending = self._pending.pop(request_id, None)
            if pending is not None:
                ret_code = int(payload.get("retCode", 0) or 0)
                if ret_code != 0:
                    pending.error = BybitTradeWebSocketError(
                        f"[{ret_code}] {payload.get('retMsg', 'Bybit trade ws request failed')}"
                    )
                else:
                    pending.response = payload
                pending.event.set()
                return

        for callback in list(self._callbacks):
            callback(payload)

    def _on_error(self, error: Any) -> None:
        self._logger.error("bybit trade ws error: %s", error)
        if not self._connected and self._connect_error is None:
            self._connect_error = error if isinstance(error, Exception) else BybitTradeWebSocketError(str(error))
            self._opened_event.set()
            self._auth_event.set()
        self._fail_all_pending(error if isinstance(error, Exception) else BybitTradeWebSocketError(str(error)))

    def _on_close(self, code: Any, message: Any) -> None:
        self._connected = False
        self._authenticated = False
        self._opened_event.set()
        self._auth_event.set()
        self._logger.info("bybit trade ws closed | code=%s | message=%s", code, message)
        if not self._closing:
            self._fail_all_pending(BybitTradeWebSocketError("bybit trade ws disconnected"))

    def _auth_payload(self) -> dict[str, Any]:
        expires = str(self._client.current_timestamp_ms() + 10_000)
        signature = hmac.new(
            self._credentials.api_secret.encode("utf-8"),
            f"GET/realtime{expires}".encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return {"op": "auth", "args": [self._credentials.api_key, expires, signature]}

    def _send(self, payload: dict[str, Any]) -> None:
        with self._lock:
            ws_app = self._ws_app
        if ws_app is None:
            raise BybitTradeWebSocketError("bybit trade ws is not connected")
        ws_app.send(json.dumps(payload))

    def _fail_all_pending(self, error: Exception) -> None:
        with self._lock:
            pending_items = list(self._pending.values())
            self._pending.clear()
        for pending in pending_items:
            pending.error = error
            pending.event.set()

    @staticmethod
    def _summarize_response(payload: dict[str, Any]) -> dict[str, Any]:
        data = payload.get("data", {}) if isinstance(payload.get("data"), dict) else {}
        return {
            "retCode": payload.get("retCode"),
            "retMsg": payload.get("retMsg"),
            "orderId": data.get("orderId"),
            "orderLinkId": data.get("orderLinkId"),
        }

