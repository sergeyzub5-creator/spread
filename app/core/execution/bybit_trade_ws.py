from __future__ import annotations

import hashlib
import hmac
import json
import random
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
    CLOSE_JOIN_TIMEOUT_SECONDS = 2.0

    def __init__(
        self,
        credentials: ExchangeCredentials,
        *,
        connect_timeout_seconds: float = 10.0,
        request_timeout_seconds: float = 10.0,
        recv_window_ms: int = 5000,
        ping_interval_seconds: float = 20.0,
    ) -> None:
        self._credentials = credentials
        self._connect_timeout_seconds = float(connect_timeout_seconds)
        self._request_timeout_seconds = float(request_timeout_seconds)
        self._recv_window_ms = int(recv_window_ms)
        self._ping_interval_seconds = float(ping_interval_seconds)
        self._logger = get_logger("execution.bybit_linear_ws")
        self._client = BybitV5HttpClient(credentials, recv_window_ms=recv_window_ms)
        self._callbacks: list[Callable[[dict[str, Any]], None]] = []
        self._lock = threading.RLock()
        self._pending: dict[str, _PendingRequest] = {}
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

    def connect(self) -> bool:
        if websocket is None:
            raise BybitTradeWebSocketError("websocket-client is required for BybitLinearTradeWebSocketTransport")
        with self._lock:
            if self._thread is not None and self._thread.is_alive() and self._connected and self._authenticated:
                self._logger.info("bybit trade ws reuse existing connection")
                return True
            if self._thread is not None and self._thread.is_alive():
                self._logger.info("bybit trade ws waiting for active connection attempt")
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
                self._thread = threading.Thread(target=self._run_forever, name="bybit-linear-trade-ws", daemon=True)
                self._thread.start()
                self._logger.info("bybit trade ws starting new connection thread")
            if self._ping_thread is None or not self._ping_thread.is_alive():
                self._ping_thread = threading.Thread(target=self._ping_loop, name="bybit-linear-trade-ping", daemon=True)
                self._ping_thread.start()

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
                self._logger.error("bybit trade ws loop crashed: %s", exc)
            finally:
                with self._lock:
                    self._ws_app = None
            if self._closing:
                break
            delay = min(backoff_seconds, 15.0)
            jitter = random.uniform(0.0, delay * 0.3)
            self._reconnect_attempts_total += 1
            self._logger.warning("bybit trade ws reconnect scheduled | delay_seconds=%.2f", delay + jitter)
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
        self._logger.info("bybit trade ws connected")

    def _on_message(self, message: str) -> None:
        payload = json.loads(message)
        if not isinstance(payload, dict):
            return

        op = str(payload.get("op", "")).strip().lower()
        ret_msg = str(payload.get("retMsg") or payload.get("ret_msg") or "").strip().lower()
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
                self._logger.info("bybit trade ws authenticated")
            else:
                error_text = str(payload.get("retMsg") or payload.get("ret_msg") or "auth failed")
                self._connect_error = BybitTradeWebSocketError(error_text)
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
        self._last_error_text = str(error)
        error_text = str(error or "").strip()
        self._connected = False
        self._authenticated = False
        if self._closing:
            self._logger.info("bybit trade ws closing | error=%s", error_text)
        elif error_text in {"Connection to remote host was lost.", "socket is already closed.", "'NoneType' object has no attribute 'sock'"}:
            self._logger.warning("bybit trade ws disconnected: %s", error_text)
        else:
            self._logger.error("bybit trade ws error: %s", error)
        if not self._connected and self._connect_error is None:
            self._connect_error = error if isinstance(error, Exception) else BybitTradeWebSocketError(str(error))
            self._opened_event.set()
            self._auth_event.set()
        self._fail_all_pending(error if isinstance(error, Exception) else BybitTradeWebSocketError(str(error)))

    def _on_close(self, code: Any, message: Any) -> None:
        self._connected = False
        self._authenticated = False
        self._last_disconnect_code = str(code) if code is not None else None
        self._last_disconnect_message = str(message) if message is not None else None
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
        try:
            ws_app.send(json.dumps(payload))
        except Exception as exc:
            with self._lock:
                self._connected = False
                self._authenticated = False
            try:
                ws_app.close()
            except Exception:
                pass
            raise BybitTradeWebSocketError(f"bybit trade ws send failed: {exc}") from exc

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

    def diagnostics(self) -> dict[str, Any]:
        with self._lock:
            return {
                "connected": self._connected,
                "authenticated": self._authenticated,
                "closing": self._closing,
                "pending_requests": len(self._pending),
                "reconnect_attempts_total": self._reconnect_attempts_total,
                "last_disconnect_code": self._last_disconnect_code,
                "last_disconnect_message": self._last_disconnect_message,
                "last_error_text": self._last_error_text,
                "last_ping_ts_ms": self._last_ping_ts_ms or None,
                "last_pong_ts_ms": self._last_pong_ts_ms or None,
                "ping_fail_count": self._ping_fail_count,
            }

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
