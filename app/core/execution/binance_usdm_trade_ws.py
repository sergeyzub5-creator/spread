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
from decimal import Decimal
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from app.core.logging.logger_factory import get_logger
from app.core.models.account import ExchangeCredentials

try:
    import websocket
except ImportError:  # pragma: no cover
    websocket = None


class ExecutionTransportError(RuntimeError):
    pass


@dataclass(slots=True)
class _PendingRequest:
    event: threading.Event
    response: dict[str, Any] | None = None
    error: Exception | None = None


class BinanceUsdmTradeWebSocketTransport:
    WS_URL = "wss://ws-fapi.binance.com/ws-fapi/v1"
    TIME_URL = "https://fapi.binance.com/fapi/v1/time"
    CLOSE_JOIN_TIMEOUT_SECONDS = 2.0
    RECONNECT_BACKOFF_MAX_SECONDS = 60.0

    def __init__(
        self,
        credentials: ExchangeCredentials,
        *,
        connect_timeout_seconds: float = 10.0,
        request_timeout_seconds: float = 10.0,
        recv_window_ms: int = 5000,
        time_sync_ttl_ms: int = 30000,
    ) -> None:
        self._credentials = credentials
        self._connect_timeout_seconds = float(connect_timeout_seconds)
        self._request_timeout_seconds = float(request_timeout_seconds)
        self._recv_window_ms = int(recv_window_ms)
        self._logger = get_logger("execution.binance_usdm_ws")
        self._callbacks: list[Callable[[dict[str, Any]], None]] = []
        self._lock = threading.RLock()
        self._pending: dict[str, _PendingRequest] = {}
        self._ws_app = None
        self._thread: threading.Thread | None = None
        self._connected = False
        self._closing = False
        self._reconnect_attempts_total = 0
        self._session_ready = False
        self._opened_event = threading.Event()
        self._connect_error: Exception | None = None
        self._time_offset_ms = 0
        self._last_time_sync_at_ms = 0
        self._time_sync_ttl_ms = int(time_sync_ttl_ms)

    def connect(self) -> bool:
        if websocket is None:
            raise ExecutionTransportError("websocket-client is required for BinanceUsdmTradeWebSocketTransport")

        with self._lock:
            if self._thread is not None and self._thread.is_alive() and self._connected:
                self._logger.info("binance trade ws reuse existing connection")
                return True
            self._closing = False
            self._connected = False
            self._connect_error = None
            self._opened_event.clear()
        self._sync_time_offset()
        with self._lock:
            self._thread = threading.Thread(target=self._run_forever, name="binance-usdm-trade-ws", daemon=True)
            self._thread.start()
            self._logger.info("binance trade ws starting new connection thread")

        if not self._opened_event.wait(timeout=self._connect_timeout_seconds):
            raise ExecutionTransportError("binance trade ws connect timeout")
        if self._connect_error is not None:
            raise ExecutionTransportError(str(self._connect_error))
        if not self._connected:
            raise ExecutionTransportError("binance trade ws failed to connect")
        return False

    def close(self) -> None:
        with self._lock:
            self._closing = True
            ws_app = self._ws_app
            thread = self._thread
        if ws_app is not None:
            try:
                ws_app.close()
            except Exception as exc:
                self._logger.warning("binance trade ws close request failed: %s", exc)
        if (
            thread is not None
            and thread.is_alive()
            and thread is not threading.current_thread()
        ):
            thread.join(timeout=self.CLOSE_JOIN_TIMEOUT_SECONDS)
            if thread.is_alive():
                self._logger.warning(
                    "binance trade ws thread did not stop within timeout | timeout_seconds=%s",
                    self.CLOSE_JOIN_TIMEOUT_SECONDS,
                )
        with self._lock:
            self._connected = False
            self._ws_app = None
            self._thread = None
            self._opened_event.set()
        self._fail_all_pending(ExecutionTransportError("binance trade ws closed"))

    def on_event(self, callback: Callable[[dict[str, Any]], None]) -> None:
        self._callbacks.append(callback)

    def request(
        self,
        method: str,
        params: dict[str, Any],
        on_request_sent: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        connection_reused = self.connect()

        try:
            return self._request_once(method, params, connection_reused=connection_reused, on_request_sent=on_request_sent)
        except ExecutionTransportError as exc:
            if "[-1021]" not in str(exc):
                raise
            self._logger.warning("binance trade ws timestamp drift detected, resyncing time")
            self._sync_time_offset(force=True)
            return self._request_once(method, params, connection_reused=connection_reused, on_request_sent=on_request_sent)

    def _request_once(
        self,
        method: str,
        params: dict[str, Any],
        *,
        connection_reused: bool,
        on_request_sent: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        request_id = str(uuid.uuid4())
        signed_params = self._sign_params(params)
        payload = {
            "id": request_id,
            "method": method,
            "params": signed_params,
        }
        sent_at_ms = 0
        pending = _PendingRequest(event=threading.Event())
        with self._lock:
            self._pending[request_id] = pending

        try:
            self._send(payload)
            sent_at_ms = int(time.time() * 1000)
            request_meta = {
                "request_id": request_id,
                "method": method,
                "sent_at_ms": sent_at_ms,
                "connection_reused": connection_reused,
                "time_offset_ms": self._time_offset_ms,
                "time_synced_at_ms": self._last_time_sync_at_ms,
            }
            if on_request_sent is not None:
                on_request_sent(dict(request_meta))
            self._logger.info(
                "binance trade ws request sent | id=%s | method=%s | sent_at_ms=%s | connected=%s | reused=%s | params=%s",
                request_id,
                method,
                sent_at_ms,
                self._connected,
                connection_reused,
                self._sanitize_params(signed_params),
            )
        except Exception as exc:
            with self._lock:
                self._pending.pop(request_id, None)
            raise ExecutionTransportError(str(exc)) from exc

        if not pending.event.wait(timeout=self._request_timeout_seconds):
            with self._lock:
                self._pending.pop(request_id, None)
            raise ExecutionTransportError(f"binance trade ws request timeout: {method}")

        if pending.error is not None:
            raise ExecutionTransportError(str(pending.error)) from pending.error
        if pending.response is None:
            raise ExecutionTransportError(f"binance trade ws empty response: {method}")
        response_at_ms = int(time.time() * 1000)
        response_meta = {
            "request_id": request_id,
            "method": method,
            "sent_at_ms": sent_at_ms,
            "response_at_ms": response_at_ms,
            "latency_ms": max(0, response_at_ms - sent_at_ms),
            "connection_reused": connection_reused,
            "time_offset_ms": self._time_offset_ms,
            "time_synced_at_ms": self._last_time_sync_at_ms,
        }
        pending.response["_transport_meta"] = response_meta
        response_summary = self._summarize_response(pending.response)
        self._logger.info(
            "binance trade ws response received | id=%s | method=%s | response_at_ms=%s | latency_ms=%s | reused=%s | summary=%s",
            request_id,
            method,
            response_at_ms,
            max(0, response_at_ms - sent_at_ms),
            connection_reused,
            response_summary,
        )
        return pending.response

    def _run_forever(self) -> None:
        backoff_seconds = 1.0
        while not self._closing:
            with self._lock:
                self._session_ready = False
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
                if self._connect_error is None:
                    self._connect_error = exc
                    self._opened_event.set()
                self._logger.error("binance trade ws loop crashed: %s", exc)
            finally:
                with self._lock:
                    self._ws_app = None
            if self._closing:
                break
            with self._lock:
                if not self._connected and self._connect_error is None:
                    self._connect_error = ExecutionTransportError("binance trade ws run_forever exited without connect")
                    self._opened_event.set()
            delay = min(backoff_seconds, self.RECONNECT_BACKOFF_MAX_SECONDS)
            jitter = random.uniform(0.0, delay * 0.3)
            self._reconnect_attempts_total += 1
            self._logger.warning(
                "binance trade ws reconnect scheduled | delay_seconds=%.2f | attempt=%s",
                delay + jitter,
                self._reconnect_attempts_total,
            )
            time.sleep(delay + jitter)
            if self._session_ready:
                backoff_seconds = 1.0
            else:
                backoff_seconds = min(backoff_seconds * 2.0, self.RECONNECT_BACKOFF_MAX_SECONDS)

    def _on_open(self) -> None:
        self._connected = True
        self._session_ready = True
        self._connect_error = None
        self._opened_event.set()
        self._logger.info("binance trade ws connected")

    def _on_message(self, message: str) -> None:
        data = json.loads(message)
        if not isinstance(data, dict):
            return

        response_id = str(data.get("id", "")).strip()
        if response_id:
            with self._lock:
                pending = self._pending.pop(response_id, None)
            if pending is not None:
                status = int(data.get("status", 0) or 0)
                error_payload = data.get("error")
                if error_payload is not None or status >= 400:
                    pending.error = ExecutionTransportError(self._error_message(data))
                else:
                    pending.response = data
                pending.event.set()
                return

        for callback in list(self._callbacks):
            callback(data)

    def _on_error(self, error: Any) -> None:
        self._logger.error("binance trade ws error: %s", error)
        if not self._connected and self._connect_error is None:
            self._connect_error = error if isinstance(error, Exception) else ExecutionTransportError(str(error))
            self._opened_event.set()
        self._fail_all_pending(error if isinstance(error, Exception) else ExecutionTransportError(str(error)))

    def _on_close(self, status_code: Any, message: Any) -> None:
        with self._lock:
            self._connected = False
            self._ws_app = None
        self._opened_event.set()
        self._logger.info("binance trade ws closed | code=%s | message=%s", status_code, message)
        if not self._closing:
            self._fail_all_pending(ExecutionTransportError("binance trade ws disconnected"))

    def _send(self, payload: dict[str, Any]) -> None:
        with self._lock:
            ws_app = self._ws_app
            connected = self._connected
        if ws_app is None or not connected:
            raise ExecutionTransportError("binance trade ws is not connected")
        try:
            ws_app.send(json.dumps(payload))
        except Exception as exc:
            # Socket can be dropped asynchronously by network/peer (e.g. SSLEOFError during send).
            # Mark connection as down and fail request through transport error for controlled reconnect.
            with self._lock:
                self._connected = False
            try:
                ws_app.close()
            except Exception:
                pass
            raise ExecutionTransportError(f"binance trade ws send failed: {exc}") from exc

    def diagnostics(self) -> dict[str, Any]:
        with self._lock:
            return {
                "connected": self._connected,
                "closing": self._closing,
                "pending_requests": len(self._pending),
                "time_offset_ms": self._time_offset_ms,
                "reconnect_attempts_total": self._reconnect_attempts_total,
            }

    def _sign_params(self, params: dict[str, Any]) -> dict[str, Any]:
        normalized = {key: self._normalize_value(value) for key, value in params.items() if value is not None}
        normalized["apiKey"] = self._credentials.api_key
        normalized["timestamp"] = int(time.time() * 1000) + self._time_offset_ms
        normalized["recvWindow"] = self._recv_window_ms

        signing_payload = urlencode(
            [(key, normalized[key]) for key in sorted(normalized)],
        )
        signature = hmac.new(
            self._credentials.api_secret.encode("utf-8"),
            signing_payload.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        normalized["signature"] = signature
        return normalized

    def _fail_all_pending(self, error: Exception) -> None:
        with self._lock:
            pending_items = list(self._pending.values())
            self._pending.clear()
        for pending in pending_items:
            pending.error = error
            pending.event.set()

    @staticmethod
    def _normalize_value(value: Any) -> Any:
        if isinstance(value, Decimal):
            text = format(value, "f")
            return text.rstrip("0").rstrip(".") if "." in text else text
        if isinstance(value, bool):
            return "true" if value else "false"
        return value

    @staticmethod
    def _error_message(payload: dict[str, Any]) -> str:
        error = payload.get("error")
        if isinstance(error, dict):
            code = error.get("code")
            msg = error.get("msg")
            if code is not None and msg:
                return f"[{code}] {msg}"
        status = payload.get("status")
        if status is not None:
            return f"websocket status {status}"
        return "binance trade ws request failed"

    def _sync_time_offset(self, *, force: bool = False) -> None:
        now_ms = int(time.time() * 1000)
        if not force and self._last_time_sync_at_ms > 0:
            if now_ms - self._last_time_sync_at_ms <= self._time_sync_ttl_ms:
                return
        request = Request(
            self.TIME_URL,
            headers={"User-Agent": "spread-sniper-ui-shell/1.0"},
            method="GET",
        )
        try:
            with urlopen(request, timeout=self._connect_timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except Exception as exc:
            raise ExecutionTransportError(f"failed to sync Binance server time: {exc}") from exc

        server_time = int(payload.get("serverTime", 0) or 0)
        if server_time <= 0:
            raise ExecutionTransportError("failed to sync Binance server time: invalid serverTime")
        local_time = int(time.time() * 1000)
        self._time_offset_ms = server_time - local_time
        self._last_time_sync_at_ms = local_time
        self._logger.info("binance trade ws time synced | offset_ms=%s | force=%s", self._time_offset_ms, force)

    @staticmethod
    def _sanitize_params(params: dict[str, Any]) -> dict[str, Any]:
        sanitized = dict(params)
        if "apiKey" in sanitized:
            api_key = str(sanitized["apiKey"])
            sanitized["apiKey"] = f"{api_key[:4]}***" if api_key else "***"
        if "signature" in sanitized:
            sanitized["signature"] = "***"
        return sanitized

    @staticmethod
    def _summarize_response(payload: dict[str, Any]) -> dict[str, Any]:
        result = payload.get("result")
        if not isinstance(result, dict):
            return {"status": payload.get("status")}
        return {
            "status": result.get("status"),
            "symbol": result.get("symbol"),
            "orderId": result.get("orderId"),
            "clientOrderId": result.get("clientOrderId"),
            "executedQty": result.get("executedQty"),
            "avgPrice": result.get("avgPrice"),
        }
