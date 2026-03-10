from __future__ import annotations

import base64
import hashlib
import hmac
import json
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from app.core.logging.logger_factory import get_logger
from app.core.models.account import ExchangeCredentials

try:
    import websocket
except ImportError:  # pragma: no cover
    websocket = None


class BitgetTradeWebSocketError(RuntimeError):
    pass


@dataclass(slots=True)
class _PendingRequest:
    event: threading.Event
    response: dict[str, Any] | None = None
    error: Exception | None = None


class BitgetLinearTradeWebSocketTransport:
    WS_URL = "wss://ws.bitget.com/v2/ws/private"
    VERIFY_PATH = "/user/verify"

    def __init__(
        self,
        credentials: ExchangeCredentials,
        *,
        connect_timeout_seconds: float = 10.0,
        request_timeout_seconds: float = 10.0,
        ping_interval_seconds: float = 30.0,
    ) -> None:
        self._credentials = credentials
        self._connect_timeout_seconds = float(connect_timeout_seconds)
        self._request_timeout_seconds = float(request_timeout_seconds)
        self._ping_interval_seconds = float(ping_interval_seconds)
        self._logger = get_logger("execution.bitget_linear_ws")
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
        self._last_ping_ts_ms = 0
        self._last_pong_ts_ms = 0
        self._ping_fail_count = 0

    def connect(self) -> bool:
        if websocket is None:
            raise BitgetTradeWebSocketError("websocket-client is required for BitgetLinearTradeWebSocketTransport")
        with self._lock:
            if self._thread is not None and self._thread.is_alive() and self._connected and self._authenticated:
                self._logger.info("bitget trade ws reuse existing connection")
                return True
            self._closing = False
            self._connected = False
            self._authenticated = False
            self._connect_error = None
            self._opened_event.clear()
            self._auth_event.clear()
            self._thread = threading.Thread(target=self._run_forever, name="bitget-linear-trade-ws", daemon=True)
            self._thread.start()
            if self._ping_thread is None or not self._ping_thread.is_alive():
                self._ping_thread = threading.Thread(target=self._ping_loop, name="bitget-linear-trade-ping", daemon=True)
                self._ping_thread.start()
            self._logger.info("bitget trade ws starting new connection | account_mode=classic")

        if not self._opened_event.wait(timeout=self._connect_timeout_seconds):
            raise BitgetTradeWebSocketError("bitget trade ws connect timeout")
        if not self._auth_event.wait(timeout=self._connect_timeout_seconds):
            raise BitgetTradeWebSocketError("bitget trade ws auth timeout")
        if self._connect_error is not None:
            raise BitgetTradeWebSocketError(str(self._connect_error))
        if not self._connected or not self._authenticated:
            raise BitgetTradeWebSocketError("bitget trade ws failed to connect")
        return False

    def close(self) -> None:
        with self._lock:
            self._closing = True
            ws_app = self._ws_app
        if ws_app is not None:
            ws_app.close()
        self._fail_all_pending(BitgetTradeWebSocketError("bitget trade ws closed"))

    def on_event(self, callback: Callable[[dict[str, Any]], None]) -> None:
        self._callbacks.append(callback)

    def request(
        self,
        channel: str,
        args_payload: dict[str, Any],
        on_request_sent: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        connection_reused = self.connect()
        return self._request_once(channel, args_payload, connection_reused=connection_reused, on_request_sent=on_request_sent)

    def _request_once(
        self,
        channel: str,
        args_payload: dict[str, Any],
        *,
        connection_reused: bool,
        on_request_sent: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        request_id = str(args_payload.get("id") or uuid.uuid4())
        normalized_args = dict(args_payload)
        normalized_args["id"] = request_id
        params = normalized_args.get("params")
        if channel == "place-order" and isinstance(params, dict):
            normalized_params = dict(params)
            if not normalized_params.get("clientOid"):
                normalized_params["clientOid"] = request_id
            normalized_args["params"] = normalized_params
        payload = {"op": "trade", "args": [normalized_args]}
        pending = _PendingRequest(event=threading.Event())
        with self._lock:
            self._pending[request_id] = pending

        try:
            self._send(payload)
            sent_at_ms = int(time.time() * 1000)
            meta = {
                "request_id": request_id,
                "channel": channel,
                "sent_at_ms": sent_at_ms,
                "connection_reused": connection_reused,
            }
            if on_request_sent is not None:
                on_request_sent(dict(meta))
            self._logger.info(
                "bitget trade ws request sent | id=%s | channel=%s | sent_at_ms=%s | reused=%s | args=%s",
                request_id,
                channel,
                sent_at_ms,
                connection_reused,
                normalized_args,
            )
        except Exception as exc:
            with self._lock:
                self._pending.pop(request_id, None)
            raise BitgetTradeWebSocketError(str(exc)) from exc

        if not pending.event.wait(timeout=self._request_timeout_seconds):
            with self._lock:
                self._pending.pop(request_id, None)
            raise BitgetTradeWebSocketError(f"bitget trade ws request timeout: {channel}")
        if pending.error is not None:
            raise BitgetTradeWebSocketError(str(pending.error)) from pending.error
        if pending.response is None:
            raise BitgetTradeWebSocketError(f"bitget trade ws empty response: {channel}")

        response_at_ms = int(time.time() * 1000)
        pending.response["_transport_meta"] = {
            "request_id": request_id,
            "channel": channel,
            "sent_at_ms": sent_at_ms,
            "response_at_ms": response_at_ms,
            "latency_ms": max(0, response_at_ms - sent_at_ms),
            "connection_reused": connection_reused,
        }
        self._logger.info(
            "bitget trade ws response received | id=%s | channel=%s | response_at_ms=%s | latency_ms=%s | reused=%s | summary=%s",
            request_id,
            channel,
            response_at_ms,
            max(0, response_at_ms - sent_at_ms),
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
            self._logger.error("bitget trade ws loop crashed: %s", exc)

    def _ping_loop(self) -> None:
        while not self._closing:
            time.sleep(self._ping_interval_seconds)
            if self._closing:
                return
            try:
                self._last_ping_ts_ms = int(time.time() * 1000)
                self._send_raw("ping")
            except Exception:
                self._ping_fail_count += 1
                return

    def _on_open(self) -> None:
        self._connected = True
        self._opened_event.set()
        self._send(self._login_payload())
        self._logger.info("bitget trade ws connected")

    def _on_message(self, message: str) -> None:
        if str(message).strip().lower() == "pong":
            self._last_pong_ts_ms = int(time.time() * 1000)
            return
        payload = json.loads(message)
        if not isinstance(payload, dict):
            return
        self._logger.info("bitget trade ws raw message | payload=%s", payload)

        event = str(payload.get("event", "")).strip().lower()
        if event == "login":
            code = str(payload.get("code", "")).strip()
            if code == "0":
                self._authenticated = True
                self._auth_event.set()
                self._logger.info("bitget trade ws authenticated")
            else:
                self._connect_error = BitgetTradeWebSocketError(payload.get("msg", "login failed"))
                self._auth_event.set()
            return
        if event == "error" and not self._authenticated:
            self._connect_error = BitgetTradeWebSocketError(self._error_message(payload))
            self._auth_event.set()
            return
        if event == "error":
            error = BitgetTradeWebSocketError(self._error_message(payload))
            request_id = self._extract_request_id(payload)
            if request_id:
                with self._lock:
                    pending = self._pending.pop(request_id, None)
                if pending is not None:
                    pending.error = error
                    pending.event.set()
                    return
            with self._lock:
                pending_items = list(self._pending.values())
                self._pending.clear()
            if pending_items:
                for pending in pending_items:
                    pending.error = error
                    pending.event.set()
                self._logger.warning(
                    "bitget trade ws error propagated to pending requests | code=%s | msg=%s | pending_count=%s",
                    payload.get("code"),
                    payload.get("msg"),
                    len(pending_items),
                )
                return
            for callback in list(self._callbacks):
                callback(payload)
            return

        if event == "trade":
            request_id = self._extract_request_id(payload)
            if request_id:
                with self._lock:
                    pending = self._pending.pop(request_id, None)
                if pending is not None:
                    code = str(payload.get("code", "")).strip()
                    if code not in {"0", "00000"}:
                        pending.error = BitgetTradeWebSocketError(self._error_message(payload))
                    else:
                        pending.response = payload
                    pending.event.set()
                    return

        for callback in list(self._callbacks):
            callback(payload)

    def _on_error(self, error: Any) -> None:
        self._logger.error("bitget trade ws error: %s", error)
        if not self._connected and self._connect_error is None:
            self._connect_error = error if isinstance(error, Exception) else BitgetTradeWebSocketError(str(error))
            self._opened_event.set()
            self._auth_event.set()
        self._fail_all_pending(error if isinstance(error, Exception) else BitgetTradeWebSocketError(str(error)))

    def _on_close(self, code: Any, message: Any) -> None:
        self._connected = False
        self._authenticated = False
        self._opened_event.set()
        self._auth_event.set()
        with self._lock:
            pending_ids = list(self._pending.keys())
        self._logger.info(
            "bitget trade ws closed | code=%s | message=%s | pending_ids=%s",
            code,
            message,
            pending_ids,
        )
        if not self._closing:
            detail = "bitget trade ws disconnected"
            if pending_ids:
                detail = (
                    "bitget trade ws disconnected while request was pending; "
                    "check Bitget websocket trade permission or request payload"
                )
            self._fail_all_pending(BitgetTradeWebSocketError(detail))

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

    def _send(self, payload: dict[str, Any]) -> None:
        self._send_raw(json.dumps(payload))

    def _send_raw(self, payload: str) -> None:
        with self._lock:
            ws_app = self._ws_app
        if ws_app is None:
            raise BitgetTradeWebSocketError("bitget trade ws is not connected")
        ws_app.send(payload)

    def diagnostics(self) -> dict[str, Any]:
        with self._lock:
            return {
                "connected": self._connected,
                "authenticated": self._authenticated,
                "closing": self._closing,
                "pending_requests": len(self._pending),
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
    def _extract_request_id(payload: dict[str, Any]) -> str | None:
        direct_id = payload.get("id")
        if direct_id:
            return str(direct_id)
        arg = payload.get("arg")
        if isinstance(arg, dict):
            value = arg.get("id")
            if value:
                return str(value)
        if isinstance(arg, list) and arg and isinstance(arg[0], dict):
            value = arg[0].get("id")
            if value:
                return str(value)
        return None

    @staticmethod
    def _error_message(payload: dict[str, Any]) -> str:
        code = payload.get("code")
        msg = payload.get("msg")
        if code is not None and msg:
            return f"[{code}] {msg}"
        return "bitget trade ws request failed"

    @staticmethod
    def _summarize_response(payload: dict[str, Any]) -> dict[str, Any]:
        arg = payload.get("arg")
        if isinstance(arg, dict):
            item = arg
        elif isinstance(arg, list) and arg and isinstance(arg[0], dict):
            item = arg[0]
        else:
            return {"event": payload.get("event"), "code": payload.get("code"), "id": payload.get("id")}
        params = item.get("params", {}) if isinstance(item.get("params"), dict) else {}
        return {
            "event": payload.get("event"),
            "code": payload.get("code"),
            "instId": item.get("instId"),
            "channel": item.get("channel"),
            "orderId": params.get("orderId"),
            "clientOid": params.get("clientOid"),
        }
