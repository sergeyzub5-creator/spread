from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from app.core.logging.logger_factory import get_logger
from app.core.models.account import ExchangeCredentials
from app.core.models.execution import ExecutionStreamEvent

try:
    import websocket
except ImportError:  # pragma: no cover
    websocket = None


class BinanceUsdmUserDataStream:
    BASE_URL = "https://fapi.binance.com"
    WS_BASE_URL = "wss://fstream.binance.com/ws"
    LISTEN_KEY_PATH = "/fapi/v1/listenKey"

    def __init__(
        self,
        credentials: ExchangeCredentials,
        *,
        timeout_seconds: float = 10.0,
        keepalive_interval_seconds: float = 30 * 60,
    ) -> None:
        self._credentials = credentials
        self._timeout_seconds = float(timeout_seconds)
        self._keepalive_interval_seconds = float(keepalive_interval_seconds)
        self._logger = get_logger("execution.binance_usdm_user_stream")
        self._callbacks: list[Callable[[ExecutionStreamEvent], None]] = []
        self._lock = threading.RLock()
        self._listen_key: str | None = None
        self._thread: threading.Thread | None = None
        self._keepalive_thread: threading.Thread | None = None
        self._ws_app = None
        self._connected = False
        self._closing = False
        self._opened_event = threading.Event()
        self._connect_error: Exception | None = None

    def connect(self) -> None:
        if websocket is None:
            raise RuntimeError("websocket-client is required for BinanceUsdmUserDataStream")

        with self._lock:
            if self._thread is not None and self._thread.is_alive() and self._connected:
                self._logger.info("binance user data stream reuse existing connection | listen_key=%s", self._listen_key)
                return
            self._closing = False
            self._connected = False
            self._connect_error = None
            self._opened_event.clear()
            self._listen_key = self._listen_key or self._create_listen_key()
            self._thread = threading.Thread(target=self._run_forever, name="binance-usdm-user-stream", daemon=True)
            self._thread.start()
            self._logger.info("binance user data stream starting new connection | listen_key=%s", self._listen_key)
            if self._keepalive_thread is None or not self._keepalive_thread.is_alive():
                self._keepalive_thread = threading.Thread(target=self._keepalive_loop, name="binance-usdm-user-keepalive", daemon=True)
                self._keepalive_thread.start()

        if not self._opened_event.wait(timeout=self._timeout_seconds):
            raise RuntimeError("binance user data stream connect timeout")
        if self._connect_error is not None:
            raise RuntimeError(str(self._connect_error))
        if not self._connected:
            raise RuntimeError("binance user data stream failed to connect")

    def on_execution_event(self, callback: Callable[[ExecutionStreamEvent], None]) -> None:
        self._callbacks.append(callback)

    def close(self) -> None:
        with self._lock:
            self._closing = True
            ws_app = self._ws_app
            listen_key = self._listen_key
        if ws_app is not None:
            ws_app.close()
        if listen_key:
            try:
                self._delete_listen_key(listen_key)
            except Exception as exc:
                self._logger.warning("binance user stream delete listenKey failed: %s", exc)

    def _run_forever(self) -> None:
        with self._lock:
            listen_key = self._listen_key
        if not listen_key:
            self._connect_error = RuntimeError("missing Binance listenKey")
            self._opened_event.set()
            return

        self._ws_app = websocket.WebSocketApp(
            f"{self.WS_BASE_URL}/{listen_key}",
            on_open=lambda ws: self._on_open(),
            on_message=lambda ws, message: self._on_message(message),
            on_error=lambda ws, error: self._on_error(error),
            on_close=lambda ws, status_code, message: self._on_close(status_code, message),
        )
        try:
            self._ws_app.run_forever()
        except Exception as exc:  # pragma: no cover
            self._connect_error = exc
            self._opened_event.set()
            self._logger.error("binance user stream loop crashed: %s", exc)

    def _keepalive_loop(self) -> None:
        while not self._closing:
            time.sleep(self._keepalive_interval_seconds)
            if self._closing:
                break
            with self._lock:
                listen_key = self._listen_key
            if not listen_key:
                continue
            try:
                self._keepalive_listen_key(listen_key)
                self._logger.info("binance user stream listenKey refreshed")
            except Exception as exc:
                self._logger.warning("binance user stream keepalive failed: %s", exc)

    def _on_open(self) -> None:
        self._connected = True
        self._connect_error = None
        self._opened_event.set()
        self._logger.info("binance user data stream connected")

    def _on_message(self, message: str) -> None:
        payload = json.loads(message)
        if not isinstance(payload, dict):
            return
        event = self._normalize_event(payload)
        if event is None:
            return
        self._logger.info(
            "binance user data event received | event_type=%s | event_time=%s | transaction_time=%s | symbol=%s | order_id=%s | status=%s | exec_type=%s",
            event.event_type,
            event.event_time,
            event.transaction_time,
            event.symbol,
            event.order_id,
            event.order_status,
            event.execution_type,
        )
        for callback in list(self._callbacks):
            callback(event)

    def _on_error(self, error: Any) -> None:
        self._logger.error("binance user data stream error: %s", error)
        if not self._connected and self._connect_error is None:
            self._connect_error = error if isinstance(error, Exception) else RuntimeError(str(error))
            self._opened_event.set()

    def _on_close(self, status_code: Any, message: Any) -> None:
        self._connected = False
        self._opened_event.set()
        self._logger.info("binance user data stream closed | code=%s | message=%s", status_code, message)

    def _normalize_event(self, payload: dict[str, Any]) -> ExecutionStreamEvent | None:
        event_name = str(payload.get("e", "")).strip()
        if not event_name:
            return None

        if event_name == "ORDER_TRADE_UPDATE":
            order_payload = payload.get("o", {})
            if not isinstance(order_payload, dict):
                order_payload = {}
            return ExecutionStreamEvent(
                exchange="binance",
                event_type="order_trade_update",
                event_time=self._int_or_none(payload.get("E")),
                transaction_time=self._int_or_none(order_payload.get("T") or payload.get("T")),
                symbol=self._str_or_none(order_payload.get("s")),
                order_id=self._str_or_none(order_payload.get("i")),
                client_order_id=self._str_or_none(order_payload.get("c")),
                order_status=self._str_or_none(order_payload.get("X")),
                execution_type=self._str_or_none(order_payload.get("x")),
                side=self._str_or_none(order_payload.get("S")),
                order_type=self._str_or_none(order_payload.get("o")),
                position_side=self._str_or_none(order_payload.get("ps")),
                last_fill_qty=self._str_or_none(order_payload.get("l")),
                cumulative_fill_qty=self._str_or_none(order_payload.get("z")),
                last_fill_price=self._str_or_none(order_payload.get("L")),
                average_price=self._str_or_none(order_payload.get("ap")),
                realized_pnl=self._str_or_none(order_payload.get("rp")),
                raw=payload,
            )

        return ExecutionStreamEvent(
            exchange="binance",
            event_type=event_name.lower(),
            event_time=self._int_or_none(payload.get("E")),
            transaction_time=self._int_or_none(payload.get("T")),
            symbol=None,
            order_id=None,
            client_order_id=None,
            order_status=None,
            execution_type=None,
            side=None,
            order_type=None,
            position_side=None,
            last_fill_qty=None,
            cumulative_fill_qty=None,
            last_fill_price=None,
            average_price=None,
            realized_pnl=None,
            raw=payload,
        )

    def _create_listen_key(self) -> str:
        payload = self._listen_key_request("POST")
        listen_key = str(payload.get("listenKey", "")).strip()
        if not listen_key:
            raise RuntimeError("binance user stream did not return listenKey")
        self._logger.info("binance user stream listenKey created")
        return listen_key

    def _keepalive_listen_key(self, listen_key: str) -> None:
        self._listen_key_request("PUT", listen_key=listen_key)

    def _delete_listen_key(self, listen_key: str) -> None:
        self._listen_key_request("DELETE", listen_key=listen_key)

    def _listen_key_request(self, method: str, *, listen_key: str | None = None) -> dict[str, Any]:
        headers = {
            "User-Agent": "spread-sniper-ui-shell/1.0",
            "X-MBX-APIKEY": self._credentials.api_key,
        }
        url = f"{self.BASE_URL}{self.LISTEN_KEY_PATH}"
        if listen_key:
            url = f"{url}?listenKey={listen_key}"
        request = Request(url, headers=headers, method=method)
        try:
            with urlopen(request, timeout=self._timeout_seconds) as response:
                body = response.read().decode("utf-8")
                if not body:
                    return {}
                payload = json.loads(body)
                return payload if isinstance(payload, dict) else {}
        except HTTPError as exc:
            raise RuntimeError(f"http {exc.code}") from exc
        except URLError as exc:
            raise RuntimeError(f"network error: {exc.reason}") from exc

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
