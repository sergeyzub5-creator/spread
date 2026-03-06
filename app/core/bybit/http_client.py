from __future__ import annotations

import hashlib
import hmac
import json
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from app.core.models.account import ExchangeCredentials


class BybitApiError(RuntimeError):
    def __init__(self, message: str, code: int | None = None) -> None:
        super().__init__(message)
        self.code = code


class BybitV5HttpClient:
    BASE_URL = "https://api.bybit.com"
    TIME_PATH = "/v5/market/time"

    def __init__(
        self,
        credentials: ExchangeCredentials | None = None,
        *,
        timeout_seconds: float = 10.0,
        recv_window_ms: int = 5000,
        time_sync_ttl_ms: int = 30000,
    ) -> None:
        self._credentials = credentials
        self._timeout_seconds = float(timeout_seconds)
        self._recv_window_ms = int(recv_window_ms)
        self._time_sync_ttl_ms = int(time_sync_ttl_ms)
        self._time_offset_ms = 0
        self._last_time_sync_at_ms = 0

    def current_timestamp_ms(self) -> int:
        self.sync_time_offset()
        return int(time.time() * 1000) + self._time_offset_ms

    def time_offset_ms(self) -> int:
        return self._time_offset_ms

    def sync_time_offset(self, *, force: bool = False) -> int:
        now_ms = int(time.time() * 1000)
        if not force and self._last_time_sync_at_ms > 0:
            if now_ms - self._last_time_sync_at_ms <= self._time_sync_ttl_ms:
                return self._time_offset_ms

        payload = self.get(self.TIME_PATH, auth=False)
        result = payload.get("result", {}) if isinstance(payload, dict) else {}
        server_time_ms = int(result.get("timeSecond", 0) or 0) * 1000
        time_nano = str(result.get("timeNano", "")).strip()
        if time_nano.isdigit():
            server_time_ms = int(int(time_nano) / 1_000_000)
        if server_time_ms <= 0:
            raise BybitApiError("failed to sync Bybit server time")

        local_time_ms = int(time.time() * 1000)
        self._time_offset_ms = server_time_ms - local_time_ms
        self._last_time_sync_at_ms = local_time_ms
        return self._time_offset_ms

    def get(self, path: str, params: dict[str, Any] | None = None, *, auth: bool = False) -> dict[str, Any]:
        return self.request("GET", path, params=params, auth=auth)

    def post(self, path: str, body: dict[str, Any] | None = None, *, auth: bool = False) -> dict[str, Any]:
        return self.request("POST", path, body=body, auth=auth)

    def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
        auth: bool = False,
    ) -> dict[str, Any]:
        normalized_method = str(method or "GET").strip().upper()
        params = {key: value for key, value in (params or {}).items() if value is not None}
        body = {key: value for key, value in (body or {}).items() if value is not None}
        headers = {"User-Agent": "spread-sniper-ui-shell/1.0"}
        query_string = urlencode([(key, str(params[key])) for key in sorted(params)])
        body_text = json.dumps(body, separators=(",", ":"), ensure_ascii=True) if body else ""
        url = f"{self.BASE_URL}{path}"
        if query_string:
            url = f"{url}?{query_string}"

        if auth:
            if self._credentials is None:
                raise BybitApiError("Bybit credentials are required for authenticated request")
            timestamp = str(self.current_timestamp_ms())
            recv_window = str(self._recv_window_ms)
            if normalized_method == "GET":
                param_text = query_string
            else:
                param_text = body_text
            signature_payload = f"{timestamp}{self._credentials.api_key}{recv_window}{param_text}"
            signature = hmac.new(
                self._credentials.api_secret.encode("utf-8"),
                signature_payload.encode("utf-8"),
                hashlib.sha256,
            ).hexdigest()
            headers.update(
                {
                    "X-BAPI-API-KEY": self._credentials.api_key,
                    "X-BAPI-TIMESTAMP": timestamp,
                    "X-BAPI-RECV-WINDOW": recv_window,
                    "X-BAPI-SIGN": signature,
                }
            )
        if normalized_method == "POST":
            headers["Content-Type"] = "application/json"

        request = Request(
            url,
            data=body_text.encode("utf-8") if normalized_method == "POST" else None,
            headers=headers,
            method=normalized_method,
        )
        try:
            with urlopen(request, timeout=self._timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            raise self._http_error(exc) from exc
        except URLError as exc:
            raise BybitApiError(f"network error: {exc.reason}") from exc

        if not isinstance(payload, dict):
            raise BybitApiError("invalid Bybit response payload")
        ret_code = int(payload.get("retCode", 0) or 0)
        if ret_code != 0:
            raise BybitApiError(str(payload.get("retMsg", "Bybit request failed")), code=ret_code)
        return payload

    @staticmethod
    def _http_error(error: HTTPError) -> BybitApiError:
        try:
            payload = json.loads(error.read().decode("utf-8"))
            if isinstance(payload, dict):
                ret_code = payload.get("retCode")
                ret_msg = payload.get("retMsg")
                if ret_msg:
                    return BybitApiError(str(ret_msg), code=int(ret_code) if ret_code is not None else None)
        except Exception:
            pass
        return BybitApiError(f"http {error.code}")
