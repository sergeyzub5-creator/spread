from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from app.core.models.account import ExchangeCredentials


class BitgetApiError(RuntimeError):
    def __init__(self, message: str, code: str | None = None) -> None:
        super().__init__(message)
        self.code = code


class BitgetPublicHttpClient:
    BASE_URL = "https://api.bitget.com"

    def __init__(self, *, timeout_seconds: float = 10.0) -> None:
        self._timeout_seconds = float(timeout_seconds)

    def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        params = {key: value for key, value in (params or {}).items() if value is not None}
        query = urlencode([(key, str(value)) for key, value in params.items()])
        url = f"{self.BASE_URL}{path}"
        if query:
            url = f"{url}?{query}"
        request = Request(url, headers={"User-Agent": "spread-sniper-ui-shell/1.0"})
        try:
            with urlopen(request, timeout=self._timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            raise self._http_error(exc) from exc
        except URLError as exc:
            raise BitgetApiError(f"network error: {exc.reason}") from exc

        if not isinstance(payload, dict):
            raise BitgetApiError("invalid Bitget response payload")
        code = str(payload.get("code", "")).strip()
        if code != "00000":
            raise BitgetApiError(str(payload.get("msg", "Bitget request failed")), code=code or None)
        return payload

    @staticmethod
    def _http_error(error: HTTPError) -> BitgetApiError:
        try:
            payload = json.loads(error.read().decode("utf-8"))
            if isinstance(payload, dict):
                code = str(payload.get("code", "")).strip() or None
                message = str(payload.get("msg") or payload.get("message") or "").strip()
                if message:
                    return BitgetApiError(message, code=code)
        except Exception:
            pass
        return BitgetApiError(f"http {error.code}")


class BitgetSignedHttpClient:
    BASE_URL = "https://api.bitget.com"

    def __init__(
        self,
        credentials: ExchangeCredentials,
        *,
        timeout_seconds: float = 10.0,
    ) -> None:
        self._credentials = credentials
        self._timeout_seconds = float(timeout_seconds)

    def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        return self.request("GET", path, params=params)

    def post(self, path: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
        return self.request("POST", path, body=body)

    def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_method = str(method or "GET").strip().upper()
        params = {key: value for key, value in (params or {}).items() if value is not None}
        body = {key: value for key, value in (body or {}).items() if value is not None}

        query = urlencode([(key, str(value)) for key, value in params.items()])
        body_text = json.dumps(body, separators=(",", ":"), ensure_ascii=True) if body else ""
        url = f"{self.BASE_URL}{path}"
        if query:
            url = f"{url}?{query}"

        timestamp = str(int(time.time() * 1000))
        request_path = path if not query else f"{path}?{query}"
        prehash = f"{timestamp}{normalized_method}{request_path}{body_text}"
        signature = base64.b64encode(
            hmac.new(
                self._credentials.api_secret.encode("utf-8"),
                prehash.encode("utf-8"),
                hashlib.sha256,
            ).digest()
        ).decode("utf-8")

        headers = {
            "User-Agent": "spread-sniper-ui-shell/1.0",
            "ACCESS-KEY": self._credentials.api_key,
            "ACCESS-SIGN": signature,
            "ACCESS-PASSPHRASE": self._credentials.api_passphrase,
            "ACCESS-TIMESTAMP": timestamp,
            "locale": "en-US",
            "Content-Type": "application/json",
        }
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
            raise BitgetPublicHttpClient._http_error(exc) from exc
        except URLError as exc:
            raise BitgetApiError(f"network error: {exc.reason}") from exc

        if not isinstance(payload, dict):
            raise BitgetApiError("invalid Bitget response payload")
        code = str(payload.get("code", "")).strip()
        if code != "00000":
            raise BitgetApiError(
                str(payload.get("msg") or payload.get("message") or "Bitget request failed"),
                code=code or None,
            )
        return payload
