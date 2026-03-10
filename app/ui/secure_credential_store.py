from __future__ import annotations

import base64
import json
import os
import threading
from pathlib import Path
from typing import Any
from uuid import uuid4


_STORE_PATH = Path(__file__).resolve().parents[1] / "data" / "credentials.secure.json"
_LOCK = threading.RLock()
_BLOB_SCOPE = b"spread-sniper-ui-shell"


if os.name == "nt":
    import ctypes
    from ctypes import wintypes

    class _DataBlob(ctypes.Structure):
        _fields_ = [
            ("cbData", wintypes.DWORD),
            ("pbData", ctypes.POINTER(ctypes.c_byte)),
        ]

    _crypt32 = ctypes.windll.crypt32
    _kernel32 = ctypes.windll.kernel32


def _load_store() -> dict[str, Any]:
    if not _STORE_PATH.exists():
        return {"version": 1, "credentials": {}}
    try:
        payload = json.loads(_STORE_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return {"version": 1, "credentials": {}}
    credentials = payload.get("credentials")
    if not isinstance(credentials, dict):
        credentials = {}
    return {"version": 1, "credentials": credentials}


def _save_store(payload: dict[str, Any]) -> None:
    _STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    _STORE_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _protect_bytes(raw: bytes) -> bytes:
    if os.name != "nt":
        return raw
    input_buffer = ctypes.create_string_buffer(raw)
    input_blob = _DataBlob(len(raw), ctypes.cast(input_buffer, ctypes.POINTER(ctypes.c_byte)))
    entropy_buffer = ctypes.create_string_buffer(_BLOB_SCOPE)
    entropy_blob = _DataBlob(len(_BLOB_SCOPE), ctypes.cast(entropy_buffer, ctypes.POINTER(ctypes.c_byte)))
    output_blob = _DataBlob()
    if not _crypt32.CryptProtectData(
        ctypes.byref(input_blob),
        None,
        ctypes.byref(entropy_blob),
        None,
        None,
        0,
        ctypes.byref(output_blob),
    ):
        raise OSError("CryptProtectData failed")
    try:
        return ctypes.string_at(output_blob.pbData, output_blob.cbData)
    finally:
        if output_blob.pbData:
            _kernel32.LocalFree(output_blob.pbData)


def _unprotect_bytes(raw: bytes) -> bytes:
    if os.name != "nt":
        return raw
    input_buffer = ctypes.create_string_buffer(raw)
    input_blob = _DataBlob(len(raw), ctypes.cast(input_buffer, ctypes.POINTER(ctypes.c_byte)))
    entropy_buffer = ctypes.create_string_buffer(_BLOB_SCOPE)
    entropy_blob = _DataBlob(len(_BLOB_SCOPE), ctypes.cast(entropy_buffer, ctypes.POINTER(ctypes.c_byte)))
    output_blob = _DataBlob()
    if not _crypt32.CryptUnprotectData(
        ctypes.byref(input_blob),
        None,
        ctypes.byref(entropy_blob),
        None,
        None,
        0,
        ctypes.byref(output_blob),
    ):
        raise OSError("CryptUnprotectData failed")
    try:
        return ctypes.string_at(output_blob.pbData, output_blob.cbData)
    finally:
        if output_blob.pbData:
            _kernel32.LocalFree(output_blob.pbData)


def _encode_payload(payload: dict[str, str]) -> str:
    raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    return base64.b64encode(_protect_bytes(raw)).decode("ascii")


def _decode_payload(encoded: str) -> dict[str, str] | None:
    try:
        raw = base64.b64decode(str(encoded or "").encode("ascii"))
        payload = json.loads(_unprotect_bytes(raw).decode("utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    return {str(key): str(value or "") for key, value in payload.items()}


def save_exchange_credentials(
    *,
    exchange_code: str,
    api_key: str,
    api_secret: str,
    api_passphrase: str = "",
    credential_ref: str | None = None,
) -> str:
    normalized_exchange = str(exchange_code or "").strip().lower()
    if not normalized_exchange:
        raise ValueError("exchange_code is required")
    if not str(api_key or "").strip() or not str(api_secret or "").strip():
        raise ValueError("api_key and api_secret are required")
    ref = str(credential_ref or uuid4().hex).strip()
    payload = {
        "exchange_code": normalized_exchange,
        "api_key": str(api_key or "").strip(),
        "api_secret": str(api_secret or "").strip(),
        "api_passphrase": str(api_passphrase or "").strip(),
    }
    with _LOCK:
        store = _load_store()
        credentials = dict(store.get("credentials") or {})
        credentials[ref] = {
            "exchange_code": normalized_exchange,
            "payload": _encode_payload(payload),
        }
        store["credentials"] = credentials
        _save_store(store)
    return ref


def load_exchange_credentials(credential_ref: str | None) -> dict[str, str] | None:
    ref = str(credential_ref or "").strip()
    if not ref:
        return None
    with _LOCK:
        store = _load_store()
        record = (store.get("credentials") or {}).get(ref)
    if not isinstance(record, dict):
        return None
    payload = _decode_payload(str(record.get("payload", "")))
    if payload is None:
        return None
    return payload


def delete_exchange_credentials(credential_ref: str | None) -> None:
    ref = str(credential_ref or "").strip()
    if not ref:
        return
    with _LOCK:
        store = _load_store()
        credentials = dict(store.get("credentials") or {})
        if ref in credentials:
            credentials.pop(ref, None)
            store["credentials"] = credentials
            _save_store(store)


def has_exchange_credentials(credential_ref: str | None) -> bool:
    return load_exchange_credentials(credential_ref) is not None


def masked_api_key(credential_ref: str | None, fallback_key: str | None = None) -> str:
    api_key = str(fallback_key or "").strip()
    if not api_key:
        credentials = load_exchange_credentials(credential_ref)
        if credentials is None:
            return ""
        api_key = str(credentials.get("api_key", "")).strip()
    if not api_key:
        return ""
    if len(api_key) <= 8:
        return "*" * len(api_key)
    return f"{api_key[:4]}...{api_key[-4:]}"
