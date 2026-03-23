from __future__ import annotations

import json
import logging
import os
import threading
from pathlib import Path
from typing import Any


class _WorkerIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "worker_id"):
            record.worker_id = "-"
        return True


class _ScannerLogFilter(logging.Filter):
    _PREFIXES = (
        "scanner.",
        "ui.spot_futures_scanner",
    )

    def filter(self, record: logging.LogRecord) -> bool:
        name = str(getattr(record, "name", "") or "")
        return any(name.startswith(prefix) for prefix in self._PREFIXES)


class _ScannerV2LogFilter(logging.Filter):
    _PREFIXES = (
        "scanner.v2.",
        "ui.scanner_v2",
    )

    def filter(self, record: logging.LogRecord) -> bool:
        name = str(getattr(record, "name", "") or "")
        return any(name.startswith(prefix) for prefix in self._PREFIXES)


_CONFIGURED = False
_LOG_DIR = Path(__file__).resolve().parents[3] / "logs"
_SESSION_TRACE_PATH = _LOG_DIR / "session_trace.log"
_EVENTS_LOG_PATH = _LOG_DIR / "runtime_events.log"
_SCANNER_LOG_PATH = _LOG_DIR / "scanner_trace.log"
_SCANNER_V2_LOG_PATH = _LOG_DIR / "scanner_v2_trace.log"

# session_trace.log — основной подробный текстовый лог по умолчанию.
# Отключается только явно: SPREAD_SNIPER_SESSION_TRACE_LOG=0
_SESSION_TRACE_LOG_ENABLED = os.environ.get("SPREAD_SNIPER_SESSION_TRACE_LOG", "1").strip().lower() not in (
    "0",
    "false",
    "no",
    "off",
)

# runtime_events.log — отдельный JSONL-журнал событий emit_event.
_EVENTS_LOG_ENABLED = os.environ.get("SPREAD_SNIPER_EVENTS_LOG", "1").strip().lower() not in (
    "0",
    "false",
    "no",
    "off",
)
_SCANNER_LOG_ENABLED = os.environ.get("SPREAD_SNIPER_SCANNER_LOG", "1").strip().lower() not in (
    "0",
    "false",
    "no",
    "off",
)
_SCANNER_V2_LOG_ENABLED = os.environ.get("SPREAD_SNIPER_SCANNER_V2_LOG", "1").strip().lower() not in (
    "0",
    "false",
    "no",
    "off",
)

_DEFAULT_SKIP_EVENT_TYPES = frozenset(
    {
        "left_quote_update",
        "right_quote_update",
        "spread_update",
        "quote_received",
        "left_order_sent",
        "right_order_sent",
        "entry_left_sent",
        "entry_right_sent",
        "entry_left_ack",
        "entry_right_ack",
        "entry_left_fill",
        "entry_right_fill",
        "left_order_event",
        "right_order_event",
        "entry_left_event",
        "entry_right_event",
        "rest_poll_started",
        "rest_poll_stopped",
        "dual_exec_attempts_bound",
        "entry_attempts_bound",
    }
)
_EVENTS_LOG_ALL = os.environ.get("SPREAD_SNIPER_EVENTS_LOG_ALL", "0").strip().lower() in ("1", "true", "yes", "on")
_EVENTS_LOG_COMPACT = os.environ.get("SPREAD_SNIPER_EVENTS_LOG_COMPACT", "1").strip().lower() not in (
    "0",
    "false",
    "no",
    "off",
)

_EVENTS_LOG_EXCLUDE: set[str] = set()
for _part in os.environ.get("SPREAD_SNIPER_EVENTS_LOG_EXCLUDE", "").split(","):
    _p = _part.strip()
    if _p:
        _EVENTS_LOG_EXCLUDE.add(_p)

_events_log_lock = threading.Lock()


def _configure_root_logging() -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.ERROR)
    console_handler.addFilter(_WorkerIdFilter())
    console_handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | worker_id=%(worker_id)s | %(message)s")
    )

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.handlers.clear()
    root_logger.addHandler(console_handler)

    _LOG_DIR.mkdir(parents=True, exist_ok=True)
    if _SESSION_TRACE_LOG_ENABLED:
        file_handler = logging.FileHandler(_SESSION_TRACE_PATH, mode="a", encoding="utf-8")
        file_handler.setLevel(logging.INFO)
        file_handler.addFilter(_WorkerIdFilter())
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | worker_id=%(worker_id)s | %(message)s")
        )
        root_logger.addHandler(file_handler)

    if _SCANNER_LOG_ENABLED:
        scanner_handler = logging.FileHandler(_SCANNER_LOG_PATH, mode="a", encoding="utf-8")
        scanner_handler.setLevel(logging.INFO)
        scanner_handler.addFilter(_WorkerIdFilter())
        scanner_handler.addFilter(_ScannerLogFilter())
        scanner_handler.setFormatter(
            logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | worker_id=%(worker_id)s | %(message)s")
        )
        root_logger.addHandler(scanner_handler)

    if _SCANNER_V2_LOG_ENABLED:
        scanner_v2_handler = logging.FileHandler(_SCANNER_V2_LOG_PATH, mode="a", encoding="utf-8")
        scanner_v2_handler.setLevel(logging.INFO)
        scanner_v2_handler.addFilter(_WorkerIdFilter())
        scanner_v2_handler.addFilter(_ScannerV2LogFilter())
        scanner_v2_handler.setFormatter(
            logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | worker_id=%(worker_id)s | %(message)s")
        )
        root_logger.addHandler(scanner_v2_handler)

    _CONFIGURED = True


def get_logger(name: str, worker_id: str | None = None) -> logging.LoggerAdapter:
    _configure_root_logging()
    logger = logging.getLogger(name)
    return logging.LoggerAdapter(logger, {"worker_id": worker_id or "-"})


def reset_session_trace_log() -> Path:
    """Сбрасывает основной подробный лог и, если включено, JSONL-журнал событий."""
    global _CONFIGURED
    root_logger = logging.getLogger()
    for handler in list(root_logger.handlers):
        try:
            handler.flush()
            handler.close()
        except Exception:
            pass
    root_logger.handlers.clear()
    _CONFIGURED = False

    _LOG_DIR.mkdir(parents=True, exist_ok=True)
    _SESSION_TRACE_PATH.write_text("", encoding="utf-8")
    if _SCANNER_LOG_ENABLED:
        _SCANNER_LOG_PATH.write_text("", encoding="utf-8")
    elif _SCANNER_LOG_PATH.exists():
        _SCANNER_LOG_PATH.write_text("", encoding="utf-8")
    if _SCANNER_V2_LOG_ENABLED:
        _SCANNER_V2_LOG_PATH.write_text("", encoding="utf-8")
    elif _SCANNER_V2_LOG_PATH.exists():
        _SCANNER_V2_LOG_PATH.write_text("", encoding="utf-8")
    if _EVENTS_LOG_ENABLED:
        header = (
            json.dumps(
                {
                    "_schema": "runtime_events_v1",
                    "note": "timestamp_ms UTC-ish; event_type + payload; no quote ticks; no raw exchange blobs",
                },
                ensure_ascii=False,
            )
            + "\n"
        )
        _EVENTS_LOG_PATH.write_text(header, encoding="utf-8")
    elif _EVENTS_LOG_PATH.exists():
        _EVENTS_LOG_PATH.write_text("", encoding="utf-8")

    _configure_root_logging()
    return _SESSION_TRACE_PATH


def session_trace_log_path() -> Path:
    """Путь к основному подробному текстовому логу."""
    return _SESSION_TRACE_PATH


def full_session_log_path() -> Path:
    """Legacy alias: основной подробный лог теперь session_trace.log."""
    return _SESSION_TRACE_PATH


def full_session_log_enabled() -> bool:
    return _SESSION_TRACE_LOG_ENABLED


def events_log_enabled() -> bool:
    return _EVENTS_LOG_ENABLED


def events_log_path() -> Path:
    """Путь к отдельному JSONL-журналу runtime-событий."""
    return _EVENTS_LOG_PATH


def scanner_log_enabled() -> bool:
    return _SCANNER_LOG_ENABLED


def scanner_log_path() -> Path:
    return _SCANNER_LOG_PATH


def scanner_v2_log_enabled() -> bool:
    return _SCANNER_V2_LOG_ENABLED


def scanner_v2_log_path() -> Path:
    return _SCANNER_V2_LOG_PATH


def reset_events_log() -> Path:
    """Сбрасывает только JSONL-журнал runtime-событий."""
    _LOG_DIR.mkdir(parents=True, exist_ok=True)
    _EVENTS_LOG_PATH.write_text("", encoding="utf-8")
    return _EVENTS_LOG_PATH


def _compact_event_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if not payload:
        return {}
    out: dict[str, Any] = {}
    for key, value in payload.items():
        if key == "raw":
            continue
        if value is None:
            continue
        if isinstance(value, dict):
            nested = _compact_event_payload(value)
            if nested:
                out[key] = nested
        else:
            out[key] = value
    return out


def append_runtime_event(*, worker_id: str, event_type: str, timestamp_ms: int, payload: dict[str, Any]) -> None:
    if not _EVENTS_LOG_ENABLED:
        return
    if not _EVENTS_LOG_ALL and event_type in _DEFAULT_SKIP_EVENT_TYPES:
        return
    if _EVENTS_LOG_EXCLUDE:
        for excluded in _EVENTS_LOG_EXCLUDE:
            if excluded in event_type:
                return

    compact_payload = _compact_event_payload(dict(payload)) if _EVENTS_LOG_COMPACT else dict(payload)
    line_obj = {
        "worker_id": worker_id,
        "event_type": event_type,
        "timestamp": timestamp_ms,
        "payload": compact_payload,
    }
    line = json.dumps(line_obj, ensure_ascii=False, default=str) + "\n"

    _LOG_DIR.mkdir(parents=True, exist_ok=True)
    with _events_log_lock:
        with open(_EVENTS_LOG_PATH, "a", encoding="utf-8") as handle:
            handle.write(line)
