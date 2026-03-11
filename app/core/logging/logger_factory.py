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


_CONFIGURED = False
_LOG_DIR = Path(__file__).resolve().parents[3] / "logs"

# По умолчанию: один файл session_trace.log — только события (JSONL, как раньше один лог, но без простыни INFO).
# Полный лог всех logger'ов — опционально в session_trace_full.log.
_SESSION_TRACE_PATH = _LOG_DIR / "session_trace.log"
_FULL_LOG_PATH = _LOG_DIR / "session_trace_full.log"

# Полный INFO в файл (всё как раньше простынёй) — только если SPREAD_SNIPER_FULL_SESSION_LOG=1
_FULL_SESSION_LOG_ENABLED = os.environ.get("SPREAD_SNIPER_FULL_SESSION_LOG", "0").strip().lower() in ("1", "true", "yes", "on")

# Запись событий emit_event в session_trace.log — по умолчанию да; выключить: SPREAD_SNIPER_EVENTS_LOG=0
_EVENTS_LOG_ENABLED = os.environ.get("SPREAD_SNIPER_EVENTS_LOG", "1").strip().lower() not in ("0", "false", "no", "off")

# По умолчанию в лог попадают только строки, по которым удобно строить отчёт за часы работы.
# Исключаем: тики котировок; дубликаты (sent/ack/fill под другим именем); потоковые order_event с сырым raw;
# rest_poll_*; тяжёлый execution_stream_health_updated (оставляем только warning при деградации — отдельное событие).
# Включить ВСЁ как раньше: SPREAD_SNIPER_EVENTS_LOG_ALL=1
_DEFAULT_SKIP_EVENT_TYPES = frozenset(
    {
        "left_quote_update",
        "right_quote_update",
        "spread_update",
        "quote_received",
        # Дубликаты имён (то же самое уже в *_order_ack / *_order_filled / entry_started)
        "left_order_sent",
        "right_order_sent",
        "entry_left_sent",
        "entry_right_sent",
        "entry_left_ack",
        "entry_right_ack",
        "entry_left_fill",
        "entry_right_fill",
        # Один поток WS/REST — десятки строк на один fill; для отчёта достаточно ack + filled + done
        "left_order_event",
        "right_order_event",
        "entry_left_event",
        "entry_right_event",
        # Шум опроса
        "rest_poll_started",
        "rest_poll_stopped",
        # Дублирует dual_exec_started / entry_started
        "dual_exec_attempts_bound",
        "entry_attempts_bound",
        # Каждый раз огромный payload streams; при сбое будет execution_stream_health_warning
        "execution_stream_health_updated",
    }
)
_EVENTS_LOG_ALL = os.environ.get("SPREAD_SNIPER_EVENTS_LOG_ALL", "0").strip().lower() in ("1", "true", "yes", "on")

# Ужать payload: выкинуть raw (биржевой ответ) и пустые поля — строка короче, отчёт читабельнее.
_EVENTS_LOG_COMPACT = os.environ.get("SPREAD_SNIPER_EVENTS_LOG_COMPACT", "1").strip().lower() not in ("0", "false", "no", "off")

# Дополнительно: через запятую подстроки event_type, которые не писать
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

    handler = logging.StreamHandler()
    handler.setLevel(logging.ERROR)
    handler.addFilter(_WorkerIdFilter())
    handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | worker_id=%(worker_id)s | %(message)s")
    )

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.handlers.clear()
    root_logger.addHandler(handler)

    _LOG_DIR.mkdir(parents=True, exist_ok=True)
    if _FULL_SESSION_LOG_ENABLED:
        file_handler = logging.FileHandler(_FULL_LOG_PATH, mode="a", encoding="utf-8")
        file_handler.setLevel(logging.INFO)
        file_handler.addFilter(_WorkerIdFilter())
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | worker_id=%(worker_id)s | %(message)s")
        )
        root_logger.addHandler(file_handler)

    _CONFIGURED = True


def get_logger(name: str, worker_id: str | None = None) -> logging.LoggerAdapter:
    _configure_root_logging()
    logger = logging.getLogger(name)
    return logging.LoggerAdapter(logger, {"worker_id": worker_id or "-"})


def reset_session_trace_log() -> Path:
    """
    Обнуляет session_trace.log (события JSONL) при каждом запуске.
    Если включён полный лог — обнуляет session_trace_full.log.
    """
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
    if _FULL_SESSION_LOG_ENABLED:
        for rotated_path in _LOG_DIR.glob(f"{_FULL_LOG_PATH.name}.*"):
            try:
                rotated_path.unlink()
            except Exception:
                pass
        _FULL_LOG_PATH.write_text("", encoding="utf-8")
    _SESSION_TRACE_PATH.write_text("", encoding="utf-8")
    # Маркер формата — удобно при разборе для отчёта
    if _EVENTS_LOG_ENABLED:
        header = (
            json.dumps(
                {
                    "_schema": "session_events_v1",
                    "note": "timestamp_ms UTC-ish; event_type + payload; no quote ticks; no raw exchange blobs",
                },
                ensure_ascii=False,
            )
            + "\n"
        )
        _SESSION_TRACE_PATH.write_text(header, encoding="utf-8")
    _configure_root_logging()
    return _SESSION_TRACE_PATH


def session_trace_log_path() -> Path:
    """Путь к основному логу (по умолчанию только события JSONL)."""
    return _SESSION_TRACE_PATH


def full_session_log_path() -> Path:
    """Путь к полному логу, если включён FULL_SESSION_LOG."""
    return _FULL_LOG_PATH


def full_session_log_enabled() -> bool:
    return _FULL_SESSION_LOG_ENABLED


def events_log_enabled() -> bool:
    return _EVENTS_LOG_ENABLED


def events_log_path() -> Path:
    """Совпадает с session_trace_log_path — один файл."""
    return _SESSION_TRACE_PATH


def reset_events_log() -> Path:
    """То же, что обнуление session_trace.log."""
    _LOG_DIR.mkdir(parents=True, exist_ok=True)
    _SESSION_TRACE_PATH.write_text("", encoding="utf-8")
    return _SESSION_TRACE_PATH


def _compact_event_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Убирает raw и значения None — меньше строка, те же факты для отчёта."""
    if not payload:
        return {}
    out: dict[str, Any] = {}
    for k, v in payload.items():
        if k == "raw":
            continue
        if v is None:
            continue
        if isinstance(v, dict):
            nested = _compact_event_payload(v)
            if nested:
                out[k] = nested
        else:
            out[k] = v
    return out


def append_runtime_event(*, worker_id: str, event_type: str, timestamp_ms: int, payload: dict[str, Any]) -> None:
    if not _EVENTS_LOG_ENABLED:
        return
    if not _EVENTS_LOG_ALL and event_type in _DEFAULT_SKIP_EVENT_TYPES:
        return
    if _EVENTS_LOG_EXCLUDE:
        for ex in _EVENTS_LOG_EXCLUDE:
            if ex in event_type:
                return
    pl = _compact_event_payload(dict(payload)) if _EVENTS_LOG_COMPACT else dict(payload)
    line_obj = {
        "worker_id": worker_id,
        "event_type": event_type,
        "timestamp": timestamp_ms,
        "payload": pl,
    }
    line = json.dumps(line_obj, ensure_ascii=False, default=str) + "\n"
    _LOG_DIR.mkdir(parents=True, exist_ok=True)
    with _events_log_lock:
        with open(_SESSION_TRACE_PATH, "a", encoding="utf-8") as f:
            f.write(line)
