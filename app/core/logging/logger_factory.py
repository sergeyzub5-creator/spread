from __future__ import annotations

import logging
from pathlib import Path


class _WorkerIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "worker_id"):
            record.worker_id = "-"
        return True


_CONFIGURED = False
_LOG_DIR = Path(__file__).resolve().parents[3] / "logs"
_SESSION_TRACE_PATH = _LOG_DIR / "session_trace.log"


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

    _LOG_DIR.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(_SESSION_TRACE_PATH, mode="a", encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.addFilter(_WorkerIdFilter())
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | worker_id=%(worker_id)s | %(message)s")
    )

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.addHandler(file_handler)
    _CONFIGURED = True


def get_logger(name: str, worker_id: str | None = None) -> logging.LoggerAdapter:
    _configure_root_logging()
    logger = logging.getLogger(name)
    return logging.LoggerAdapter(logger, {"worker_id": worker_id or "-"})


def reset_session_trace_log() -> Path:
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
    for rotated_path in _LOG_DIR.glob(f"{_SESSION_TRACE_PATH.name}.*"):
        try:
            rotated_path.unlink()
        except Exception:
            pass
    _SESSION_TRACE_PATH.write_text("", encoding="utf-8")
    return _SESSION_TRACE_PATH


def session_trace_log_path() -> Path:
    return _SESSION_TRACE_PATH
