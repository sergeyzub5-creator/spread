from __future__ import annotations

import logging


class _WorkerIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "worker_id"):
            record.worker_id = "-"
        return True


_CONFIGURED = False


def _configure_root_logging() -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return

    handler = logging.StreamHandler()
    handler.addFilter(_WorkerIdFilter())
    handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | worker_id=%(worker_id)s | %(message)s")
    )

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    _CONFIGURED = True


def get_logger(name: str, worker_id: str | None = None) -> logging.LoggerAdapter:
    _configure_root_logging()
    logger = logging.getLogger(name)
    return logging.LoggerAdapter(logger, {"worker_id": worker_id or "-"})
