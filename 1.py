import sys
import time

from PySide6.QtCore import QTimer
from PySide6.QtWidgets import QApplication

from app.core.app import CoreApp
from app.core.logging.logger_factory import (
    full_session_log_enabled,
    full_session_log_path,
    get_logger,
    reset_session_trace_log,
    session_trace_log_path,
)
from app.ui import AppWindow
from app.ui.coordinator import UiCoordinator
from app.ui.theme import get_theme_manager
from app.ui.widgets.startup_splash import StartupSplash


def main() -> int:
    minimum_splash_ms = 2000
    # Один лог по умолчанию: session_trace.log = только события (JSONL). Полный простыня — FULL_SESSION_LOG=1.
    trace_path = reset_session_trace_log()
    startup_logger = get_logger("app.startup")
    startup_logger.info("session log reset | path=%s | mode=events_jsonl", trace_path)
    if full_session_log_enabled():
        startup_logger.info("full session log also enabled | path=%s", full_session_log_path())
    startup_logger.info("session startup begin")

    app = QApplication(sys.argv)
    app.setApplicationName("Spread Sniper UI Shell")
    get_theme_manager().set_theme("dark")

    splash = StartupSplash()
    splash.start()
    app.processEvents()

    startup_started_at = time.perf_counter()

    phase_started_at = time.perf_counter()
    startup_logger.info("startup phase begin | phase=core_app.create_default")
    core_app = CoreApp.create_default()
    startup_logger.info(
        "startup phase done | phase=core_app.create_default | elapsed_ms=%d",
        int((time.perf_counter() - phase_started_at) * 1000),
    )

    phase_started_at = time.perf_counter()
    startup_logger.info("startup phase begin | phase=ui_coordinator.create")
    coordinator = UiCoordinator(core_app=core_app)
    startup_logger.info(
        "startup phase done | phase=ui_coordinator.create | elapsed_ms=%d",
        int((time.perf_counter() - phase_started_at) * 1000),
    )

    phase_started_at = time.perf_counter()
    startup_logger.info("startup phase begin | phase=ui_coordinator.bootstrap")
    coordinator.bootstrap()
    startup_logger.info(
        "startup phase done | phase=ui_coordinator.bootstrap | elapsed_ms=%d",
        int((time.perf_counter() - phase_started_at) * 1000),
    )

    phase_started_at = time.perf_counter()
    startup_logger.info("startup phase begin | phase=window.create")
    window = AppWindow(coordinator=coordinator)
    startup_logger.info(
        "startup phase done | phase=window.create | elapsed_ms=%d",
        int((time.perf_counter() - phase_started_at) * 1000),
    )

    elapsed_ms = int((time.perf_counter() - startup_started_at) * 1000)
    remaining_splash_ms = max(0, minimum_splash_ms - elapsed_ms)
    startup_logger.info(
        "ui startup complete | elapsed_ms=%d | log=%s",
        elapsed_ms,
        session_trace_log_path(),
    )
    startup_logger.info(
        "startup splash minimum hold | minimum_ms=%d | elapsed_ms=%d | remaining_ms=%d",
        minimum_splash_ms,
        elapsed_ms,
        remaining_splash_ms,
    )

    def _finish_startup() -> None:
        splash.close()
        window.show()

    if remaining_splash_ms > 0:
        QTimer.singleShot(remaining_splash_ms, _finish_startup)
    else:
        _finish_startup()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
