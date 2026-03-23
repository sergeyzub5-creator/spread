from __future__ import annotations

import os
import sys

from PySide6.QtGui import QFont
from PySide6.QtWidgets import QApplication

from app.futures_spread_scanner_v2.common.logger import (
    events_log_path,
    full_session_log_enabled,
    full_session_log_path,
    get_logger,
    reset_session_trace_log,
    scanner_v2_log_enabled,
    scanner_v2_log_path,
)
from app.futures_spread_scanner_v2 import FuturesSpreadScannerV2Window
from app.futures_spread_scanner_v2.common.global_focus import install_global_click_affordance, install_global_line_edit_blur
from app.futures_spread_scanner_v2.common.theme import get_theme_manager


def _sanitize_process_network_env() -> None:
    for key in (
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
        "GIT_HTTP_PROXY",
        "GIT_HTTPS_PROXY",
    ):
        os.environ.pop(key, None)


def main() -> int:
    _sanitize_process_network_env()
    trace_path = reset_session_trace_log()
    startup_logger = get_logger("scanner_v2.startup")
    startup_logger.info("session log reset | path=%s | mode=full_text", trace_path)
    if full_session_log_enabled():
        startup_logger.info("main session log enabled | path=%s", full_session_log_path())
    startup_logger.info("runtime events log enabled | path=%s", events_log_path())
    if scanner_v2_log_enabled():
        startup_logger.info("scanner v2 log enabled | path=%s", scanner_v2_log_path())

    app = QApplication(sys.argv)
    app.setApplicationName("Spread Sniper Scanner V2")
    app_font = QFont("Segoe UI")
    app_font.setPointSize(9)
    app.setFont(app_font)
    install_global_line_edit_blur(app)
    install_global_click_affordance(app)
    get_theme_manager().set_theme("dark")
    window = FuturesSpreadScannerV2Window()
    window.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
