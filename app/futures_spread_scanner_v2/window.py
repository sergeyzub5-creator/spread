from __future__ import annotations

from PySide6.QtGui import QFont
from PySide6.QtWidgets import QMainWindow

from app.futures_spread_scanner_v2.common.logger import get_logger
from app.futures_spread_scanner_v2.workspace_tabs import FuturesSpreadWorkspaceTabs
from app.futures_spread_scanner_v2.common.i18n import get_language_manager, tr
from app.futures_spread_scanner_v2.common.theme import build_app_stylesheet, get_theme_manager
from app.futures_spread_scanner_v2.common.brand_header import build_app_icon


class FuturesSpreadScannerV2Window(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self._logger = get_logger("scanner.v2.window")
        base_font = QFont("Segoe UI")
        base_font.setPointSize(9)
        self.setFont(base_font)
        self.setWindowTitle(tr("scanner.window_title"))
        self.setWindowIcon(build_app_icon())
        self.resize(1100, 720)

        self.language_manager = get_language_manager()
        self.language_manager.language_changed.connect(self._retranslate_ui)
        self.theme_manager = get_theme_manager()
        self.theme_manager.theme_changed.connect(self._apply_theme)

        self.workspace_tabs = FuturesSpreadWorkspaceTabs(self)
        self.setCentralWidget(self.workspace_tabs)

        self._apply_theme()
        self._retranslate_ui()
        self._logger.info("window init complete")

    def _retranslate_ui(self, _lang: str | None = None) -> None:
        self.setWindowTitle(tr("scanner.window_title"))
        self.workspace_tabs.retranslate_ui()
        self._logger.info("window retranslate")

    def _apply_theme(self) -> None:
        self.setStyleSheet(build_app_stylesheet())
        self.workspace_tabs.apply_theme()
        self._logger.info("window theme applied")

    def closeEvent(self, event) -> None:  # type: ignore[override]
        self._logger.info("window close")
        try:
            self.workspace_tabs.close()
        except Exception:
            pass
        super().closeEvent(event)


__all__ = ["FuturesSpreadScannerV2Window"]
