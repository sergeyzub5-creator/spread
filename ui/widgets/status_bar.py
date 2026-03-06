from __future__ import annotations

import socket
import threading
from datetime import datetime

from PySide6.QtCore import QObject, QRectF, QTimer, Qt, Signal
from PySide6.QtGui import QColor, QPainter, QPen
from PySide6.QtWidgets import QFrame, QHBoxLayout, QLabel, QWidget

from ui.i18n import tr
from ui.theme import theme_color


class WifiIndicator(QWidget):
    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self._main_color = QColor(theme_color("net_idle"))
        self._accent_color = QColor(theme_color("net_idle_border"))
        self.setFixedSize(16, 16)

    def set_colors(self, main_color: str, accent_color: str) -> None:
        self._main_color = QColor(main_color)
        self._accent_color = QColor(accent_color)
        self.update()

    def paintEvent(self, event) -> None:
        super().paintEvent(event)
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)

        center_x = self.width() / 2.0
        base_y = self.height() + 1.0

        for radius in (3.0, 5.5, 7.5):
            rect = QRectF(center_x - radius, base_y - radius * 2.0, radius * 2.0, radius * 2.0)
            pen = QPen(self._main_color, 1.5, Qt.PenStyle.SolidLine, Qt.PenCapStyle.RoundCap)
            painter.setPen(pen)
            painter.setBrush(Qt.BrushStyle.NoBrush)
            painter.drawArc(rect, 0, 180 * 16)

        painter.setPen(Qt.PenStyle.NoPen)
        painter.setBrush(self._accent_color)
        painter.drawEllipse(int(center_x - 1.6), int(base_y - 2.4), 3, 3)


class _NetworkWorker(QObject):
    finished = Signal(bool)

    @staticmethod
    def check_online() -> bool:
        try:
            with socket.create_connection(("8.8.8.8", 53), timeout=1.0):
                return True
        except OSError:
            return False


class NetworkStatusBar(QFrame):
    network_checked = Signal(bool)

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self._online: bool | None = None
        self._check_in_progress = False
        self._recording_active = False
        self._build_ui()
        self._error_hide_timer = QTimer(self)
        self._error_hide_timer.setSingleShot(True)
        self._error_hide_timer.timeout.connect(self.error_label.hide)
        self.network_checked.connect(self._on_network_result)

        self.setFrameStyle(QFrame.Shape.NoFrame)

        self.clock_timer = QTimer(self)
        self.clock_timer.timeout.connect(self._update_clock)
        self.clock_timer.start(1000)
        self._update_clock()

        self.network_timer = QTimer(self)
        self.network_timer.timeout.connect(self._trigger_network_check)
        self.network_timer.start(5000)

        self.apply_theme()
        self._trigger_network_check()
        self.set_recording_state(False)

    def _build_ui(self) -> None:
        layout = QHBoxLayout(self)
        layout.setContentsMargins(10, 4, 10, 4)
        layout.setSpacing(8)

        self.network_capsule = QFrame()
        self.network_capsule.setObjectName("networkCapsule")
        self.network_capsule.setFixedWidth(44)
        self.network_capsule.setFixedHeight(34)
        network_layout = QHBoxLayout(self.network_capsule)
        network_layout.setContentsMargins(0, 0, 0, 0)
        network_layout.setSpacing(0)
        self.net_indicator = WifiIndicator()
        network_layout.addWidget(self.net_indicator, 0, Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(self.network_capsule)

        layout.addStretch()

        self.time_label = QLabel()
        self.time_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.time_label.setMinimumWidth(148)
        self.time_label.setFixedHeight(34)
        layout.addWidget(self.time_label)

        self.error_label = QLabel()
        self.error_label.hide()
        layout.addWidget(self.error_label)

    def stop_background_tasks(self) -> None:
        self.clock_timer.stop()
        self.network_timer.stop()

    def _update_clock(self) -> None:
        self.time_label.setText(datetime.now().strftime("%H:%M:%S"))

    def _trigger_network_check(self) -> None:
        if self._check_in_progress:
            return
        self._check_in_progress = True

        def _run_check() -> None:
            online = _NetworkWorker.check_online()
            self.network_checked.emit(online)

        threading.Thread(target=_run_check, name="network-status-check", daemon=True).start()

    def _on_network_result(self, online: bool) -> None:
        self._check_in_progress = False
        online = bool(online)
        if self._online is online:
            return
        self._online = online
        self._apply_network_state()

    def _apply_network_state(self) -> None:
        if self._online is None:
            self.net_indicator.set_colors(theme_color("net_idle"), theme_color("net_idle_border"))
            self.network_capsule.setToolTip(tr("status.network.checking"))
            return
        if self._online:
            self.net_indicator.set_colors(theme_color("net_online"), theme_color("net_online_border"))
            self.network_capsule.setToolTip(tr("status.network.online"))
            return
        self.net_indicator.set_colors(theme_color("net_offline"), theme_color("net_offline_border"))
        self.network_capsule.setToolTip(tr("status.network.offline"))

    def set_recording_state(self, active: bool, tooltip: str = "") -> None:
        self._recording_active = bool(active)
        del tooltip

    def apply_theme(self) -> None:
        self.setStyleSheet(
            f"""
            QFrame {{
                background-color: {theme_color('window_bg')};
                border-top: 1px solid {theme_color('border')};
                padding: 4px 10px;
            }}
            QLabel {{
                color: {theme_color('text_muted')};
                font-size: 13px;
            }}
            """
        )
        self.time_label.setStyleSheet(
            f"""
            QLabel {{
                color: {theme_color('text_primary')};
                background: qlineargradient(
                    x1: 0, y1: 0, x2: 0, y2: 1,
                    stop: 0 {theme_color('surface')},
                    stop: 1 {theme_color('surface_alt')}
                );
                border: 1px solid {theme_color('border')};
                border-radius: 10px;
                font-size: 20px;
                font-weight: 800;
                font-family: "Segoe UI", "Consolas", monospace;
                letter-spacing: 1px;
                padding: 1px 12px;
            }}
            """
        )
        self.network_capsule.setStyleSheet(
            f"""
            QFrame#networkCapsule {{
                background: qlineargradient(
                    x1: 0, y1: 0, x2: 0, y2: 1,
                    stop: 0 {theme_color('surface')},
                    stop: 1 {theme_color('surface_alt')}
                );
                border: 1px solid {theme_color('border')};
                border-radius: 10px;
            }}
            """
        )
        self.error_label.setStyleSheet(f"color: {theme_color('danger')};")
        self._apply_network_state()
        self.set_recording_state(self._recording_active)

    def retranslate_ui(self) -> None:
        self._apply_network_state()
        self.set_recording_state(self._recording_active)

    def _show_notice(self, message: str, color: str, timeout_ms: int) -> None:
        self.error_label.setStyleSheet(f"color: {color};")
        self.error_label.setText(message)
        self.error_label.show()
        self._error_hide_timer.start(max(0, int(timeout_ms)))

    def show_message(self, message: str, timeout_ms: int = 2400) -> None:
        self._show_notice(str(message or ""), theme_color("text_muted"), timeout_ms)

    def show_error(self, message: str, timeout_ms: int = 5000) -> None:
        self._show_notice(tr("status.warning", message=message), theme_color("danger"), timeout_ms)
