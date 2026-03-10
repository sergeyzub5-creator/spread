from __future__ import annotations

from math import cos, pi

from PySide6.QtCore import QEasingCurve, QPropertyAnimation, QRectF, Qt, QTimer, QVariantAnimation, Signal
from PySide6.QtGui import QPainter
from PySide6.QtWidgets import QApplication, QLabel, QVBoxLayout, QWidget

from app.ui.i18n import tr
from app.ui.widgets.brand_header import render_logo_pixmap


class PulseLogoWidget(QWidget):
    def __init__(self, base_logo_size: int = 92, parent=None) -> None:
        super().__init__(parent)
        self._base_logo_size = max(40, int(base_logo_size))
        self._scale = 1.0
        self._logo_px = render_logo_pixmap(self._base_logo_size * 2)
        self.setFixedHeight(126)

    def set_scale(self, scale: float) -> None:
        clamped = max(0.90, min(1.20, float(scale)))
        if abs(clamped - self._scale) < 0.001:
            return
        self._scale = clamped
        self.update()

    def paintEvent(self, event) -> None:
        super().paintEvent(event)
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        painter.setRenderHint(QPainter.RenderHint.SmoothPixmapTransform, True)

        width = self._base_logo_size * self._scale
        height = self._base_logo_size * self._scale
        target = QRectF((self.width() - width) / 2.0, (self.height() - height) / 2.0, width, height)
        source = QRectF(0.0, 0.0, float(self._logo_px.width()), float(self._logo_px.height()))
        painter.drawPixmap(target, self._logo_px, source)


class StartupSplash(QWidget):
    finished = Signal()

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self._base_logo_size = 92

        self.setWindowFlags(
            Qt.WindowType.FramelessWindowHint
            | Qt.WindowType.SplashScreen
            | Qt.WindowType.WindowStaysOnTopHint
        )
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground, True)
        self.setFixedSize(520, 300)
        self.setWindowOpacity(0.0)
        self._finishing = False

        self._build_ui(tr("splash.title"), tr("splash.loading"))
        self._build_animations()

    def _build_ui(self, title: str, subtitle: str) -> None:
        self.setStyleSheet(
            """
            QWidget#SplashRoot {
                background-color: #0b1220;
                border: 1px solid #1e293b;
                border-radius: 14px;
            }
            QLabel#SplashTitle {
                color: #e2e8f0;
                font-size: 26px;
                font-weight: 700;
                letter-spacing: 0.3px;
            }
            QLabel#SplashSubtitle {
                color: #94a3b8;
                font-size: 13px;
            }
            """
        )

        root_layout = QVBoxLayout(self)
        root_layout.setContentsMargins(10, 10, 10, 10)

        self.root = QWidget()
        self.root.setObjectName("SplashRoot")
        root_layout.addWidget(self.root)

        layout = QVBoxLayout(self.root)
        layout.setContentsMargins(22, 18, 22, 18)
        layout.setSpacing(8)
        layout.addStretch()

        self.logo_widget = PulseLogoWidget(base_logo_size=self._base_logo_size)
        layout.addWidget(self.logo_widget)

        self.title_label = QLabel(title)
        self.title_label.setObjectName("SplashTitle")
        self.title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(self.title_label)

        self.subtitle_label = QLabel(subtitle)
        self.subtitle_label.setObjectName("SplashSubtitle")
        self.subtitle_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(self.subtitle_label)

        layout.addStretch()

    def _build_animations(self) -> None:
        self._pulse_anim = QVariantAnimation(self)
        self._pulse_anim.setStartValue(0.0)
        self._pulse_anim.setEndValue(1.0)
        self._pulse_anim.setDuration(1700)
        self._pulse_anim.setEasingCurve(QEasingCurve.Type.Linear)
        self._pulse_anim.setLoopCount(-1)
        self._pulse_anim.valueChanged.connect(self._on_pulse)

        self._fade_in = QPropertyAnimation(self, b"windowOpacity", self)
        self._fade_in.setDuration(260)
        self._fade_in.setStartValue(0.0)
        self._fade_in.setEndValue(1.0)
        self._fade_in.setEasingCurve(QEasingCurve.Type.OutCubic)

        self._fade_out = QPropertyAnimation(self, b"windowOpacity", self)
        self._fade_out.setDuration(260)
        self._fade_out.setStartValue(1.0)
        self._fade_out.setEndValue(0.0)
        self._fade_out.setEasingCurve(QEasingCurve.Type.InCubic)
        self._fade_out.finished.connect(self._on_done)

    def _on_pulse(self, value) -> None:
        t = float(value)
        wave = 0.5 - 0.5 * cos(2.0 * pi * t)
        self.logo_widget.set_scale(1.0 + (0.15 * wave))

    def _center_on_screen(self) -> None:
        screen = QApplication.primaryScreen()
        if screen is None:
            return
        rect = screen.availableGeometry()
        self.move(rect.center() - self.rect().center())

    def start(self) -> None:
        self._center_on_screen()
        self.setWindowOpacity(1.0)
        self.show()
        self.raise_()
        self._pulse_anim.start()

    def finish(self) -> None:
        if self._finishing:
            return
        self._finishing = True
        self._pulse_anim.stop()
        self.close()
        self.finished.emit()

    def _on_done(self) -> None:
        self._pulse_anim.stop()
        self.close()
        self.finished.emit()


class ShutdownSplash(QWidget):
    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self._base_logo_size = 92

        self.setWindowFlags(
            Qt.WindowType.FramelessWindowHint
            | Qt.WindowType.SplashScreen
            | Qt.WindowType.WindowStaysOnTopHint
        )
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground, True)
        self.setFixedSize(520, 300)
        self.setWindowOpacity(0.0)

        self._build_ui(tr("splash.title"), tr("splash.closing"))
        self._build_animations()

    def _build_ui(self, title: str, subtitle: str) -> None:
        self.setStyleSheet(
            """
            QWidget#SplashRoot {
                background-color: #0b1220;
                border: 1px solid #1e293b;
                border-radius: 14px;
            }
            QLabel#SplashTitle {
                color: #e2e8f0;
                font-size: 26px;
                font-weight: 700;
                letter-spacing: 0.3px;
            }
            QLabel#SplashSubtitle {
                color: #94a3b8;
                font-size: 13px;
            }
            """
        )

        root_layout = QVBoxLayout(self)
        root_layout.setContentsMargins(10, 10, 10, 10)

        self.root = QWidget()
        self.root.setObjectName("SplashRoot")
        root_layout.addWidget(self.root)

        layout = QVBoxLayout(self.root)
        layout.setContentsMargins(22, 18, 22, 18)
        layout.setSpacing(8)
        layout.addStretch()

        self.logo_widget = PulseLogoWidget(base_logo_size=self._base_logo_size)
        layout.addWidget(self.logo_widget)

        self.title_label = QLabel(title)
        self.title_label.setObjectName("SplashTitle")
        self.title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(self.title_label)

        self.subtitle_label = QLabel(subtitle)
        self.subtitle_label.setObjectName("SplashSubtitle")
        self.subtitle_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(self.subtitle_label)

        layout.addStretch()

    def _build_animations(self) -> None:
        self._pulse_anim = QVariantAnimation(self)
        self._pulse_anim.setStartValue(0.0)
        self._pulse_anim.setEndValue(1.0)
        self._pulse_anim.setDuration(1700)
        self._pulse_anim.setEasingCurve(QEasingCurve.Type.Linear)
        self._pulse_anim.setLoopCount(-1)
        self._pulse_anim.valueChanged.connect(self._on_pulse)

        self._fade_in = QPropertyAnimation(self, b"windowOpacity", self)
        self._fade_in.setDuration(220)
        self._fade_in.setStartValue(0.0)
        self._fade_in.setEndValue(1.0)
        self._fade_in.setEasingCurve(QEasingCurve.Type.OutCubic)

    def _on_pulse(self, value) -> None:
        t = float(value)
        wave = 0.5 - 0.5 * cos(2.0 * pi * t)
        self.logo_widget.set_scale(1.0 + (0.15 * wave))

    def _center_on_screen(self) -> None:
        screen = QApplication.primaryScreen()
        if screen is None:
            return
        rect = screen.availableGeometry()
        self.move(rect.center() - self.rect().center())

    def start(self) -> None:
        self._center_on_screen()
        self.show()
        self.raise_()
        self._pulse_anim.start()
        self._fade_in.start()

    def finish(self, delay_ms: int = 420) -> None:
        def _close() -> None:
            self._pulse_anim.stop()
            self.close()

        QTimer.singleShot(max(0, int(delay_ms)), _close)

