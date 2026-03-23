from __future__ import annotations

from PySide6.QtCore import QPointF, QRectF, QSize, Qt
from PySide6.QtGui import QColor, QIcon, QLinearGradient, QPainter, QPen, QPixmap
from PySide6.QtSvg import QSvgRenderer
from PySide6.QtWidgets import QSizePolicy, QWidget


def build_logo_svg(size: int, glow_a: str = "#19B8FF", glow_b: str = "#00E0B8") -> str:
    return f"""
<svg xmlns="http://www.w3.org/2000/svg" width="{size}" height="{size}" viewBox="0 0 100 100">
  <defs>
    <linearGradient id="g" x1="0" y1="0" x2="0" y2="1">
      <stop offset="0%" stop-color="{glow_a}"/>
      <stop offset="100%" stop-color="{glow_b}"/>
    </linearGradient>
  </defs>
  <path d="M22 30 C22 22 29 16 38 16 H74 C79 16 83 20 83 25 C83 30 79 34 74 34 H45 C38 34 34 37 34 42 C34 47 38 50 45 50 H62 C74 50 82 56 82 67 C82 78 73 84 62 84 H26 C21 84 17 80 17 75 C17 70 21 66 26 66 H58 C65 66 69 63 69 58 C69 53 65 50 58 50 H41 C29 50 22 43 22 30 Z" fill="url(#g)"/>
</svg>
"""


def render_logo_pixmap(size: int, glow_a: str = "#19B8FF", glow_b: str = "#00E0B8") -> QPixmap:
    renderer = QSvgRenderer(build_logo_svg(size * 2, glow_a, glow_b).encode("utf-8"))
    source = QPixmap(size * 2, size * 2)
    source.fill(Qt.GlobalColor.transparent)
    painter = QPainter(source)
    painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
    renderer.render(painter)
    painter.end()
    return source.scaled(size, size, Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation)


def build_app_icon() -> QIcon:
    icon = QIcon()
    for size in (16, 24, 32, 48, 64, 128, 256):
        icon.addPixmap(render_logo_pixmap(size))
    return icon


class NeonHeader(QWidget):
    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self._logo_size = 48
        self._line_y = 34
        self._show_lines = True
        self._glow_a = "#19B8FF"
        self._glow_b = "#00E0B8"
        self._logo_px = QPixmap()
        self.setFixedHeight(52)
        self.setMinimumWidth(520)
        self.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Fixed)
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        self._rebuild_logo()

    def set_palette(self, glow_a: str, glow_b: str) -> None:
        self._glow_a = glow_a
        self._glow_b = glow_b
        self._rebuild_logo()
        self.update()

    def _rebuild_logo(self) -> None:
        self._logo_px = render_logo_pixmap(self._logo_size, self._glow_a, self._glow_b)

    def sizeHint(self) -> QSize:
        return QSize(620, 52)

    def paintEvent(self, _event) -> None:
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        painter.setRenderHint(QPainter.RenderHint.SmoothPixmapTransform, True)
        center_x = self.width() / 2.0

        if self._show_lines:
            y = float(self._line_y)
            left_x1 = 12.0
            left_x2 = center_x - 22.0
            right_x1 = center_x + 22.0
            right_x2 = float(self.width()) - 12.0

            left_glow = QLinearGradient(left_x1, y, left_x2, y)
            left_glow.setColorAt(0.0, QColor(0, 214, 255, 0))
            left_glow.setColorAt(1.0, QColor(self._glow_a))
            painter.setPen(QPen(left_glow, 2, Qt.PenStyle.SolidLine, Qt.PenCapStyle.RoundCap))
            painter.drawLine(int(left_x1), int(y), int(left_x2), int(y))

            right_glow = QLinearGradient(right_x1, y, right_x2, y)
            right_glow.setColorAt(0.0, QColor(self._glow_b))
            right_glow.setColorAt(1.0, QColor(0, 255, 198, 0))
            painter.setPen(QPen(right_glow, 2, Qt.PenStyle.SolidLine, Qt.PenCapStyle.RoundCap))
            painter.drawLine(int(right_x1), int(y), int(right_x2), int(y))

        logo_rect = QRectF(0, 0, self._logo_size, self._logo_size)
        logo_rect.moveCenter(QPointF(center_x, self._logo_size / 2.0 + 2))
        painter.drawPixmap(logo_rect.toRect(), self._logo_px)

