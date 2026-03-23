from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QColor, QFont, QMouseEvent, QPainter, QPen, QPixmap
from PySide6.QtWidgets import QLabel, QLineEdit

from app.futures_spread_scanner_v2.runtime.contracts import PerpRowState
from app.futures_spread_scanner_v2.common.theme import THEMES

_SOFT_RADIUS = 16
_SOFT_RADIUS_SM = 12
_EDIT_MIN_HEIGHT = 14

_RUNTIME_WIDGET_ROW_HEIGHT = 52
_RUNTIME_WIDGET_SEPARATOR_ROW_HEIGHT = 30


def _base_exchange_split_x(total_width: int) -> int:
    return max(84, (int(total_width) + 1) // 2)


def _runtime_palette() -> dict[str, str]:
    return THEMES["dark"]


def _runtime_widget_stylesheet() -> str:
    d = _runtime_palette()
    border = d["border"]
    surface = d["surface"]
    surface_alt = d["surface_alt"]
    fg = d["text_primary"]
    muted = d["text_muted"]
    accent = d["accent"]
    accent_bg = d["accent_bg"]
    accent_bg_hover = d["accent_bg_hover"]
    return f"""
        QWidget {{ background: transparent; color: {fg}; }}
        QFrame#scannerVolumeRow {{ background-color: {surface_alt}; border: 1px solid {border}; border-radius: {_SOFT_RADIUS}px; }}
        QFrame#scannerVolumeLabelCapsule {{ background-color: {surface}; border: 1px solid {border}; border-radius: 999px; }}
        QLabel#scannerVolumeLabel {{ color: {fg}; font-weight: 700; font-size: 11px; padding: 0; background: transparent; }}
        QLabel#scannerPairsStatusLabel {{ color: {muted}; font-weight: 600; font-size: 11px; padding: 0 4px; background: transparent; }}
        QLineEdit#scannerVolumeEdit {{ background-color: {surface}; color: {fg}; border: 1px solid {border}; border-radius: {_SOFT_RADIUS_SM}px; padding: 4px 10px; font-weight: 600; font-size: 11px; min-height: {_EDIT_MIN_HEIGHT}px; }}
        QLineEdit#scannerVolumeEdit:focus {{ border: 1px solid {accent}; }}
        QLineEdit#scannerVolumeEdit::placeholder {{ color: {muted}; }}
        QPushButton#scannerVolumeApply {{ background-color: {accent_bg}; color: {fg}; border: 1px solid {accent}; border-radius: {_SOFT_RADIUS_SM}px; padding: 4px 12px; font-weight: 700; }}
        QPushButton#scannerVolumeApply:hover, QPushButton#scannerVolumeApply:pressed {{ background-color: {accent_bg_hover}; border: 1px solid {accent}; }}
        QPushButton#scannerAddNotificationBtn {{ background-color: {surface}; color: {fg}; border: 1px solid {border}; border-radius: {_SOFT_RADIUS_SM}px; padding: 4px 12px; font-weight: 700; }}
        QPushButton#scannerAddNotificationBtn:hover, QPushButton#scannerAddNotificationBtn:pressed {{ background-color: {surface_alt}; border: 1px solid {border}; }}
        QToolButton#scannerVolumeSettingsBtn {{ padding: 6px 10px; border-radius: {_SOFT_RADIUS_SM}px; color: {fg}; background: transparent; border: 1px solid transparent; }}
        QToolButton#scannerVolumeSettingsBtn:hover {{ background-color: {surface}; border: 1px solid {border}; }}
        QToolButton#scannerVolumeSettingsBtn:pressed {{ background-color: {surface_alt}; border: 1px solid {accent}; }}
        QFrame#scannerTableBlock {{ border: 2px solid {border}; border-radius: {_SOFT_RADIUS}px; background-color: {surface}; }}
        QFrame#scannerTableBlock[scanner_block="exchange"] {{ background-color: {QColor(surface_alt).darker(125).name()}; }}
        QLabel#scannerTableBlockTitle {{ color: {fg}; font-weight: 700; font-size: 11px; padding: 8px 10px; background: transparent; border: 1px solid transparent; border-top-left-radius: {_SOFT_RADIUS - 1}px; border-top-right-radius: {_SOFT_RADIUS - 1}px; }}
        QLabel#scannerTableBlockTitle[scanner_block="exchange"] {{ color: {muted}; }}
        QFrame#scannerPairSearchRow {{ background-color: {surface_alt}; border-top: 2px solid {border}; border-bottom: 2px solid {border}; border-left: none; border-right: none; }}
        QLineEdit#scannerPairSearchEdit {{ background-color: {surface}; color: {fg}; border: 1px solid {border}; border-radius: {_SOFT_RADIUS_SM}px; padding: 4px 28px 4px 10px; margin: 0; font-weight: 600; font-size: 11px; min-height: 18px; }}
        QLineEdit#scannerPairSearchEdit:focus {{ border: 1px solid {accent}; }}
        QFrame#experimentalExchangeSubHeader {{ background: #161d27; border-top: 1px solid #3d4e63; border-bottom: 1px solid #3d4e63; }}
        QLabel#experimentalExchangeSubHeaderLabel {{ color: #f1f5fb; font-weight: 700; background: transparent; border: none; }}
    """


@dataclass(slots=True)
class StarterRowViewModel:
    kind: str
    canonical: str = ""
    bookmarked: bool = False


BaseExchangeRowViewModel = PerpRowState


class _ClickableHeaderLabel(QLabel):
    clicked = Signal()

    def mousePressEvent(self, event) -> None:  # type: ignore[override]
        if event.button() == Qt.MouseButton.LeftButton:
            self.clicked.emit()
            event.accept()
            return
        super().mousePressEvent(event)


def _set_font_point_size_safe(font: QFont, point_size: int) -> None:
    if point_size > 0:
        font.setPointSize(point_size)


class _PairSearchEdit(QLineEdit):
    def __init__(self, parent=None) -> None:
        super().__init__(parent)


class _ExchangeTitleLabel(QLabel):
    clicked = Signal(str)

    def __init__(self, text: str = "", parent=None) -> None:
        super().__init__(text, parent)
        self._status_color = QColor(239, 68, 68)
        self._logo_pixmap: QPixmap | None = None
        self._logo_size = 15

    def set_exchange_logo(self, exchange_id: str) -> None:
        normalized = str(exchange_id or "").strip().lower()
        logo_path = Path(__file__).resolve().parents[3] / "ui" / "assets" / "logos" / "exchanges" / f"{normalized}.png"
        if logo_path.exists():
            pixmap = QPixmap(str(logo_path))
            if not pixmap.isNull():
                self._logo_pixmap = pixmap
                self.update()
                return
        self._logo_pixmap = None
        self.update()

    def set_status_ok(self, ok: bool) -> None:
        self._status_color = QColor(34, 197, 94) if ok else QColor(239, 68, 68)
        self.update()

    def mousePressEvent(self, event: QMouseEvent) -> None:  # type: ignore[override]
        exchange_id = str(self.property("exchange_id") or "")
        if exchange_id:
            self.clicked.emit(exchange_id)
        super().mousePressEvent(event)

    def paintEvent(self, event) -> None:  # type: ignore[override]
        super().paintEvent(event)
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        text = self.text() or ""
        font_metrics = self.fontMetrics()
        text_width = font_metrics.horizontalAdvance(text)
        logo_width = self._logo_size if self._logo_pixmap is not None else 0
        logo_gap = 8 if logo_width else 0
        total_width = logo_width + logo_gap + text_width
        left = max(4, int((self.width() - total_width) / 2) - 4)
        if self._logo_pixmap is not None:
            scaled = self._logo_pixmap.scaled(
                self._logo_size,
                self._logo_size,
                Qt.AspectRatioMode.KeepAspectRatio,
                Qt.TransformationMode.SmoothTransformation,
            )
            pix_y = max(0, int((self.height() - scaled.height()) / 2))
            painter.drawPixmap(left, pix_y, scaled)
        painter.setPen(Qt.PenStyle.NoPen)
        painter.setBrush(self._status_color)
        radius = 4
        x = min(self.width() - 12, max((self.width() // 2) + (text_width // 2) + 6, 12))
        y = max(8, (self.height() // 2) - radius)
        painter.drawEllipse(x, y, radius * 2, radius * 2)


__all__ = [
    "BaseExchangeRowViewModel",
    "StarterRowViewModel",
    "_ClickableHeaderLabel",
    "_ExchangeTitleLabel",
    "_RUNTIME_WIDGET_ROW_HEIGHT",
    "_RUNTIME_WIDGET_SEPARATOR_ROW_HEIGHT",
    "_EDIT_MIN_HEIGHT",
    "_SOFT_RADIUS",
    "_SOFT_RADIUS_SM",
    "_PairSearchEdit",
    "_base_exchange_split_x",
    "_runtime_palette",
    "_runtime_widget_stylesheet",
    "_set_font_point_size_safe",
]
