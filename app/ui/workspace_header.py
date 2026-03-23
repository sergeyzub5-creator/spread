from __future__ import annotations

from pathlib import Path

from PySide6.QtCore import QPoint, QSize, Qt, QTimer
from PySide6.QtGui import QColor, QFocusEvent, QFont, QIcon, QMouseEvent, QPainter, QPen, QPixmap
from PySide6.QtWidgets import QFrame, QHBoxLayout, QLabel, QLineEdit, QPushButton, QToolButton

_ROW_PAD = (12, 10, 12, 10)
_INNER_SPACING = 10
_EDIT_MIN_WIDTH = 98
_EDIT_MIN_HEIGHT = 14


def _header_font(*, point_size: int = 9, bold: bool = False) -> QFont:
    font = QFont("Segoe UI")
    font.setPointSize(max(1, int(point_size)))
    font.setBold(bool(bold))
    return font


def _build_gear_icon() -> QIcon:
    pixmap = QPixmap(14, 14)
    pixmap.fill(Qt.GlobalColor.transparent)
    painter = QPainter(pixmap)
    painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
    pen = QPen(QColor("#d8e1ea"), 1.4, Qt.PenStyle.SolidLine, Qt.PenCapStyle.RoundCap)
    painter.setPen(pen)
    painter.setBrush(Qt.BrushStyle.NoBrush)
    center = QPoint(7, 7)
    for angle in range(0, 360, 45):
        painter.save()
        painter.translate(center)
        painter.rotate(angle)
        painter.drawLine(0, -5, 0, -3)
        painter.restore()
    painter.drawEllipse(center, 3, 3)
    painter.drawEllipse(center, 1, 1)
    painter.end()
    return QIcon(pixmap)


class _VolumeFilterEdit(QLineEdit):
    def __init__(self, host=None, parent=None) -> None:
        super().__init__(parent)
        self._host = host

    def _call_host(self, method_name: str) -> None:
        host = self._host
        if host is None:
            return
        method = getattr(host, method_name, None)
        if callable(method):
            method()

    def mousePressEvent(self, event: QMouseEvent) -> None:
        if self.isReadOnly():
            self.setReadOnly(False)
            self._call_host("_apply_editable_style")
            self.setFocus()
            self.selectAll()
        super().mousePressEvent(event)

    def focusInEvent(self, event: QFocusEvent) -> None:
        if self.isReadOnly():
            self.setReadOnly(False)
            self._call_host("_apply_editable_style")
        super().focusInEvent(event)

    def focusOutEvent(self, event: QFocusEvent) -> None:
        super().focusOutEvent(event)
        if not self.isReadOnly():
            QTimer.singleShot(0, lambda: self._call_host("_on_volume_edit_focus_out"))


class WorkspaceHeaderBar(QFrame):
    def __init__(self, parent=None, edit_host=None) -> None:
        super().__init__(parent)
        self.setFont(_header_font())
        self.setObjectName("scannerVolumeRow")
        layout = QHBoxLayout(self)
        layout.setContentsMargins(*_ROW_PAD)
        layout.setSpacing(_INNER_SPACING)

        self.label = QLabel()
        self.label.setFont(_header_font(bold=True))
        self.label.setObjectName("scannerVolumeLabel")

        self.label_capsule = QFrame()
        self.label_capsule.setObjectName("scannerVolumeLabelCapsule")
        capsule_layout = QHBoxLayout(self.label_capsule)
        capsule_layout.setContentsMargins(14, 8, 14, 8)
        capsule_layout.setSpacing(0)
        capsule_layout.addWidget(self.label, 0)

        self.edit = _VolumeFilterEdit(edit_host if edit_host is not None else parent, self)
        self.edit.setFont(_header_font())
        self.edit.setObjectName("scannerVolumeEdit")
        self.edit.setFocusPolicy(Qt.FocusPolicy.ClickFocus)
        self.edit.setMinimumWidth(_EDIT_MIN_WIDTH)
        self.edit.setMinimumHeight(_EDIT_MIN_HEIGHT)
        self.edit.setClearButtonEnabled(False)

        self.apply_btn = QPushButton()
        self.apply_btn.setFont(_header_font(bold=True))
        self.apply_btn.setObjectName("scannerVolumeApply")
        self.apply_btn.setMinimumHeight(_EDIT_MIN_HEIGHT)

        self.add_notification_btn = QPushButton()
        self.add_notification_btn.setFont(_header_font(bold=True))
        self.add_notification_btn.setObjectName("scannerAddNotificationBtn")
        self.add_notification_btn.setMinimumHeight(_EDIT_MIN_HEIGHT)
        alarm_icon_path = Path(__file__).resolve().parent / "assets" / "logos" / "alarm.svg"
        self.add_notification_btn.setIcon(QIcon(str(alarm_icon_path)))
        self.add_notification_btn.setIconSize(QSize(16, 16))
        self.add_notification_btn.setLayoutDirection(Qt.LayoutDirection.RightToLeft)

        self.pairs_status_label = QLabel()
        self.pairs_status_label.setFont(_header_font())
        self.pairs_status_label.setObjectName("scannerPairsStatusLabel")

        self.settings_btn = QToolButton()
        self.settings_btn.setFont(_header_font())
        self.settings_btn.setObjectName("scannerVolumeSettingsBtn")
        self.settings_btn.setAutoRaise(True)
        self.settings_btn.setText("")
        self.settings_btn.setIcon(_build_gear_icon())
        self.settings_btn.setIconSize(QSize(14, 14))

        layout.addWidget(self.label_capsule, 0)
        layout.addWidget(self.edit, 0)
        layout.addWidget(self.apply_btn, 0)
        layout.addWidget(self.add_notification_btn, 0)
        layout.addStretch(1)
        layout.addWidget(self.pairs_status_label, 0)
        layout.addWidget(self.settings_btn, 0)
