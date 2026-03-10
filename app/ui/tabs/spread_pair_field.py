from __future__ import annotations

from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QFocusEvent, QMouseEvent
from PySide6.QtWidgets import QLineEdit


class PairLineEdit(QLineEdit):
    field_focused = Signal()
    field_clicked = Signal()
    field_blurred = Signal()

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self._locked_selection = False

    def mark_selected_value(self) -> None:
        self._locked_selection = True

    def clear_selected_value_lock(self) -> None:
        self._locked_selection = False

    def has_locked_selection(self) -> bool:
        return self._locked_selection

    def focusInEvent(self, event: QFocusEvent) -> None:
        self.setReadOnly(False)
        super().focusInEvent(event)
        self.field_focused.emit()

    def mousePressEvent(self, event: QMouseEvent) -> None:
        super().mousePressEvent(event)
        self.field_clicked.emit()

    def focusOutEvent(self, event: QFocusEvent) -> None:
        super().focusOutEvent(event)
        self.setReadOnly(True)
        self.field_blurred.emit()

    def keyPressEvent(self, event) -> None:
        if self._locked_selection:
            key = event.key()
            if event.text() or key in (Qt.Key.Key_Backspace, Qt.Key.Key_Delete):
                self.clear()
                self._locked_selection = False
        super().keyPressEvent(event)
