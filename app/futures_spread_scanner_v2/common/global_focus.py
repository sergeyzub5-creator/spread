from __future__ import annotations

from PySide6.QtCore import QEvent, QObject, Qt
from PySide6.QtWidgets import QAbstractButton, QApplication, QLineEdit, QTabBar, QWidget


class _GlobalLineEditBlurFilter(QObject):
    def eventFilter(self, watched: QObject, event: QEvent) -> bool:
        if event.type() != QEvent.Type.MouseButtonPress:
            return False
        app = QApplication.instance()
        if app is None:
            return False
        focus_widget = app.focusWidget()
        if not isinstance(focus_widget, QLineEdit):
            return False
        try:
            button = event.button()  # type: ignore[attr-defined]
        except Exception:
            button = None
        if button != Qt.MouseButton.LeftButton:
            return False
        try:
            global_pos = event.globalPosition().toPoint()  # type: ignore[attr-defined]
        except Exception:
            global_pos = None
        target = app.widgetAt(global_pos) if global_pos is not None else None
        if _is_same_or_child_widget(focus_widget, target):
            return False
        focus_widget.clearFocus()
        return False


class _GlobalClickableAffordanceFilter(QObject):
    def eventFilter(self, watched: QObject, event: QEvent) -> bool:
        if isinstance(watched, QTabBar):
            return self._handle_tab_bar(watched, event)
        if isinstance(watched, QWidget) and _is_clickable_widget(watched):
            return self._handle_clickable_widget(watched, event)
        return False

    def _handle_tab_bar(self, tab_bar: QTabBar, event: QEvent) -> bool:
        _ = tab_bar
        _ = event
        return False

    def _handle_clickable_widget(self, widget: QWidget, event: QEvent) -> bool:
        if event.type() == QEvent.Type.Enter:
            if not bool(widget.property("appHover")):
                widget.setProperty("appHover", True)
                widget.style().unpolish(widget)
                widget.style().polish(widget)
                widget.update()
        elif event.type() == QEvent.Type.Leave:
            if bool(widget.property("appHover")):
                widget.setProperty("appHover", False)
                widget.style().unpolish(widget)
                widget.style().polish(widget)
                widget.update()
        elif event.type() == QEvent.Type.MouseButtonPress:
            try:
                button = event.button()  # type: ignore[attr-defined]
            except Exception:
                button = None
            if button == Qt.MouseButton.LeftButton:
                widget.setProperty("appClickActive", True)
                widget.style().unpolish(widget)
                widget.style().polish(widget)
                widget.update()
        elif event.type() in {QEvent.Type.MouseButtonRelease, QEvent.Type.Hide, QEvent.Type.EnabledChange}:
            if bool(widget.property("appClickActive")):
                widget.setProperty("appClickActive", False)
                widget.style().unpolish(widget)
                widget.style().polish(widget)
                widget.update()
        return False


def _is_same_or_child_widget(parent: QWidget, child: QWidget | None) -> bool:
    current = child
    while current is not None:
        if current is parent:
            return True
        current = current.parentWidget()
    return False


def _is_clickable_widget(widget: QWidget) -> bool:
    if bool(widget.property("appDisableClickAffordance")):
        return False
    if isinstance(widget, QAbstractButton):
        return True
    return hasattr(widget, "clicked")


def install_global_line_edit_blur(app: QApplication) -> None:
    existing = app.property("_global_line_edit_blur_filter")
    if isinstance(existing, QObject):
        return
    blur_filter = _GlobalLineEditBlurFilter(app)
    app.installEventFilter(blur_filter)
    app.setProperty("_global_line_edit_blur_filter", blur_filter)


def install_global_click_affordance(app: QApplication) -> None:
    existing = app.property("_global_click_affordance_filter")
    if isinstance(existing, QObject):
        return
    affordance_filter = _GlobalClickableAffordanceFilter(app)
    app.installEventFilter(affordance_filter)
    app.setProperty("_global_click_affordance_filter", affordance_filter)
