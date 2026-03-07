from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QDialog, QHBoxLayout, QLabel, QPushButton, QVBoxLayout, QWidget

from app.ui.i18n import tr
from app.ui.theme import theme_color


class ConfirmDialog(QDialog):
    def __init__(self, *, title: str, message: str, confirm_text: str, parent=None) -> None:
        super().__init__(parent)
        self.setModal(True)
        self.setWindowTitle(title)
        self.setFixedSize(336, 144)

        root = QVBoxLayout(self)
        root.setContentsMargins(8, 8, 8, 8)
        root.setSpacing(0)

        shell = QWidget()
        shell.setObjectName("confirmShell")
        shell_layout = QVBoxLayout(shell)
        shell_layout.setContentsMargins(16, 16, 16, 14)
        shell_layout.setSpacing(8)

        eyebrow = QLabel("Удаление")
        eyebrow.setObjectName("confirmEyebrow")
        shell_layout.addWidget(eyebrow)

        title_label = QLabel(title)
        title_label.setObjectName("confirmTitle")
        title_label.setWordWrap(True)
        shell_layout.addWidget(title_label)

        message_label = QLabel(message)
        message_label.setObjectName("confirmMessage")
        message_label.setWordWrap(True)
        shell_layout.addWidget(message_label)
        shell_layout.addSpacing(2)

        actions = QHBoxLayout()
        actions.setContentsMargins(0, 6, 0, 0)
        actions.setSpacing(8)

        confirm_btn = QPushButton(confirm_text)
        confirm_btn.setObjectName("confirmAccept")
        confirm_btn.clicked.connect(self.accept)

        cancel_btn = QPushButton(tr("common.cancel"))
        cancel_btn.setObjectName("confirmCancel")
        cancel_btn.clicked.connect(self.reject)

        actions.addWidget(confirm_btn)
        actions.addWidget(cancel_btn)
        actions.addStretch(1)
        shell_layout.addLayout(actions)

        root.addWidget(shell)
        self._apply_theme()

    @staticmethod
    def ask(*, title: str, message: str, confirm_text: str, parent=None) -> bool:
        dialog = ConfirmDialog(title=title, message=message, confirm_text=confirm_text, parent=parent)
        return dialog.exec() == QDialog.DialogCode.Accepted

    def _apply_theme(self) -> None:
        border = theme_color("border")
        surface = theme_color("surface")
        surface_alt = theme_color("surface_alt")
        window_bg = theme_color("window_bg")
        text_primary = theme_color("text_primary")
        text_muted = theme_color("text_muted")
        accent = theme_color("accent")
        danger = theme_color("danger")
        self.setStyleSheet(
            f"""
            QDialog {{
                background-color: {window_bg};
            }}
            QWidget#confirmShell {{
                background: qlineargradient(
                    x1: 0, y1: 0, x2: 0, y2: 1,
                    stop: 0 {surface_alt},
                    stop: 1 {surface}
                );
                border: 1px solid {border};
                border-radius: 16px;
            }}
            QLabel#confirmEyebrow {{
                color: {accent};
                font-size: 10px;
                font-weight: 700;
                letter-spacing: 0.5px;
                text-transform: uppercase;
                background: transparent;
            }}
            QLabel#confirmTitle {{
                color: {text_primary};
                font-size: 16px;
                font-weight: 700;
                background: transparent;
            }}
            QLabel#confirmMessage {{
                color: {text_muted};
                font-size: 12px;
                line-height: 1.35;
                background: transparent;
            }}
            QPushButton#confirmCancel {{
                background-color: {surface_alt};
                color: {text_primary};
                border: 1px solid {border};
                border-radius: 12px;
                padding: 6px 14px;
                font-weight: 600;
                min-width: 92px;
            }}
            QPushButton#confirmCancel:hover {{
                background-color: {surface};
            }}
            QPushButton#confirmAccept {{
                background-color: {surface};
                color: {text_primary};
                border: 1px solid {danger};
                border-radius: 12px;
                padding: 6px 14px;
                font-weight: 700;
                min-width: 92px;
            }}
            QPushButton#confirmAccept:hover {{
                background-color: {surface_alt};
                border-color: {danger};
            }}
            """
        )
