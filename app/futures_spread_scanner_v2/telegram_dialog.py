from __future__ import annotations

from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QFont
from PySide6.QtWidgets import QDialog, QLabel, QLayout, QLineEdit, QPushButton, QSizePolicy, QVBoxLayout, QWidget, QHBoxLayout

from app.futures_spread_scanner_v2.common.i18n import tr
from app.futures_spread_scanner_v2.common.telegram_bot import QtTelegramBotClient
from app.futures_spread_scanner_v2.common.theme import theme_color


def _set_font_point_size_safe(font: QFont, point_size: int) -> None:
    if point_size > 0:
        font.setPointSize(point_size)


class TelegramConfigDialog(QDialog):
    _test_result = Signal(bool, str)

    def __init__(
        self,
        *,
        bot_token: str = "",
        chat_id: str = "",
        has_saved_credentials: bool = False,
        parent=None,
    ) -> None:
        super().__init__(parent)
        self._remove_requested = False
        self._has_saved_credentials = bool(has_saved_credentials)
        self._test_in_flight = False
        self._telegram_client = QtTelegramBotClient(self)
        self._telegram_client.message_finished.connect(self._on_telegram_message_finished)
        self._test_request_id: str | None = None

        self.setModal(True)
        self.setWindowModality(Qt.WindowModality.ApplicationModal)
        self.setWindowTitle(tr("scanner.telegram_title"))
        self.setMinimumWidth(380)
        self.setWindowFlags(
            Qt.WindowType.Dialog
            | Qt.WindowType.WindowTitleHint
            | Qt.WindowType.WindowCloseButtonHint
            | Qt.WindowType.CustomizeWindowHint
        )
        self.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, True)

        root = QVBoxLayout(self)
        root.setContentsMargins(8, 8, 8, 8)
        root.setSpacing(0)

        shell = QWidget()
        shell.setObjectName("telegramConfigShell")
        shell_layout = QVBoxLayout(shell)
        shell_layout.setContentsMargins(16, 16, 16, 14)
        shell_layout.setSpacing(10)

        title_label = QLabel(tr("scanner.telegram_title"))
        title_label.setObjectName("telegramConfigTitle")
        shell_layout.addWidget(title_label)

        self._subtitle_label = QLabel(tr("scanner.telegram_subtitle"))
        self._subtitle_label.setObjectName("telegramConfigSubtitle")
        self._subtitle_label.setWordWrap(True)
        shell_layout.addWidget(self._subtitle_label)

        self._token_label = QLabel(tr("scanner.telegram_token"))
        self._token_label.setObjectName("telegramConfigFieldLabel")
        shell_layout.addWidget(self._token_label)

        self.token_edit = QLineEdit()
        self.token_edit.setObjectName("telegramConfigEdit")
        self.token_edit.setPlaceholderText(tr("scanner.telegram_token_placeholder"))
        self.token_edit.setEchoMode(QLineEdit.EchoMode.Password)
        self.token_edit.setText(str(bot_token or ""))
        shell_layout.addWidget(self.token_edit)

        self._chat_label = QLabel(tr("scanner.telegram_chat_id"))
        self._chat_label.setObjectName("telegramConfigFieldLabel")
        shell_layout.addWidget(self._chat_label)

        self.chat_id_edit = QLineEdit()
        self.chat_id_edit.setObjectName("telegramConfigEdit")
        self.chat_id_edit.setPlaceholderText(tr("scanner.telegram_chat_id_placeholder"))
        self.chat_id_edit.setText(str(chat_id or ""))
        shell_layout.addWidget(self.chat_id_edit)

        self.connected_panel = QWidget()
        self.connected_panel.setObjectName("telegramConfigConnectedPanel")
        connected_layout = QVBoxLayout(self.connected_panel)
        connected_layout.setContentsMargins(14, 12, 14, 12)
        connected_layout.setSpacing(8)
        connected_header = QHBoxLayout()
        connected_header.setContentsMargins(0, 0, 0, 0)
        connected_header.setSpacing(8)
        self.connected_dot = QLabel()
        self.connected_dot.setObjectName("telegramConfigConnectedDot")
        self.connected_dot.setFixedSize(10, 10)
        connected_header.addWidget(self.connected_dot, 0)
        self.connected_title = QLabel(tr("scanner.telegram_connected"))
        self.connected_title.setObjectName("telegramConfigConnectedTitle")
        connected_header.addWidget(self.connected_title, 0)
        connected_header.addStretch(1)
        connected_layout.addLayout(connected_header)
        self.connected_chat_label = QLabel("")
        self.connected_chat_label.setObjectName("telegramConfigConnectedInfo")
        self.connected_chat_label.setWordWrap(True)
        connected_layout.addWidget(self.connected_chat_label)
        shell_layout.addWidget(self.connected_panel)

        self.status_label = QLabel("")
        self.status_label.setObjectName("telegramConfigStatus")
        self.status_label.setWordWrap(True)
        if has_saved_credentials:
            self.status_label.setText(tr("scanner.telegram_saved"))
        shell_layout.addWidget(self.status_label)

        actions = QHBoxLayout()
        actions.setContentsMargins(0, 8, 0, 0)
        actions.setSpacing(8)

        self.connect_btn = QPushButton(tr("scanner.telegram_connect"))
        self.connect_btn.setObjectName("telegramConfigAccept")
        self.connect_btn.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
        self.connect_btn.clicked.connect(self._on_connect_clicked)
        actions.addWidget(self.connect_btn)

        self.edit_btn = QPushButton(tr("scanner.telegram_edit"))
        self.edit_btn.setObjectName("telegramConfigSecondary")
        self.edit_btn.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
        self.edit_btn.clicked.connect(self._on_edit_clicked)
        actions.addWidget(self.edit_btn)

        self.test_btn = QPushButton(tr("scanner.telegram_test"))
        self.test_btn.setObjectName("telegramConfigSecondary")
        self.test_btn.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
        self.test_btn.clicked.connect(self._on_test_clicked)
        actions.addWidget(self.test_btn)

        self.remove_btn = QPushButton(tr("scanner.telegram_disconnect"))
        self.remove_btn.setObjectName("telegramConfigSecondary")
        self.remove_btn.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
        self.remove_btn.setEnabled(bool(has_saved_credentials))
        self.remove_btn.clicked.connect(self._on_remove_clicked)
        actions.addWidget(self.remove_btn)

        cancel_btn = QPushButton(tr("common.cancel"))
        cancel_btn.setObjectName("telegramConfigSecondary")
        cancel_btn.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
        cancel_btn.clicked.connect(self.reject)
        actions.addWidget(cancel_btn)
        actions.addStretch(1)

        shell_layout.addLayout(actions)
        root.addWidget(shell)
        root.setSizeConstraint(QLayout.SizeConstraint.SetFixedSize)

        self._apply_fonts()
        self._apply_theme()
        self._test_result.connect(self._on_test_result)
        self._set_edit_mode(not has_saved_credentials)
        self.adjustSize()

    @property
    def bot_token(self) -> str:
        return self.token_edit.text().strip()

    @property
    def chat_id(self) -> str:
        return self.chat_id_edit.text().strip()

    @property
    def remove_requested(self) -> bool:
        return self._remove_requested

    def _apply_fonts(self) -> None:
        base_font = QFont("Segoe UI")
        _set_font_point_size_safe(base_font, 9)
        self.setFont(base_font)

    def _on_connect_clicked(self) -> None:
        if not self.bot_token or not self.chat_id:
            self.status_label.setText(tr("scanner.telegram_error_required"))
            return
        self._remove_requested = False
        self.accept()

    def _on_remove_clicked(self) -> None:
        self._remove_requested = True
        self.accept()

    def _on_edit_clicked(self) -> None:
        self._set_edit_mode(True)

    def _on_test_clicked(self) -> None:
        if self._test_in_flight:
            return
        if not self.bot_token or not self.chat_id:
            self.status_label.setText(tr("scanner.telegram_error_required"))
            return
        self._test_in_flight = True
        self.test_btn.setEnabled(False)
        self.status_label.setVisible(True)
        self.status_label.setText(tr("scanner.telegram_testing"))
        self._test_request_id = self._telegram_client.send_message(
            bot_token=self.bot_token,
            chat_id=self.chat_id,
            text="тест",
        )

    def _on_telegram_message_finished(self, request_id: str, ok: bool, message: str) -> None:
        if request_id != self._test_request_id:
            return
        self._test_request_id = None
        if ok:
            self._on_test_result(True, tr("scanner.telegram_test_sent"))
        else:
            self._on_test_result(False, f"{tr('scanner.telegram_test_failed')} {message}".strip())

    def _on_test_result(self, ok: bool, message: str) -> None:
        self._test_in_flight = False
        self.test_btn.setEnabled(True)
        self.status_label.setVisible(True)
        self.status_label.setText(message)

    def _set_edit_mode(self, enabled: bool) -> None:
        self._token_label.setVisible(enabled)
        self.token_edit.setVisible(enabled)
        self._chat_label.setVisible(enabled)
        self.chat_id_edit.setVisible(enabled)
        self.connected_panel.setVisible(not enabled and self._has_saved_credentials)
        self.connect_btn.setVisible(enabled)
        self.edit_btn.setVisible(not enabled and self._has_saved_credentials)
        self.test_btn.setVisible(not enabled and self._has_saved_credentials)
        self.remove_btn.setVisible(self._has_saved_credentials)
        self.connected_chat_label.setText(f'{tr("scanner.telegram_chat_id")}: {self.chat_id}' if self.chat_id else "")
        self._subtitle_label.setVisible(enabled)
        self.status_label.setVisible(enabled or self._test_in_flight)
        if enabled or self._has_saved_credentials:
            self.status_label.setText("")
        self.adjustSize()

    def _apply_theme(self) -> None:
        border = theme_color("border")
        surface = theme_color("surface")
        surface_alt = theme_color("surface_alt")
        window_bg = theme_color("window_bg")
        text_primary = theme_color("text_primary")
        text_muted = theme_color("text_muted")
        accent = theme_color("accent")
        self.setStyleSheet(
            f"""
            QDialog {{
                background-color: {window_bg};
            }}
            QWidget#telegramConfigShell {{
                background: qlineargradient(
                    x1: 0, y1: 0, x2: 0, y2: 1,
                    stop: 0 {surface_alt},
                    stop: 1 {surface}
                );
                border: 1px solid {border};
                border-radius: 16px;
            }}
            QLabel#telegramConfigTitle {{
                color: {text_primary};
                font-size: 16px;
                font-weight: 700;
                background: transparent;
            }}
            QLabel#telegramConfigSubtitle {{
                color: {text_muted};
                font-size: 12px;
                background: transparent;
            }}
            QLabel#telegramConfigFieldLabel {{
                color: {text_primary};
                font-size: 11px;
                font-weight: 600;
                background: transparent;
                margin-top: 2px;
            }}
            QLabel#telegramConfigStatus {{
                color: {text_muted};
                font-size: 11px;
                background: transparent;
                min-height: 18px;
            }}
            QWidget#telegramConfigConnectedPanel {{
                background-color: {surface_alt};
                border: 1px solid {border};
                border-radius: 16px;
            }}
            QLabel#telegramConfigConnectedDot {{
                background-color: #38d66b;
                border: none;
                border-radius: 5px;
                min-width: 10px;
                max-width: 10px;
                min-height: 10px;
                max-height: 10px;
            }}
            QLabel#telegramConfigConnectedTitle {{
                color: {text_primary};
                font-size: 13px;
                font-weight: 700;
                background: transparent;
            }}
            QLabel#telegramConfigConnectedInfo {{
                color: {text_primary};
                font-size: 12px;
                font-weight: 600;
                background: transparent;
            }}
            QLineEdit#telegramConfigEdit {{
                background-color: {surface};
                color: {text_primary};
                border: 1px solid {border};
                border-radius: 12px;
                padding: 8px 10px;
                font-weight: 600;
                min-height: 18px;
            }}
            QLineEdit#telegramConfigEdit:focus {{
                border: 1px solid {accent};
            }}
            QPushButton#telegramConfigAccept {{
                background-color: {surface};
                color: {text_primary};
                border: 1px solid {accent};
                border-radius: 12px;
                padding: 7px 14px;
                font-weight: 700;
            }}
            QPushButton#telegramConfigAccept:hover {{
                background-color: {surface_alt};
            }}
            QPushButton#telegramConfigSecondary {{
                background-color: {surface_alt};
                color: {text_primary};
                border: 1px solid {border};
                border-radius: 12px;
                padding: 7px 14px;
                font-weight: 600;
            }}
            QPushButton#telegramConfigSecondary:hover {{
                background-color: {surface};
            }}
            QPushButton#telegramConfigSecondary:disabled {{
                color: {text_muted};
            }}
            """
        )


__all__ = ["TelegramConfigDialog"]
