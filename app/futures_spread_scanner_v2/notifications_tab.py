from __future__ import annotations

from pathlib import Path

from PySide6.QtCore import QSize, Qt
from PySide6.QtGui import QIcon
from PySide6.QtWidgets import QDialog, QHBoxLayout, QLabel, QToolButton, QVBoxLayout, QWidget

from app.futures_spread_scanner_v2.settings import load_v2_settings, save_v2_settings
from app.futures_spread_scanner_v2.telegram_dialog import TelegramConfigDialog
from app.futures_spread_scanner_v2.common.i18n import tr
from app.futures_spread_scanner_v2.common.secure_credential_store import (
    delete_telegram_credentials,
    find_telegram_credential_ref,
    load_telegram_credentials,
    save_telegram_credentials,
)
from app.futures_spread_scanner_v2.common.theme import theme_color


class FuturesSpreadNotificationsTab(QWidget):
    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self.setObjectName("notificationsTab")
        self._telegram_credential_ref = str(load_v2_settings().get("telegram_credential_ref") or "").strip() or None
        if self._telegram_credential_ref is None:
            self._telegram_credential_ref = find_telegram_credential_ref()

        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(8)

        top_row = QHBoxLayout()
        top_row.setContentsMargins(0, 0, 0, 0)
        top_row.setSpacing(4)

        self._title = QLabel(tr("notifications.title"))
        self._title.setObjectName("notificationsTitle")
        self._title.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        top_row.addWidget(self._title, 0)
        top_row.addStretch(1)

        self._telegram_status_dot = QLabel("●")
        self._telegram_status_dot.setObjectName("notificationsTelegramStatusDot")
        top_row.addWidget(self._telegram_status_dot, 0, Qt.AlignmentFlag.AlignVCenter)

        self._telegram_status_text = QLabel()
        self._telegram_status_text.setObjectName("notificationsTelegramStatusText")
        top_row.addWidget(self._telegram_status_text, 0, Qt.AlignmentFlag.AlignVCenter)

        self._telegram_btn = QToolButton()
        self._telegram_btn.setObjectName("notificationsTelegramBtn")
        self._telegram_btn.setAutoRaise(True)
        self._telegram_btn.setToolTip(tr("scanner.telegram_tooltip"))
        telegram_icon_path = Path(__file__).resolve().parent / "assets" / "logos" / "telegram.svg"
        self._telegram_btn.setIcon(QIcon(str(telegram_icon_path)))
        self._telegram_btn.setIconSize(QSize(18, 18))
        self._telegram_btn.setToolButtonStyle(Qt.ToolButtonStyle.ToolButtonTextBesideIcon)
        self._telegram_btn.setLayoutDirection(Qt.LayoutDirection.LeftToRight)
        self._telegram_btn.setFixedHeight(34)
        self._telegram_btn.setText("Telegram")
        self._telegram_btn.clicked.connect(self._on_telegram_clicked)
        top_row.addWidget(self._telegram_btn, 0, Qt.AlignmentFlag.AlignTop)

        layout.addLayout(top_row)

        self._subtitle = QLabel(tr("notifications.subtitle"))
        self._subtitle.setObjectName("notificationsSubtitle")
        self._subtitle.setWordWrap(True)
        layout.addWidget(self._subtitle)
        layout.addStretch(1)

        self.apply_theme()
        self.retranslate_ui()
        self._sync_telegram_button_state()

    def retranslate_ui(self) -> None:
        self._title.setText(tr("notifications.title"))
        self._subtitle.setText(tr("notifications.subtitle"))
        self._telegram_btn.setToolTip(tr("scanner.telegram_tooltip"))
        self._sync_telegram_button_state()

    def apply_theme(self) -> None:
        border = theme_color("border")
        text_primary = theme_color("text_primary")
        text_muted = theme_color("text_muted")
        surface = theme_color("surface")
        surface_alt = theme_color("surface_alt")
        window_bg = theme_color("window_bg")
        self.setStyleSheet(
            f"""
            QWidget#notificationsTab {{
                background-color: {window_bg};
            }}
            QLabel#notificationsTitle {{
                color: {text_primary};
                font-size: 18px;
                font-weight: 700;
                background: transparent;
            }}
            QLabel#notificationsSubtitle {{
                color: {text_muted};
                font-size: 12px;
                background: transparent;
            }}
            QLabel#notificationsTelegramStatusDot {{
                font-size: 12px;
                font-weight: 700;
                color: #ef4444;
                padding: 0 1px 0 0;
                background: transparent;
            }}
            QLabel#notificationsTelegramStatusText {{
                font-size: 12px;
                font-weight: 600;
                color: {text_primary};
                padding: 0 2px 0 0;
                background: transparent;
            }}
            QToolButton#notificationsTelegramBtn {{
                min-height: 34px;
                padding: 0 12px;
                border: 1px solid #355070;
                border-radius: 17px;
                background: {surface};
                color: {text_primary};
                font-weight: 700;
            }}
            QToolButton#notificationsTelegramBtn:hover {{
                background: {surface_alt};
            }}
            QToolButton#notificationsTelegramBtn[connected="true"] {{
                border-color: #355070;
                color: {text_primary};
            }}
            """
        )

    def _sync_telegram_button_state(self) -> None:
        has_credentials = bool(load_telegram_credentials(self._telegram_credential_ref))
        self._telegram_status_text.setText("подкл." if has_credentials else "не подкл.")
        self._telegram_btn.setProperty("connected", has_credentials)
        self._telegram_btn.style().unpolish(self._telegram_btn)
        self._telegram_btn.style().polish(self._telegram_btn)
        self._telegram_btn.update()
        self._telegram_status_dot.setStyleSheet(
            f"color: {'#22c55e' if has_credentials else '#ef4444'}; font-size: 12px; font-weight: 700; padding: 0 1px 0 0;"
        )

    def _on_telegram_clicked(self) -> None:
        credentials = load_telegram_credentials(self._telegram_credential_ref) or {}
        dialog = TelegramConfigDialog(
            bot_token=str(credentials.get("bot_token", "")),
            chat_id=str(credentials.get("chat_id", "")),
            has_saved_credentials=bool(credentials),
            parent=self.window(),
        )
        if dialog.exec() != QDialog.DialogCode.Accepted:
            return
        if dialog.remove_requested:
            delete_telegram_credentials(self._telegram_credential_ref)
            self._telegram_credential_ref = None
            save_v2_settings(telegram_credential_ref=None)
            self._sync_telegram_button_state()
            return
        credential_ref = save_telegram_credentials(
            bot_token=dialog.bot_token,
            chat_id=dialog.chat_id,
            credential_ref=self._telegram_credential_ref,
        )
        self._telegram_credential_ref = credential_ref
        save_v2_settings(telegram_credential_ref=credential_ref)
        self._sync_telegram_button_state()


__all__ = ["FuturesSpreadNotificationsTab"]
