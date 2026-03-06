from __future__ import annotations

from PySide6.QtCore import Signal
from PySide6.QtGui import QColor, QFont, QPainter, QPen
from PySide6.QtWidgets import QFrame, QGroupBox, QHBoxLayout, QLabel, QLineEdit, QPushButton, QSizePolicy, QVBoxLayout, QWidget

from ui.exchange_catalog import get_exchange_meta, normalize_exchange_code, requires_passphrase
from ui.i18n import tr
from ui.theme import theme_color
from ui.widgets.exchange_badge import build_exchange_pixmap


class StatusDot(QWidget):
    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self._fill = QColor("#ef4444")
        self._border = QColor("#dc2626")
        self.setFixedSize(10, 10)

    def set_colors(self, fill: str, border: str) -> None:
        self._fill = QColor(fill)
        self._border = QColor(border)
        self.update()

    def paintEvent(self, event) -> None:
        super().paintEvent(event)
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        painter.setPen(QPen(self._border, 1.0))
        painter.setBrush(self._fill)
        painter.drawEllipse(self.rect().adjusted(1, 1, -1, -1))


class ExchangePanel(QFrame):
    connect_clicked = Signal(str, dict)
    disconnect_clicked = Signal(str)
    close_positions_clicked = Signal(str)
    remove_clicked = Signal(str)
    cancel_clicked = Signal()
    edit_clicked = Signal(str)

    def __init__(self, exchange_name: str, exchange_type: str, is_new: bool = False, parent=None) -> None:
        super().__init__(parent)
        self.exchange_name = exchange_name
        self.exchange_type = normalize_exchange_code(exchange_type)
        self.exchange_meta = get_exchange_meta(self.exchange_type)
        self.is_connected = False
        self.testnet = False
        self.is_new = is_new
        self.edit_mode = is_new

        self._init_ui()
        self.apply_theme()
        self._update_ui_state()
        self.retranslate_ui()

    @staticmethod
    def _rgba(hex_color: str, alpha: float) -> str:
        color = str(hex_color or "").strip()
        if color.startswith("#") and len(color) == 7:
            try:
                r = int(color[1:3], 16)
                g = int(color[3:5], 16)
                b = int(color[5:7], 16)
                a = max(0.0, min(1.0, float(alpha)))
                return f"rgba({r}, {g}, {b}, {a:.3f})"
            except ValueError:
                return color
        return color

    def _soft_button_style(self, role: str) -> str:
        roles = {
            "primary": (
                self._rgba(theme_color("accent"), 0.15),
                self._rgba(theme_color("accent"), 0.54),
                theme_color("text_primary"),
                self._rgba(theme_color("accent"), 0.22),
            ),
            "danger": (
                self._rgba(theme_color("danger"), 0.16),
                self._rgba(theme_color("danger"), 0.54),
                theme_color("text_primary"),
                self._rgba(theme_color("danger"), 0.22),
            ),
            "warning": (
                self._rgba(theme_color("warning"), 0.16),
                self._rgba(theme_color("warning"), 0.58),
                theme_color("text_primary"),
                self._rgba(theme_color("warning"), 0.24),
            ),
            "secondary": (
                self._rgba(theme_color("surface"), 0.66),
                self._rgba(theme_color("border"), 0.46),
                theme_color("text_muted"),
                self._rgba(theme_color("surface_alt"), 0.88),
            ),
        }
        bg, border, text, hover = roles[role]
        pressed = self._rgba(theme_color("surface_alt"), 0.95)
        return (
            f"QPushButton {{ background-color: {bg}; color: {text}; border: 1px solid {border}; "
            "border-radius: 10px; padding: 4px 10px; font-weight: 600; }"
            f" QPushButton:hover {{ background-color: {hover}; border-color: {border}; }}"
            f" QPushButton:pressed {{ background-color: {pressed}; border-color: {border}; }}"
            f" QPushButton:disabled {{ color: {theme_color('text_muted')}; "
            f"background-color: {self._rgba(theme_color('surface'), 0.55)}; "
            f"border-color: {self._rgba(theme_color('border'), 0.30)}; }}"
        )

    def _metric_capsule_style(self, color_key: str, bold: bool = False) -> str:
        weight = "700" if bold else "600"
        return (
            f"color: {theme_color(color_key)}; font-size: 11px; font-weight: {weight}; "
            f"background-color: {self._rgba(theme_color('window_bg'), 0.70)}; "
            f"border: 1px solid {self._rgba(theme_color('border'), 0.46)}; border-radius: 10px; "
            "padding: 4px 10px;"
        )

    def _apply_status_container_style(self) -> None:
        self.status_widget.setStyleSheet(
            f"""
            QWidget#statusWidget {{
                background-color: {self._rgba(theme_color('window_bg'), 0.72)};
                border: 1px solid {self._rgba(theme_color('border'), 0.56)};
                border-radius: 12px;
            }}
            """
        )

    def _set_status_view(self, text: str, text_color_key: str, indicator_fill: str, indicator_border: str) -> None:
        self.status_widget.setVisible(True)
        self.status_label.setVisible(True)
        self.status_indicator.setVisible(True)
        self.status_label.setText(text)
        self.status_label.setStyleSheet(f"color: {theme_color(text_color_key)}; font-size: 10px; font-weight: 600;")
        self.status_indicator.set_colors(indicator_fill, indicator_border)

    def _init_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(6)

        header = QHBoxLayout()
        header.setContentsMargins(0, 0, 0, 0)
        header.setSpacing(8)

        self.icon_label = QLabel()
        self.icon_label.setPixmap(build_exchange_pixmap(self.exchange_type, size=30))
        self.icon_label.setFixedSize(30, 30)
        self.icon_label.setAlignment(Qt.AlignmentFlag.AlignCenter)

        self.name_label = QLabel(self.exchange_name)
        font = QFont()
        font.setPointSize(10)
        font.setBold(True)
        self.name_label.setFont(font)
        self.name_label.setMinimumWidth(84)

        self.status_label = QLabel()
        self.status_indicator = StatusDot()
        self.status_widget = QWidget()
        self.status_widget.setObjectName("statusWidget")
        self.status_widget.setMinimumHeight(30)
        self.status_widget.setMaximumHeight(30)
        self.status_widget.setSizePolicy(QSizePolicy.Policy.Maximum, QSizePolicy.Policy.Fixed)
        status_layout = QHBoxLayout(self.status_widget)
        status_layout.setContentsMargins(10, 4, 10, 4)
        status_layout.setSpacing(6)
        status_layout.addWidget(self.status_indicator)
        status_layout.addWidget(self.status_label)

        header.addWidget(self.icon_label)
        header.addWidget(self.name_label)
        header.addWidget(self.status_widget)
        header.addStretch()
        layout.addLayout(header)

        self.stats_widget = QWidget()
        stats_layout = QHBoxLayout(self.stats_widget)
        stats_layout.setContentsMargins(0, 0, 0, 0)
        stats_layout.setSpacing(8)
        self.balance_label = QLabel()
        self.positions_label = QLabel()
        self.pnl_label = QLabel()
        for label in (self.balance_label, self.positions_label, self.pnl_label):
            label.setAlignment(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignLeft)
            label.setMinimumHeight(28)
            stats_layout.addWidget(label)
        stats_layout.addStretch()
        layout.addWidget(self.stats_widget)

        self.api_group = QGroupBox()
        api_layout = QHBoxLayout()
        api_layout.setSpacing(5)

        self.api_key_input = QLineEdit()
        self.api_key_input.setPlaceholderText("")
        self.api_key_input.setEchoMode(QLineEdit.EchoMode.Password)
        self.api_key_input.setMinimumWidth(180)

        self.api_secret_input = QLineEdit()
        self.api_secret_input.setPlaceholderText("")
        self.api_secret_input.setEchoMode(QLineEdit.EchoMode.Password)
        self.api_secret_input.setMinimumWidth(180)

        self.passphrase_input = QLineEdit()
        self.passphrase_input.setEchoMode(QLineEdit.EchoMode.Password)
        self.passphrase_input.setMinimumWidth(120)

        api_layout.addWidget(self.api_key_input)
        api_layout.addWidget(self.api_secret_input)
        api_layout.addWidget(self.passphrase_input)
        api_layout.addStretch()
        self.api_group.setLayout(api_layout)
        layout.addWidget(self.api_group)

        button_layout = QHBoxLayout()
        button_layout.setContentsMargins(0, 0, 0, 0)
        button_layout.setSpacing(6)

        self.connect_btn = QPushButton()
        self.connect_btn.setMinimumWidth(100)
        self.connect_btn.clicked.connect(self._on_connect)

        self.disconnect_btn = QPushButton()
        self.disconnect_btn.setMinimumWidth(100)
        self.disconnect_btn.clicked.connect(lambda: self.disconnect_clicked.emit(self.exchange_name))

        self.close_positions_btn = QPushButton()
        self.close_positions_btn.setMinimumWidth(130)
        self.close_positions_btn.clicked.connect(lambda: self.close_positions_clicked.emit(self.exchange_name))

        self.edit_btn = QPushButton()
        self.edit_btn.setMinimumWidth(100)
        self.edit_btn.clicked.connect(self._on_edit_clicked)

        self.cancel_btn = QPushButton()
        self.cancel_btn.setMinimumWidth(100)
        self.cancel_btn.clicked.connect(self._on_cancel_clicked)

        self.remove_btn = QPushButton()
        self.remove_btn.setMinimumWidth(100)
        self.remove_btn.clicked.connect(lambda: self.remove_clicked.emit(self.exchange_name))

        for button in (
            self.connect_btn,
            self.disconnect_btn,
            self.close_positions_btn,
            self.edit_btn,
            self.cancel_btn,
            self.remove_btn,
        ):
            button_layout.addWidget(button)
        button_layout.addStretch()
        layout.addLayout(button_layout)

        self._update_passphrase_hint()
        self._configure_new_mode()

    def _configure_new_mode(self) -> None:
        if not self.is_new:
            return
        self.stats_widget.setVisible(False)
        self.name_label.setVisible(False)
        self.icon_label.setFixedSize(52, 52)
        self.icon_label.setPixmap(build_exchange_pixmap(self.exchange_type, size=52))
        self.status_label.clear()
        self.status_label.setVisible(False)
        self.status_indicator.setVisible(False)
        self.status_widget.setVisible(False)

    def _update_passphrase_hint(self) -> None:
        if requires_passphrase(self.exchange_type):
            self.passphrase_input.setPlaceholderText(tr("exchange.passphrase"))
        else:
            self.passphrase_input.setPlaceholderText(tr("exchange.passphrase_optional"))

    def _update_ui_state(self) -> None:
        if self.is_connected:
            self._set_status_view(tr("exchange.status.connected"), "success", "#22c55e", "#16a34a")
            self.connect_btn.setVisible(False)
            self.disconnect_btn.setVisible(True)
            self.close_positions_btn.setVisible(True)
            self.edit_btn.setVisible(False)
            self.cancel_btn.setVisible(False)
            self.api_group.setVisible(False)
        else:
            if self.is_new:
                self.status_widget.setVisible(False)
            else:
                self._set_status_view(tr("exchange.status.disconnected"), "text_muted", "#ef4444", "#dc2626")

            self.balance_label.setText(tr("exchange.balance"))
            self.positions_label.setText(tr("exchange.positions"))
            self.pnl_label.setText(tr("exchange.pnl"))

            if self.edit_mode:
                self.connect_btn.setText(tr("exchange.add_connection") if self.is_new else tr("exchange.connect"))
                self.connect_btn.setVisible(True)
                self.disconnect_btn.setVisible(False)
                self.close_positions_btn.setVisible(False)
                self.edit_btn.setVisible(False)
                self.cancel_btn.setVisible(True)
                self.api_group.setVisible(True)
            else:
                self.connect_btn.setText(tr("exchange.connect"))
                self.connect_btn.setVisible(True)
                self.disconnect_btn.setVisible(False)
                self.close_positions_btn.setVisible(False)
                self.edit_btn.setVisible(True)
                self.cancel_btn.setVisible(False)
                self.api_group.setVisible(False)

        self.remove_btn.setVisible(not self.is_new)

    @staticmethod
    def _is_ascii(value: str) -> bool:
        try:
            value.encode("ascii")
            return True
        except UnicodeEncodeError:
            return False

    def _show_input_error(self, message: str) -> None:
        self._set_status_view(message, "danger", "#ef4444", "#dc2626")

    def retranslate_ui(self) -> None:
        self.api_group.setTitle(tr("exchange.api_group", exchange=self.exchange_meta["title"]))
        self.api_key_input.setPlaceholderText(tr("exchange.api_key"))
        self.api_secret_input.setPlaceholderText(tr("exchange.api_secret"))
        self._update_passphrase_hint()
        self.disconnect_btn.setText(tr("exchange.disconnect"))
        self.close_positions_btn.setText(tr("exchange.close_positions"))
        self.edit_btn.setText(tr("exchange.edit"))
        self.cancel_btn.setText(tr("common.cancel"))
        self.remove_btn.setText(tr("exchange.remove"))
        self._update_ui_state()

    def _on_connect(self) -> None:
        api_key = self.api_key_input.text().strip()
        api_secret = self.api_secret_input.text().strip()
        passphrase = self.passphrase_input.text().strip()

        if not api_key or not api_secret:
            self._show_input_error(tr("exchange.error.key_secret_required"))
            return
        if not self._is_ascii(api_key) or not self._is_ascii(api_secret):
            self._show_input_error(tr("exchange.error.key_secret_ascii"))
            return
        if requires_passphrase(self.exchange_type):
            if not passphrase:
                self._show_input_error(tr("exchange.error.passphrase_required"))
                return
            if not self._is_ascii(passphrase):
                self._show_input_error(tr("exchange.error.passphrase_ascii"))
                return
        elif passphrase and not self._is_ascii(passphrase):
            self._show_input_error(tr("exchange.error.passphrase_ascii"))
            return

        params = {
            "api_key": api_key,
            "api_secret": api_secret,
            "api_passphrase": passphrase,
            "testnet": False,
        }
        self.connect_clicked.emit(self.exchange_name, params)

    def _on_edit_clicked(self) -> None:
        self.edit_mode = True
        self.edit_clicked.emit(self.exchange_name)
        self._update_ui_state()

    def _on_cancel_clicked(self) -> None:
        if self.is_new:
            self.cancel_clicked.emit()
            return
        self.edit_mode = False
        self._update_ui_state()

    def set_edit_mode(self, edit_mode: bool) -> None:
        self.edit_mode = edit_mode
        self._update_ui_state()

    def mark_connected(self, connected: bool = True, demo: bool = False) -> None:
        del demo
        self.is_connected = connected
        self.testnet = False
        self._update_ui_state()

    def load_saved_data(self, params: dict) -> None:
        self.api_key_input.setText(str(params.get("api_key", "")))
        self.api_secret_input.setText(str(params.get("api_secret", "")))
        self.passphrase_input.setText(str(params.get("api_passphrase", "")))
        self.testnet = False

    def apply_theme(self) -> None:
        self.setObjectName("exchangePanel")
        soft_border = self._rgba(theme_color("border"), 0.80)
        soft_field_border = self._rgba(theme_color("border"), 0.54)
        soft_hover_border = self._rgba(theme_color("accent"), 0.70)
        panel_top = self._rgba(theme_color("surface_alt"), 0.96)
        panel_bottom = self._rgba(theme_color("window_bg"), 0.98)
        self.setStyleSheet(
            f"""
            QFrame#exchangePanel {{
                border: 2px solid {soft_border};
                border-radius: 16px;
                background: qlineargradient(
                    x1: 0, y1: 0, x2: 0, y2: 1,
                    stop: 0 {panel_top},
                    stop: 1 {panel_bottom}
                );
                margin: 0px;
                padding: 7px;
            }}
            QGroupBox {{
                border: none;
                margin-top: 0px;
                padding-top: 0px;
                color: {theme_color('text_muted')};
                font-size: 11px;
                font-weight: 600;
            }}
            QGroupBox::title {{
                subcontrol-origin: margin;
                left: 2px;
                padding: 0 4px;
            }}
            QLineEdit {{
                background-color: {theme_color('surface')};
                color: {theme_color('text_primary')};
                border: 1px solid {soft_field_border};
                border-radius: 10px;
                padding: 5px 8px;
            }}
            QLineEdit:hover {{
                border-color: {soft_hover_border};
            }}
            QLineEdit:focus {{
                border-color: {soft_hover_border};
            }}
            QLabel {{
                background: transparent;
            }}
            """
        )
        self.balance_label.setStyleSheet(self._metric_capsule_style("success", bold=True))
        self.positions_label.setStyleSheet(self._metric_capsule_style("warning"))
        self.pnl_label.setStyleSheet(self._metric_capsule_style("text_muted", bold=True))
        self.name_label.setStyleSheet(
            f"""
            color: {theme_color('text_primary')};
            background-color: {self._rgba(theme_color('window_bg'), 0.70)};
            border: 1px solid {self._rgba(theme_color('border'), 0.52)};
            border-radius: 10px;
            padding: 3px 9px;
            """
        )
        self.connect_btn.setStyleSheet(self._soft_button_style("primary"))
        self.disconnect_btn.setStyleSheet(self._soft_button_style("danger"))
        self.close_positions_btn.setStyleSheet(self._soft_button_style("warning"))
        self.edit_btn.setStyleSheet(self._soft_button_style("warning"))
        self.remove_btn.setStyleSheet(self._soft_button_style("secondary"))
        self.cancel_btn.setStyleSheet(self._soft_button_style("secondary"))
        self._apply_status_container_style()
        self._update_ui_state()
