from __future__ import annotations

from decimal import Decimal, InvalidOperation

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (
    QFrame,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QSizePolicy,
    QVBoxLayout,
    QWidget,
)

from app.ui.i18n import tr
from app.ui.theme import theme_color
from app.ui.widgets.exchange_badge import build_exchange_pixmap

_LEG_CONTAINER_WIDTH = 115
_PAIRS_CONTAINER_WIDTH = 250
_STATUS_WIDGET_WIDTH = 130
_CAPSULE_WIDTHS = {
    "entry_threshold": 90,
    "exit_threshold": 95,
    "target": 110,
    "min_step": 95,
    "volume": 120,
}

_STATUS_MAP = {
    "STARTING": ("Запуск", "starting", "#fb923c", "#f97316"),
    "WAITING_ENTRY": ("Ожидание", "waiting", "#60a5fa", "#3b82f6"),
    "WAITING_EXIT": ("В позиции", "position", "#22c55e", "#16a34a"),
    "ENTERING": ("Вход...", "active", "#fbbf24", "#f59e0b"),
    "EXITING": ("Выход...", "active", "#fbbf24", "#f59e0b"),
    "REBALANCING": ("Балансировка", "warning", "#fb923c", "#f97316"),
    "RESTORE_HEDGE": ("Хедж", "warning", "#fb923c", "#f97316"),
    "EMERGENCY_CLOSE": ("Аварийный", "danger", "#ef4444", "#dc2626"),
    "RECOVERY": ("Восстановление", "warning", "#fb923c", "#f97316"),
    "FAILED": ("Сбой", "danger", "#ef4444", "#dc2626"),
    "STOPPED": ("Остановлен", "stopped", "#ef4444", "#dc2626"),
}

_STREAM_STATUS_OVERRIDE = {
    "DISCONNECTED": ("Дисконнект", "danger", "#ef4444", "#dc2626"),
    "DEGRADED": ("Реконнект", "warning", "#fb923c", "#f97316"),
}


def _compact_usdt(raw: str) -> str:
    """Format a USDT value into compact human-readable form: 1.5Ðº, 12Ðœ, etc."""
    try:
        val = Decimal(str(raw).replace(",", "").strip())
    except (InvalidOperation, ValueError):
        return raw
    if val < Decimal("0"):
        return str(val)
    if val >= Decimal("1000000"):
        d = (val / Decimal("1000000")).quantize(Decimal("0.1"))
        return f"{d}М"
    if val >= Decimal("1000"):
        d = (val / Decimal("1000")).quantize(Decimal("0.1"))
        return f"{d}к"
    if val >= Decimal("1"):
        return str(val.quantize(Decimal("0.1")))
    return str(val)


class RuntimeCard(QFrame):
    """Visual card representing a running spread runtime."""

    stop_clicked = Signal(str)

    def __init__(
        self,
        worker_id: str,
        *,
        left_exchange: str,
        left_symbol: str,
        right_exchange: str,
        right_symbol: str,
        params: dict[str, str],
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self.worker_id = worker_id
        self._left_exchange = left_exchange
        self._left_symbol = left_symbol
        self._right_exchange = right_exchange
        self._right_symbol = right_symbol
        self._params = dict(params)
        self._running = True
        self._last_volume_text = ""
        self._last_status_key = ""

        self.setObjectName("runtimeCard")
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)

        root = QHBoxLayout(self)
        root.setContentsMargins(10, 6, 10, 6)
        root.setSpacing(6)
        root.setAlignment(Qt.AlignmentFlag.AlignVCenter)

        pairs_container = QWidget()
        pairs_container.setFixedWidth(_PAIRS_CONTAINER_WIDTH)
        pairs_container.setFixedHeight(36)
        pairs_layout = QHBoxLayout(pairs_container)
        pairs_layout.setContentsMargins(0, 0, 0, 0)
        pairs_layout.setSpacing(0)

        left_leg = self._build_leg_widget(left_exchange, left_symbol, align_right=False)
        pairs_layout.addWidget(left_leg, 1, Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)

        vs_label = QLabel("⇄")
        vs_label.setObjectName("runtimeCardVs")
        vs_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        vs_label.setFixedWidth(20)
        pairs_layout.addWidget(vs_label, 0, Qt.AlignmentFlag.AlignCenter)

        right_leg = self._build_leg_widget(right_exchange, right_symbol, align_right=True)
        pairs_layout.addWidget(right_leg, 1, Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)

        root.addWidget(pairs_container)

        root.addSpacing(10)

        self._status_widget = QWidget()
        self._status_widget.setObjectName("runtimeStatusWidget")
        self._status_widget.setFixedWidth(_STATUS_WIDGET_WIDTH)
        status_layout = QHBoxLayout(self._status_widget)
        status_layout.setContentsMargins(8, 2, 10, 2)
        status_layout.setSpacing(5)
        self._status_dot = QLabel()
        self._status_dot.setObjectName("runtimeStatusDot")
        self._status_dot.setFixedSize(10, 10)
        self._status_label = QLabel("Запуск")
        self._status_label.setObjectName("runtimeStatusLabel")
        status_layout.addWidget(self._status_dot)
        status_layout.addWidget(self._status_label)
        root.addWidget(self._status_widget)

        root.addSpacing(10)

        target_raw = params.get("entry_notional_usdt", "--")
        display_params = (
            ("entry_threshold", "Вход %", params.get("entry_threshold", "--")),
            ("exit_threshold", "Выход %", params.get("exit_threshold", "--")),
            ("target", "Цель $", _compact_usdt(target_raw)),
            ("min_step", "Мин шаг", params.get("entry_min_step_pct", "--") + "%"),
        )
        self._param_labels: list[QLabel] = []
        for param_key, title, value in display_params:
            root.addWidget(self._build_capsule(param_key, title, value))

        self._volume_label = self._build_capsule_with_label_ref("volume", "Текущий $", "0")
        root.addWidget(self._volume_label[0])

        root.addStretch(1)

        self._stop_btn = QPushButton(tr("runtime.stop"))
        self._stop_btn.setObjectName("runtimeStopButton")
        self._stop_btn.setFixedHeight(26)
        self._stop_btn.setMinimumWidth(70)
        self._stop_btn.clicked.connect(lambda: self.stop_clicked.emit(self.worker_id))
        root.addWidget(self._stop_btn)

        self.apply_theme()
        self.update_status("STARTING", "UNKNOWN")

    def _build_capsule(self, param_key: str, title: str, value: str) -> QFrame:
        capsule = QFrame()
        capsule.setObjectName("runtimeParamCapsule")
        capsule.setFixedWidth(_CAPSULE_WIDTHS.get(param_key, 95))
        c_layout = QHBoxLayout(capsule)
        c_layout.setContentsMargins(8, 3, 8, 3)
        c_layout.setSpacing(4)
        key_lbl = QLabel(title)
        key_lbl.setObjectName("runtimeParamKey")
        val_lbl = QLabel(value)
        val_lbl.setObjectName("runtimeParamValue")
        self._param_labels.extend([key_lbl, val_lbl])
        c_layout.addWidget(key_lbl)
        c_layout.addWidget(val_lbl)
        return capsule

    def _build_capsule_with_label_ref(self, param_key: str, title: str, value: str) -> tuple[QFrame, QLabel]:
        capsule = QFrame()
        capsule.setObjectName("runtimeParamCapsule")
        capsule.setFixedWidth(_CAPSULE_WIDTHS.get(param_key, 95))
        c_layout = QHBoxLayout(capsule)
        c_layout.setContentsMargins(8, 3, 8, 3)
        c_layout.setSpacing(4)
        key_lbl = QLabel(title)
        key_lbl.setObjectName("runtimeParamKey")
        val_lbl = QLabel(value)
        val_lbl.setObjectName("runtimeParamValue")
        self._param_labels.extend([key_lbl, val_lbl])
        c_layout.addWidget(key_lbl)
        c_layout.addWidget(val_lbl)
        return capsule, val_lbl

    @staticmethod
    def _build_leg_widget(exchange: str, symbol: str, *, align_right: bool = False) -> QWidget:
        container = QWidget()
        container.setFixedHeight(36)
        h = QHBoxLayout(container)
        h.setContentsMargins(0, 0, 0, 0)
        h.setSpacing(5)
        h.setAlignment(Qt.AlignmentFlag.AlignVCenter)

        icon = QLabel()
        icon.setFixedSize(26, 26)
        icon.setPixmap(build_exchange_pixmap(exchange, size=26))
        icon.setAlignment(Qt.AlignmentFlag.AlignCenter)

        stack = QVBoxLayout()
        stack.setContentsMargins(0, 0, 0, 0)
        stack.setSpacing(0)
        stack.setAlignment(Qt.AlignmentFlag.AlignVCenter)
        exch_lbl = QLabel(exchange.capitalize())
        exch_lbl.setObjectName("runtimeCardExchange")
        sym_lbl = QLabel(symbol)
        sym_lbl.setObjectName("runtimeCardSymbol")
        if align_right:
            exch_lbl.setAlignment(Qt.AlignmentFlag.AlignRight)
            sym_lbl.setAlignment(Qt.AlignmentFlag.AlignRight)
        stack.addWidget(exch_lbl)
        stack.addWidget(sym_lbl)

        if align_right:
            h.addStretch(1)
            h.addLayout(stack)
            h.addWidget(icon)
        else:
            h.addWidget(icon)
            h.addLayout(stack)
            h.addStretch(1)
        return container

    def set_status(self, running: bool) -> None:
        """Legacy method for basic running/stopped status."""
        if running:
            self.update_status("STARTING", "UNKNOWN")
        else:
            self.update_status("STOPPED", "UNKNOWN")

    def update_status(self, activity_status: str, stream_health: str) -> None:
        """Update status display based on activity and stream health."""
        activity = str(activity_status or "").strip().upper()
        health = str(stream_health or "").strip().upper()

        if health in _STREAM_STATUS_OVERRIDE and activity not in {"STOPPED", "FAILED"}:
            text, state, fill, border = _STREAM_STATUS_OVERRIDE[health]
        elif activity in _STATUS_MAP:
            text, state, fill, border = _STATUS_MAP[activity]
        else:
            text, state, fill, border = ("Запуск", "starting", "#fb923c", "#f97316")

        status_key = f"{text}:{state}"
        if status_key == self._last_status_key:
            return
        self._last_status_key = status_key

        self._running = activity not in {"STOPPED", "FAILED"}
        self._status_label.setText(text)
        self._status_dot.setProperty("dotState", state)
        self._status_dot.setStyleSheet(f"""
            background-color: {fill};
            border: 1px solid {border};
            border-radius: 4px;
            min-width: 8px; max-width: 8px;
            min-height: 8px; max-height: 8px;
        """)
        self._stop_btn.setEnabled(self._running)

    def update_volume(self, notional_usdt: str) -> None:
        text = _compact_usdt(notional_usdt)
        if text == self._last_volume_text:
            return
        self._last_volume_text = text
        self._volume_label[1].setText(text)

    def apply_theme(self) -> None:
        c_surface = theme_color("surface")
        c_surface_alt = theme_color("surface_alt")
        c_window = theme_color("window_bg")
        c_border = theme_color("border")
        c_primary = theme_color("text_primary")
        c_muted = theme_color("text_muted")
        c_danger = theme_color("danger")

        card_bg = _rgba(c_surface_alt, 0.97)
        capsule_bg = _rgba(c_surface, 0.82)

        self.setStyleSheet(f"""
            QFrame#runtimeCard {{
                border: 2px solid {_rgba(c_border, 0.80)};
                border-radius: 14px;
                background-color: {card_bg};
                padding: 6px;
            }}
            QWidget {{
                background: transparent;
            }}
            QLabel#runtimeCardExchange {{
                color: {c_muted};
                font-size: 8px;
                font-weight: 600;
                padding-left: 1px;
            }}
            QLabel#runtimeCardSymbol {{
                color: {c_primary};
                font-size: 12px;
                font-weight: 700;
            }}
            QLabel#runtimeCardVs {{
                color: {c_muted};
                font-size: 13px;
                font-weight: 600;
            }}
            QWidget#runtimeStatusWidget {{
                background-color: {_rgba(c_window, 0.72)};
                border: 1px solid {_rgba(c_border, 0.56)};
                border-radius: 12px;
            }}
            QLabel#runtimeStatusDot {{
                background-color: #22c55e;
                border: 1px solid #16a34a;
                border-radius: 4px;
                min-width: 8px; max-width: 8px;
                min-height: 8px; max-height: 8px;
            }}
            QLabel#runtimeStatusLabel {{
                color: {c_primary};
                font-size: 10px;
                font-weight: 600;
            }}
            QFrame#runtimeParamCapsule {{
                background-color: {capsule_bg};
                border: 1px solid {_rgba(c_border, 0.60)};
                border-radius: 8px;
            }}
            QLabel#runtimeParamKey {{
                color: {c_muted};
                font-size: 10px;
                font-weight: 600;
            }}
            QLabel#runtimeParamValue {{
                color: {c_primary};
                font-size: 11px;
                font-weight: 700;
            }}
            QPushButton#runtimeStopButton {{
                background-color: {_rgba(c_danger, 0.16)};
                color: {c_primary};
                border: 1px solid {_rgba(c_danger, 0.54)};
                border-radius: 10px;
                padding: 4px 10px;
                font-weight: 600;
                font-size: 11px;
            }}
            QPushButton#runtimeStopButton:hover {{
                background-color: {_rgba(c_danger, 0.22)};
            }}
            QPushButton#runtimeStopButton:pressed {{
                background-color: {_rgba(c_surface_alt, 0.95)};
            }}
            QPushButton#runtimeStopButton:disabled {{
                color: {c_muted};
                background-color: {_rgba(c_surface, 0.55)};
                border-color: {_rgba(c_border, 0.30)};
            }}
        """)


def _rgba(hex_color: str, alpha: float) -> str:
    h = str(hex_color or "#000000").lstrip("#")
    if len(h) == 3:
        h = "".join(c * 2 for c in h)
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"rgba({r}, {g}, {b}, {alpha:.2f})"

