from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QFrame,
    QHBoxLayout,
    QLabel,
    QVBoxLayout,
    QWidget,
)

from app.ui.theme import theme_color


_STATUS_COLUMN_SPECS = (
    (
        "Общий статус",
        (
            ("Фаза стратегии", "spread.status.phase_value"),
            ("Блокировка", "spread.status.block_value"),
            ("Статус циклов", "spread.status.cycle_value"),
            ("Recovery hedge", "spread.status.recovery_hedge_value"),
            ("Глобальный hedge", "spread.status.global_hedge_value"),
        ),
    ),
    (
        "Левая нога",
        (
            ("Статус ордера", "spread.status.left_order_value"),
            ("Открытый объём", "spread.status.left_position_value"),
            ("Номинал (USDT)", "spread.status.left_notional_value"),
            ("Тайминги ордера", "spread.status.left_timing_value"),
            ("Exec stream connected", "spread.status.exec_connected_value"),
            ("Exec stream auth", "spread.status.exec_auth_value"),
        ),
    ),
    (
        "Правая нога",
        (
            ("Статус ордера", "spread.status.right_order_value"),
            ("Открытый объём", "spread.status.right_position_value"),
            ("Номинал (USDT)", "spread.status.right_notional_value"),
            ("Тайминги ордера", "spread.status.right_timing_value"),
            ("Exec stream reconnects", "spread.status.exec_reconnects_value"),
            ("Exec stream last error", "spread.status.exec_last_error_value"),
        ),
    ),
)


class DiagnosticsWindow(QWidget):
    """Floating diagnostics window for entry status metrics."""

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent, Qt.WindowType.Window)
        self.setWindowTitle("Диагностика")
        self.setMinimumSize(700, 340)
        self.resize(780, 400)

        self._labels: dict[str, QLabel] = {}

        root = QVBoxLayout(self)
        root.setContentsMargins(10, 10, 10, 10)
        root.setSpacing(8)

        columns_layout = QHBoxLayout()
        columns_layout.setSpacing(8)

        for column_title, fields in _STATUS_COLUMN_SPECS:
            col_frame = QFrame()
            col_frame.setObjectName("entryStatusColumn")
            col_layout = QVBoxLayout(col_frame)
            col_layout.setContentsMargins(8, 6, 8, 6)
            col_layout.setSpacing(6)

            title_label = QLabel(column_title)
            title_label.setObjectName("entryStatusColumnTitle")
            col_layout.addWidget(title_label)

            for label_text, value_key in fields:
                metric = QFrame()
                metric.setObjectName("entryStatusMetric")
                m_layout = QVBoxLayout(metric)
                m_layout.setContentsMargins(6, 4, 6, 4)
                m_layout.setSpacing(2)

                key_lbl = QLabel(label_text)
                key_lbl.setObjectName("entryStatusKey")
                val_lbl = QLabel("--")
                val_lbl.setObjectName("entryStatusValue")

                m_layout.addWidget(key_lbl)
                m_layout.addWidget(val_lbl)
                self._labels[value_key] = val_lbl
                col_layout.addWidget(metric)

            col_layout.addStretch(1)
            columns_layout.addWidget(col_frame, 1)

        root.addLayout(columns_layout)
        root.addStretch(1)

        self.apply_theme()

    @property
    def status_labels(self) -> dict[str, QLabel]:
        return self._labels

    def apply_entry_values(self, entry_values: dict[str, str]) -> None:
        for key, value in entry_values.items():
            lbl = self._labels.get(key)
            if lbl is not None and lbl.text() != value:
                lbl.setText(value)

    def apply_execution_stream_health_tone(self, status: str) -> None:
        tone = _health_status_to_tone(status)
        for key in (
            "spread.status.exec_connected_value",
            "spread.status.exec_auth_value",
            "spread.status.exec_reconnects_value",
            "spread.status.exec_last_error_value",
        ):
            lbl = self._labels.get(key)
            if lbl is None:
                continue
            if str(lbl.property("healthTone") or "unknown") == tone:
                continue
            lbl.setProperty("healthTone", tone)
            lbl.style().unpolish(lbl)
            lbl.style().polish(lbl)
            lbl.update()

    def apply_theme(self) -> None:
        c_surface = theme_color("surface")
        c_window = theme_color("window_bg")
        c_border = theme_color("border")
        c_primary = theme_color("text_primary")
        c_muted = theme_color("text_muted")

        self.setStyleSheet(f"""
            DiagnosticsWindow {{
                background-color: {c_window};
            }}
            QFrame#entryStatusColumn {{
                background-color: {_rgba(c_surface, 0.58)};
                border: 1px solid {_rgba(c_border, 0.44)};
                border-radius: 8px;
            }}
            QLabel#entryStatusColumnTitle {{
                color: {c_primary};
                font-size: 10px;
                font-weight: 700;
            }}
            QFrame#entryStatusMetric {{
                background-color: {_rgba(c_surface, 0.46)};
                border: 1px solid {_rgba(c_border, 0.34)};
                border-radius: 6px;
            }}
            QLabel#entryStatusKey {{
                color: {c_muted};
                font-size: 10px;
                font-weight: 600;
            }}
            QLabel#entryStatusValue {{
                color: {c_primary};
                font-size: 11px;
                font-weight: 700;
            }}
            QLabel#entryStatusValue[healthTone="healthy"] {{
                color: {theme_color('success')};
            }}
            QLabel#entryStatusValue[healthTone="degraded"] {{
                color: {theme_color('warning')};
            }}
            QLabel#entryStatusValue[healthTone="disconnected"] {{
                color: {theme_color('danger')};
            }}
            QLabel#entryStatusValue[healthTone="unknown"] {{
                color: {c_muted};
            }}
        """)


def _health_status_to_tone(status: str) -> str:
    normalized = str(status or "").strip().upper()
    if normalized == "HEALTHY":
        return "healthy"
    if normalized == "DEGRADED":
        return "degraded"
    if normalized == "DISCONNECTED":
        return "disconnected"
    return "unknown"


def _rgba(hex_color: str, alpha: float) -> str:
    h = str(hex_color or "#000000").lstrip("#")
    if len(h) == 3:
        h = "".join(c * 2 for c in h)
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"rgba({r}, {g}, {b}, {alpha:.2f})"
