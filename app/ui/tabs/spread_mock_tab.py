from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
from typing import Any

from PySide6.QtCore import QPoint, QSize, QStringListModel, Qt, QTimer, Signal
from PySide6.QtGui import QAction, QCloseEvent, QColor, QFocusEvent, QIcon, QMouseEvent, QPainter, QPen, QPixmap
from PySide6.QtWidgets import QApplication, QCompleter, QFrame, QGridLayout, QHBoxLayout, QLabel, QLineEdit, QMenu, QPushButton, QScrollArea, QSizePolicy, QStyle, QVBoxLayout, QWidget

from app.core.application.spread import SpreadTableService, SpreadView
from app.ui.exchange_store import load_exchange_cards, resolve_exchange_card_credentials
from app.ui.i18n import tr
from app.ui.theme import theme_color
from app.ui.widgets.exchange_badge import build_exchange_icon
from app.ui.widgets.runtime_card import RuntimeCard
from app.ui.windows.diagnostics_window import DiagnosticsWindow


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


class SpreadMockTab(QWidget):
    action_triggered = Signal(str)
    _STATE_PATH = Path(__file__).resolve().parents[2] / "data" / "ui_state.json"

    def __init__(self, coordinator=None, parent=None) -> None:
        super().__init__(parent)
        self.setFocusPolicy(Qt.FocusPolicy.StrongFocus)
        self.coordinator = coordinator
        self._spread_service: SpreadTableService | None = None
        if coordinator is not None and getattr(coordinator, "core_app", None) is not None:
            try:
                self._spread_service = coordinator.core_app.get_spread_table_service()
            except Exception:
                self._spread_service = None
        self._slot_state = {
            "left": {"exchange": None, "market_type": None, "symbol": None},
            "right": {"exchange": None, "market_type": None, "symbol": None},
        }
        self._exchange_buttons: dict[str, QPushButton] = {}
        self._market_type_buttons: dict[str, QPushButton] = {}
        self._pair_inputs: dict[str, PairLineEdit] = {}
        self._pair_display_maps: dict[str, dict[str, str]] = {}
        self._pair_clear_actions: dict[str, QAction] = {}
        self._pair_models: dict[str, QStringListModel] = {}
        self._pair_completers: dict[str, QCompleter] = {}
        self._quote_labels: dict[str, dict[str, QLabel]] = {}
        self._transport_widgets: dict[str, dict[str, object]] = {}
        self._pending_quotes: dict[str, dict] = {}
        self._latest_public_quotes: dict[str, dict[str, Decimal]] = {}
        self._pending_spread_state: dict | None = None
        self._strategy_field_labels: list[QLabel] = []
        self._strategy_inputs: dict[str, QLineEdit] = {}
        self._spread_value_label: QLabel | None = None
        self._select_button: QPushButton | None = None
        self._start_button: QPushButton | None = None
        self._stop_button: QPushButton | None = None
        self._signal_mode_button: QPushButton | None = None
        self._simulate_entry_button: QPushButton | None = None
        self._simulate_exit_button: QPushButton | None = None
        self._strategy_title_label: QLabel | None = None
        self._entry_status_title_label: QLabel | None = None
        self._entry_status_labels: dict[str, QLabel] = {}
        self._diagnostics_window: DiagnosticsWindow | None = None
        self._runtime_cards: dict[str, RuntimeCard] = {}
        self._active_worker_id: str | None = None
        self._runtimes_layout: QVBoxLayout | None = None
        self._runtime_running = False
        self._strategy_signal_mode = "market"
        self._simulated_entry_window_open = False
        self._simulated_exit_window_open = False
        self._ui_quote_timer = QTimer(self)
        self._ui_quote_timer.setInterval(200)
        self._ui_quote_timer.timeout.connect(self._flush_pending_quotes)
        self._ui_spread_timer = QTimer(self)
        self._ui_spread_timer.setInterval(150)
        self._prev_controls_state: dict[str, object] = {}
        self._ui_spread_timer.timeout.connect(self._flush_pending_spread_state)
        self._build_ui()
        app = QApplication.instance()
        if app is not None:
            app.installEventFilter(self)
        self.apply_theme()
        self.retranslate_ui()
        if self.coordinator is not None:
            self.coordinator.public_quote_received.connect(self._on_public_quote_received)
            self.coordinator.public_quote_error.connect(self._on_public_quote_error)
            self.coordinator.instruments_loaded.connect(self._on_instruments_loaded)
            self.coordinator.worker_state_updated.connect(self._on_worker_state_updated)
            self.coordinator.worker_command_failed.connect(self._on_worker_command_failed)
        self._restore_saved_state()

    @staticmethod
    def _rgba(hex_color: str, alpha: float) -> str:
        color = str(hex_color or "").strip()
        if color.startswith("#") and len(color) == 7:
            r = int(color[1:3], 16)
            g = int(color[3:5], 16)
            b = int(color[5:7], 16)
            a = max(0, min(255, int(round(max(0.0, min(1.0, alpha)) * 255))))
            return f"rgba({r}, {g}, {b}, {a})"
        return color

    def _emit(self, name: str) -> None:
        self.action_triggered.emit(name)

    def eventFilter(self, watched, event):
        if event.type() == event.Type.MouseButtonPress:
            for slot_name, pair_input in self._pair_inputs.items():
                popup = self._pair_completers[slot_name].popup()
                if not pair_input.hasFocus() and not popup.isVisible():
                    continue
                try:
                    global_pos = event.globalPosition().toPoint()
                except AttributeError:
                    global_pos = event.globalPos()
                pair_local = pair_input.mapFromGlobal(global_pos)
                popup_local = popup.mapFromGlobal(global_pos)
                if pair_input.rect().contains(pair_local) or popup.rect().contains(popup_local):
                    break
                pair_input.clearFocus()
                pair_input.setReadOnly(True)
                pair_input.deselect()
                pair_input.setCursorPosition(0)
                popup.hide()
                self.setFocus(Qt.FocusReason.MouseFocusReason)
                break
        return super().eventFilter(watched, event)

    def closeEvent(self, event: QCloseEvent) -> None:
        app = QApplication.instance()
        if app is not None:
            app.removeEventFilter(self)
        if self.coordinator is not None:
            try:
                self.coordinator.public_quote_received.disconnect(self._on_public_quote_received)
                self.coordinator.public_quote_error.disconnect(self._on_public_quote_error)
                self.coordinator.instruments_loaded.disconnect(self._on_instruments_loaded)
                self.coordinator.worker_state_updated.disconnect(self._on_worker_state_updated)
                self.coordinator.worker_command_failed.disconnect(self._on_worker_command_failed)
            except Exception:
                pass
        super().closeEvent(event)

    def _on_public_quote_error(self, message: str) -> None:
        self._emit(f"spread:error:{message}")

    def _save_state(self) -> None:
        self._normalize_strategy_inputs()
        payload = {
            "spread_slots": {
                slot_name: {
                    "exchange": state["exchange"],
                    "market_type": state["market_type"],
                    "symbol": state["symbol"],
                }
                for slot_name, state in self._slot_state.items()
            },
            "spread_strategy": {
                key: field.text().strip()
                for key, field in self._strategy_inputs.items()
            },
        }
        self._STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        self._STATE_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _load_saved_state(self) -> dict[str, dict[str, str | None]]:
        if not self._STATE_PATH.exists():
            return {}
        try:
            payload = json.loads(self._STATE_PATH.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError):
            return {}
        slots = payload.get("spread_slots")
        strategy = payload.get("spread_strategy")
        if not isinstance(slots, dict):
            slots = {}
        result: dict[str, dict[str, str | None]] = {}
        for slot_name in ("left", "right"):
            raw_state = slots.get(slot_name)
            if not isinstance(raw_state, dict):
                continue
            result[slot_name] = {
                "exchange": raw_state.get("exchange"),
                "market_type": raw_state.get("market_type"),
                "symbol": raw_state.get("symbol"),
            }
        if isinstance(strategy, dict):
            result["__strategy__"] = {
                str(key): str(value)
                for key, value in strategy.items()
            }
        return result

    def _restore_saved_state(self) -> None:
        if self.coordinator is None:
            return
        saved = self._load_saved_state()
        if not saved:
            return
        strategy_saved = saved.get("__strategy__") or {}
        if isinstance(strategy_saved, dict):
            for key, field in self._strategy_inputs.items():
                if key in strategy_saved:
                    field.setText(str(strategy_saved[key]))
        self._normalize_strategy_inputs()
        self._strategy_signal_mode = "market"
        exchanges = dict(self.coordinator.available_quote_exchanges())
        for slot_name in ("left", "right"):
            state = saved.get(slot_name) or {}
            exchange = state.get("exchange")
            market_type = state.get("market_type")
            symbol = state.get("symbol")
            if exchange and exchange in exchanges:
                self._slot_state[slot_name]["exchange"] = exchange
                self._apply_exchange_button_state(slot_name, exchange, exchanges[exchange])
                self._update_transport_widget(slot_name)
            if exchange and market_type:
                market_types = dict(self.coordinator.list_market_types(exchange))
                market_title = market_types.get(market_type)
                if market_title:
                    self._slot_state[slot_name]["market_type"] = market_type
                    self._market_type_buttons[slot_name].setText(market_title)
                    self._pair_inputs[slot_name].setPlaceholderText(tr("spread.enter_symbol"))
            if exchange and market_type and symbol:
                self._slot_state[slot_name]["symbol"] = symbol
                pair_input = self._pair_inputs[slot_name]
                pair_input.setText(self.coordinator.display_symbol(exchange, market_type, symbol))
                pair_input.mark_selected_value()
                self._update_pair_clear_action(slot_name)
            self._clear_quotes(slot_name)
        self._update_runtime_controls()

    @staticmethod
    def _format_ui_qty(value: object) -> str:
        try:
            amount = float(value)
        except (TypeError, ValueError):
            return "--"

        abs_amount = abs(amount)
        if abs_amount >= 1_000_000:
            return f"{amount / 1_000_000:.2f}".rstrip("0").rstrip(".") + "M"
        if abs_amount >= 1_000:
            return f"{amount / 1_000:.2f}".rstrip("0").rstrip(".") + "K"
        if amount.is_integer():
            return str(int(amount))
        return f"{amount:.3f}".rstrip("0").rstrip(".")

    @classmethod
    def _format_ui_notional_usdt(cls, price_value: object, qty_value: object) -> str:
        try:
            price = float(price_value)
            qty = float(qty_value)
        except (TypeError, ValueError):
            return "--"
        return cls._format_ui_qty(price * qty)

    def _build_menu(self) -> QMenu:
        menu = QMenu(self)
        menu.setStyleSheet(
            f"""
            QMenu {{
                background: qlineargradient(
                    x1: 0, y1: 0, x2: 0, y2: 1,
                    stop: 0 {self._rgba(theme_color('surface_alt'), 0.96)},
                    stop: 1 {self._rgba(theme_color('window_bg'), 0.98)}
                );
                color: {theme_color('text_primary')};
                border: 1px solid {self._rgba(theme_color('border'), 0.62)};
                border-radius: 10px;
                padding: 5px;
                font-size: 11px;
                font-weight: 700;
            }}
            QMenu::item {{
                padding: 7px 10px;
                border-radius: 7px;
                margin: 2px 0;
            }}
            QMenu::item:selected {{
                background-color: {self._rgba(theme_color('accent'), 0.18)};
                border: 1px solid {self._rgba(theme_color('accent'), 0.44)};
                color: {theme_color('text_primary')};
            }}
            """
        )
        return menu

    @staticmethod
    def _build_clear_icon() -> QIcon:
        pixmap = QPixmap(12, 12)
        pixmap.fill(Qt.GlobalColor.transparent)
        painter = QPainter(pixmap)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        pen = QPen(QColor("#d8e1ea"), 1.8, Qt.PenStyle.SolidLine, Qt.PenCapStyle.RoundCap)
        painter.setPen(pen)
        painter.drawLine(3, 3, 9, 9)
        painter.drawLine(9, 3, 3, 9)
        painter.end()
        return QIcon(pixmap)

    def _show_menu(self, anchor: QWidget, items: list[tuple], callback) -> None:
        if not items:
            return
        menu = self._build_menu()
        for item in items:
            if len(item) == 3:
                value, title, icon_code = item
            else:
                value, title = item
                icon_code = None
            action = menu.addAction(title)
            if icon_code:
                action.setIcon(build_exchange_icon(icon_code, size=18))
            action.triggered.connect(lambda _checked=False, selected=value, selected_title=title: callback(selected, selected_title))
        menu.setFixedWidth(anchor.width())
        menu.exec(anchor.mapToGlobal(QPoint(0, anchor.height())))

    def _make_selector_button(self, text: str, object_name: str, slot_name: str, handler) -> QPushButton:
        button = QPushButton(text)
        button.setObjectName(object_name)
        if object_name == "exchangeSelector":
            button.setProperty("toneRole", "neutral")
        button.setFixedWidth(230)
        button.clicked.connect(lambda _checked=False, slot=slot_name: handler(slot))
        return button

    def _apply_exchange_button_state(self, slot_name: str, exchange_code: str | None, title: str) -> None:
        button = self._exchange_buttons[slot_name]
        button.setText(title)
        if exchange_code:
            button.setIcon(build_exchange_icon(exchange_code, size=18))
            button.setIconSize(QSize(18, 18))
        else:
            button.setIcon(QIcon())

    def _build_leg_column(self, slot_name: str) -> QWidget:
        box = QWidget()
        col = QVBoxLayout(box)
        col.setContentsMargins(0, 0, 0, 0)
        col.setSpacing(6)

        exchange_btn = self._make_selector_button(tr("spread.choose_exchange"), "exchangeSelector", slot_name, self._open_exchange_menu)
        market_type_btn = self._make_selector_button(tr("spread.choose_type"), "subSelector", slot_name, self._open_market_type_menu)

        pair_input = PairLineEdit()
        pair_input.setObjectName("pairSelector")
        pair_input.setFixedWidth(230)
        pair_input.setPlaceholderText(tr("spread.choose_instrument"))
        pair_input.textEdited.connect(lambda text, slot=slot_name: self._on_pair_text_edited(slot, text))
        pair_input.returnPressed.connect(lambda slot=slot_name: self._accept_pair_text(slot))
        pair_input.field_focused.connect(lambda slot=slot_name: self._on_pair_field_focused(slot))
        pair_input.field_clicked.connect(lambda slot=slot_name: self._on_pair_field_clicked(slot))
        pair_input.field_blurred.connect(lambda slot=slot_name: self._on_pair_field_blurred(slot))

        clear_icon = self._build_clear_icon()
        clear_action = QAction(clear_icon, "", pair_input)
        clear_action.setToolTip(tr("common.cancel"))
        pair_input.addAction(clear_action, QLineEdit.ActionPosition.TrailingPosition)
        clear_action.setVisible(False)
        clear_action.triggered.connect(lambda _checked=False, slot=slot_name: self._clear_symbol(slot))

        pair_model = QStringListModel(self)
        pair_completer = QCompleter(pair_model, self)
        pair_completer.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
        pair_completer.setCompletionMode(QCompleter.CompletionMode.PopupCompletion)
        pair_completer.setFilterMode(Qt.MatchFlag.MatchContains)
        pair_completer.setMaxVisibleItems(5)
        pair_completer.activated.connect(lambda text, slot=slot_name: self._select_pair_display(slot, text))
        pair_input.setCompleter(pair_completer)
        pair_completer.popup().setStyleSheet(
            f"""
            QListView {{
                background: qlineargradient(
                    x1: 0, y1: 0, x2: 0, y2: 1,
                    stop: 0 {self._rgba(theme_color('surface_alt'), 0.98)},
                    stop: 0.52 {self._rgba(theme_color('surface'), 0.98)},
                    stop: 1 {self._rgba(theme_color('window_bg'), 0.99)}
                );
                color: {theme_color('text_primary')};
                border: 1px solid {self._rgba(theme_color('accent'), 0.48)};
                border-radius: 12px;
                padding: 6px;
                outline: none;
                font-size: 11px;
                font-weight: 700;
            }}
            QListView::item {{
                padding: 8px 10px;
                margin: 2px 0;
                border-radius: 8px;
            }}
            QListView::item:hover {{
                background: qlineargradient(
                    x1: 0, y1: 0, x2: 1, y2: 0,
                    stop: 0 {self._rgba(theme_color('accent'), 0.16)},
                    stop: 1 {self._rgba(theme_color('surface'), 0.18)}
                );
                border: 1px solid {self._rgba(theme_color('accent'), 0.38)};
            }}
            QListView::item:selected {{
                background: qlineargradient(
                    x1: 0, y1: 0, x2: 1, y2: 0,
                    stop: 0 {self._rgba(theme_color('accent'), 0.22)},
                    stop: 1 {self._rgba(theme_color('surface'), 0.24)}
                );
                border: 1px solid {self._rgba(theme_color('accent'), 0.54)};
                color: {theme_color('text_primary')};
            }}
            QScrollBar:vertical {{
                background: transparent;
                width: 8px;
                margin: 6px 4px 6px 0;
            }}
            QScrollBar::handle:vertical {{
                background: {self._rgba(theme_color('accent'), 0.34)};
                border-radius: 4px;
                min-height: 20px;
            }}
            QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{
                height: 0;
            }}
            """
        )

        self._exchange_buttons[slot_name] = exchange_btn
        self._market_type_buttons[slot_name] = market_type_btn
        self._pair_inputs[slot_name] = pair_input
        self._pair_display_maps[slot_name] = {}
        self._pair_clear_actions[slot_name] = clear_action
        self._pair_models[slot_name] = pair_model
        self._pair_completers[slot_name] = pair_completer

        pair_row = QGridLayout()
        pair_row.setContentsMargins(0, 0, 0, 0)
        pair_row.setHorizontalSpacing(8)
        pair_row.setVerticalSpacing(0)
        pair_row.setColumnMinimumWidth(0, 68)
        pair_row.setColumnMinimumWidth(2, 68)

        ws_widget = self._build_transport_widget(slot_name, "ws")
        rest_widget = self._build_transport_widget(slot_name, "rest")

        pair_row.addWidget(ws_widget, 0, 0, Qt.AlignmentFlag.AlignCenter)
        pair_row.addWidget(pair_input, 0, 1, Qt.AlignmentFlag.AlignCenter)
        pair_row.addWidget(rest_widget, 0, 2, Qt.AlignmentFlag.AlignCenter)

        col.addWidget(exchange_btn, 0, Qt.AlignmentFlag.AlignCenter)
        col.addWidget(market_type_btn, 0, Qt.AlignmentFlag.AlignCenter)
        col.addLayout(pair_row)
        return box

    def _build_transport_widget(self, slot_name: str, route_name: str) -> QWidget:
        widget = QWidget()
        widget.setObjectName("transportWidget")
        widget.setSizePolicy(QSizePolicy.Policy.Maximum, QSizePolicy.Policy.Fixed)
        widget.setFixedWidth(68)
        row = QHBoxLayout(widget)
        row.setContentsMargins(0, 0, 0, 0)
        row.setSpacing(4)

        dot = QLabel("•")
        dot.setObjectName("transportDot")

        button = QPushButton("WS" if route_name == "ws" else "REST")
        button.setObjectName("transportBadge")
        button.setCursor(Qt.CursorShape.PointingHandCursor)
        button.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        button.setFixedHeight(20)
        button.setFixedWidth(50)

        row.addWidget(dot, 0, Qt.AlignmentFlag.AlignVCenter)
        row.addWidget(button, 0, Qt.AlignmentFlag.AlignVCenter)

        slot_controls = self._transport_widgets.setdefault(slot_name, {})
        slot_controls[f"{route_name}_widget"] = widget
        slot_controls[f"{route_name}_dot"] = dot
        slot_controls[f"{route_name}_button"] = button
        self._update_transport_widget(slot_name)
        return widget

    def _transport_state_for_exchange(self, exchange: str | None) -> dict[str, dict[str, str | bool]]:
        default = {
            "ws": {"active": False, "available": False, "reason": tr("spread.transport.choose_exchange")},
            "rest": {"active": False, "available": False, "reason": tr("spread.transport.choose_exchange")},
        }
        if not exchange:
            return default
        normalized = str(exchange).strip().lower()
        if normalized == "binance":
            return {
                "ws": {"active": True, "available": True, "reason": ""},
                "rest": {"active": False, "available": False, "reason": tr("spread.transport.binance_rest_unavailable")},
            }
        if normalized == "bitget":
            account_profile = self._find_connected_account_profile("bitget")
            is_uta = str(account_profile.get("account_type", "")).strip().lower() == "uta"
            return {
                "ws": {
                    "active": False if not is_uta else True,
                    "available": bool(is_uta),
                    "reason": "" if is_uta else tr("spread.transport.bitget_ws_unavailable"),
                },
                "rest": {"active": True if not is_uta else False, "available": True, "reason": ""},
            }
        if normalized == "bybit":
            return {
                "ws": {"active": False, "available": False, "reason": tr("spread.transport.bybit_ws_unavailable")},
                "rest": {"active": False, "available": False, "reason": tr("spread.transport.bybit_rest_unavailable")},
            }
        return default

    @staticmethod
    def _find_connected_account_profile(exchange_code: str) -> dict:
        for card in load_exchange_cards():
            if str(card.get("exchange_code", "")).strip().lower() != str(exchange_code or "").strip().lower():
                continue
            if not bool(card.get("connected")):
                continue
            return dict(card.get("account_snapshot", {}).get("account_profile", {}) or {})
        return {}

    def _update_transport_widget(self, slot_name: str) -> None:
        controls = self._transport_widgets.get(slot_name)
        required_keys = {
            "ws_widget",
            "rest_widget",
            "ws_dot",
            "rest_dot",
            "ws_button",
            "rest_button",
        }
        if not controls or not required_keys.issubset(controls):
            return
        exchange = self._slot_state[slot_name]["exchange"]
        state = self._transport_state_for_exchange(exchange)
        ws_state = state["ws"]
        rest_state = state["rest"]

        controls["ws_widget"].setVisible(bool(exchange))
        controls["rest_widget"].setVisible(bool(exchange))
        self._apply_transport_badge_state(
            dot=controls["ws_dot"],
            button=controls["ws_button"],
            label="WS",
            active=bool(ws_state["active"]),
            available=bool(ws_state["available"]),
            reason=str(ws_state["reason"] or ""),
        )
        self._apply_transport_badge_state(
            dot=controls["rest_dot"],
            button=controls["rest_button"],
            label="REST",
            active=bool(rest_state["active"]),
            available=bool(rest_state["available"]),
            reason=str(rest_state["reason"] or ""),
        )

    def _apply_transport_badge_state(
        self,
        *,
        dot: QLabel,
        button: QPushButton,
        label: str,
        active: bool,
        available: bool,
        reason: str,
    ) -> None:
        button.setText(label)
        button.setProperty("activeState", "active" if active else "inactive")
        button.setEnabled(available)
        button.setToolTip("" if available or not reason else reason)
        dot.setProperty("activeState", "active" if active else "inactive")
        dot.style().unpolish(dot)
        dot.style().polish(dot)
        button.style().unpolish(button)
        button.style().polish(button)

    def _build_quote_panel(self, slot_name: str) -> QWidget:
        panel = QWidget()
        panel.setObjectName("quotePanel")
        row = QHBoxLayout(panel)
        row.setContentsMargins(0, 0, 0, 0)
        row.setSpacing(10)
        labels: dict[str, QLabel] = {}
        for side_name in ("bid", "ask"):
            capsule = QFrame()
            capsule.setObjectName("quoteSideCapsule")
            capsule_layout = QHBoxLayout(capsule)
            capsule_layout.setContentsMargins(8, 6, 8, 6)
            capsule_layout.setSpacing(0)

            price_block = QWidget()
            price_block_layout = QHBoxLayout(price_block)
            price_block_layout.setContentsMargins(8, 0, 8, 0)
            price_block_layout.setSpacing(6)
            price_prefix = QLabel(tr("spread.bid") if side_name == "bid" else tr("spread.ask"))
            price_prefix.setObjectName("bidLabelText" if side_name == "bid" else "askLabelText")
            price_prefix.setAlignment(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignLeft)
            price_prefix.setFixedWidth(32)
            price_value = QLabel("-")
            price_value.setObjectName("quoteValueText")
            price_value.setAlignment(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignLeft)
            price_value.setMinimumWidth(64)
            price_block_layout.addWidget(price_prefix, 0)
            price_block_layout.addWidget(price_value, 1)

            divider = QFrame()
            divider.setObjectName("quoteMidDivider")
            divider.setFixedWidth(1)

            qty_block = QWidget()
            qty_block_layout = QHBoxLayout(qty_block)
            qty_block_layout.setContentsMargins(8, 0, 8, 0)
            qty_block_layout.setSpacing(6)
            qty_prefix = QLabel(tr("spread.qty"))
            qty_prefix.setObjectName("quoteQtyLabelText")
            qty_prefix.setAlignment(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignLeft)
            qty_prefix.setFixedWidth(50)
            qty_value = QLabel("- USDT")
            qty_value.setObjectName("quoteValueText")
            qty_value.setAlignment(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignRight)
            qty_value.setMinimumWidth(92)
            qty_block_layout.addWidget(qty_prefix, 0)
            qty_block_layout.addWidget(qty_value, 1)

            labels[f"{side_name}_price_prefix"] = price_prefix
            labels[f"{side_name}_price"] = price_value
            labels[f"{side_name}_qty_prefix"] = qty_prefix
            labels[f"{side_name}_qty"] = qty_value
            capsule_layout.addWidget(price_block, 1)
            capsule_layout.addWidget(divider, 0)
            capsule_layout.addWidget(qty_block, 1)
            row.addWidget(capsule, 1)
        self._quote_labels[slot_name] = labels
        return panel

    def _open_exchange_menu(self, slot_name: str) -> None:
        if self.coordinator is None:
            return
        items = [(code, title, code) for code, title in self.coordinator.available_quote_exchanges()]
        self._show_menu(self._exchange_buttons[slot_name], items, lambda value, title: self._set_exchange(slot_name, value, title))

    def _open_market_type_menu(self, slot_name: str) -> None:
        if self.coordinator is None:
            return
        exchange = self._slot_state[slot_name]["exchange"]
        if not exchange:
            return
        items = self.coordinator.list_market_types(exchange)
        self._show_menu(self._market_type_buttons[slot_name], items, lambda value, title: self._set_market_type(slot_name, value, title))

    def _on_pair_text_edited(self, slot_name: str, text: str) -> None:
        self._slot_state[slot_name]["symbol"] = None
        self._pair_inputs[slot_name].clear_selected_value_lock()
        if self.coordinator is not None:
            self.coordinator.unsubscribe_public_quote(slot_name)
        self._update_pair_clear_action(slot_name)
        self._update_pair_suggestions(slot_name, text, force_popup=True)

    def _on_pair_field_focused(self, slot_name: str) -> None:
        self._update_pair_clear_action(slot_name)
        if self._pair_inputs[slot_name].has_locked_selection():
            self._pair_completers[slot_name].popup().hide()
            return
        self._show_top_instruments(slot_name)

    def _on_pair_field_clicked(self, slot_name: str) -> None:
        self._update_pair_clear_action(slot_name)
        if self._pair_inputs[slot_name].has_locked_selection():
            self._pair_completers[slot_name].popup().hide()
            return
        self._show_top_instruments(slot_name)

    def _on_pair_field_blurred(self, slot_name: str) -> None:
        symbol = self._slot_state[slot_name]["symbol"]
        pair_input = self._pair_inputs[slot_name]
        exchange = self._slot_state[slot_name]["exchange"]
        market_type = self._slot_state[slot_name]["market_type"]
        if symbol and self.coordinator is not None and exchange and market_type:
            display_symbol = self.coordinator.display_symbol(exchange, market_type, str(symbol))
            if pair_input.text().strip().upper() != display_symbol.upper():
                pair_input.setText(display_symbol)
                pair_input.mark_selected_value()
        elif symbol and pair_input.text().strip().upper() != str(symbol).upper():
            pair_input.setText(str(symbol))
            pair_input.mark_selected_value()
        self._update_pair_clear_action(slot_name)

    def _update_pair_clear_action(self, slot_name: str) -> None:
        action = self._pair_clear_actions.get(slot_name)
        if action is None:
            return
        pair_input = self._pair_inputs[slot_name]
        action.setVisible(pair_input.has_locked_selection() and pair_input.hasFocus())

    def _update_pair_suggestions(self, slot_name: str, text: str, force_popup: bool = False) -> None:
        state = self._slot_state[slot_name]
        if self.coordinator is None:
            return
        exchange = state["exchange"]
        market_type = state["market_type"]
        if not exchange or not market_type:
            return
        query = str(text or "").strip().upper()
        items = self.coordinator.list_instrument_items(exchange, market_type)
        display_map: dict[str, str] = {}
        usdt_displays: list[str] = []
        for item in items:
            display_value = str(item.get("display", "")).strip().upper()
            actual_symbol = str(item.get("symbol", "")).strip().upper()
            quote_asset = str(item.get("quote_asset", "")).strip().upper()
            if display_value and actual_symbol and display_value not in display_map:
                display_map[display_value] = actual_symbol
                if quote_asset == "USDT":
                    usdt_displays.append(display_value)
        self._pair_display_maps[slot_name] = display_map
        displays = list(display_map.keys())
        if query:
            starts_with = [
                display
                for display in displays
                if display.startswith(query) or display_map.get(display, "") == query
            ]
            contains = [
                display
                for display in displays
                if (query in display or query in display_map.get(display, "")) and display not in starts_with
            ]
            filtered = [*starts_with, *contains][:5]
        else:
            filtered = usdt_displays[:5] or displays[:5]

        model = self._pair_models[slot_name]
        model.setStringList(filtered)
        completer = self._pair_completers[slot_name]
        completer.popup().setFixedWidth(self._pair_inputs[slot_name].width())
        if filtered and force_popup:
            completer.setCompletionPrefix(query)
            completer.complete()
        elif not filtered:
            completer.popup().hide()

    def _show_top_instruments(self, slot_name: str) -> None:
        pair_input = self._pair_inputs.get(slot_name)
        if pair_input is None or pair_input.text().strip():
            return
        self._update_pair_suggestions(slot_name, "", force_popup=True)

    def _accept_pair_text(self, slot_name: str) -> None:
        pair_input = self._pair_inputs[slot_name]
        exchange = self._slot_state[slot_name]["exchange"]
        market_type = self._slot_state[slot_name]["market_type"]
        if self.coordinator is None or not exchange or not market_type:
            return
        symbol = self.coordinator.resolve_instrument_symbol(exchange, market_type, pair_input.text())
        if symbol:
            self._set_symbol(slot_name, symbol, self.coordinator.display_symbol(exchange, market_type, symbol))

    def _select_pair_display(self, slot_name: str, display_value: str) -> None:
        exchange = self._slot_state[slot_name]["exchange"]
        market_type = self._slot_state[slot_name]["market_type"]
        if self.coordinator is None or not exchange or not market_type:
            return
        display_text = str(display_value or "").strip().upper()
        symbol = self._pair_display_maps.get(slot_name, {}).get(display_text)
        if not symbol:
            symbol = self.coordinator.resolve_instrument_symbol(exchange, market_type, display_text)
        if symbol:
            self._set_symbol(slot_name, symbol, self.coordinator.display_symbol(exchange, market_type, symbol))

    def _set_exchange(self, slot_name: str, exchange: str, title: str) -> None:
        if self.coordinator is not None:
            self.coordinator.unsubscribe_public_quote(slot_name)
        self._slot_state[slot_name]["exchange"] = exchange
        self._slot_state[slot_name]["market_type"] = None
        self._slot_state[slot_name]["symbol"] = None
        self._apply_exchange_button_state(slot_name, exchange, title)
        self._market_type_buttons[slot_name].setText(tr("spread.choose_type"))
        self._pair_inputs[slot_name].clear()
        self._pair_inputs[slot_name].clear_selected_value_lock()
        self._pair_inputs[slot_name].setPlaceholderText(tr("spread.choose_instrument"))
        self._update_pair_clear_action(slot_name)
        self._update_transport_widget(slot_name)
        self._clear_quotes(slot_name)
        if self.coordinator is not None:
            self.coordinator.prefetch_exchange_instruments(exchange)
        self._save_state()
        self._sync_spread_view_with_slots()
        self._emit(f"spread:exchange:{slot_name}:{exchange}")

    def _set_market_type(self, slot_name: str, market_type: str, title: str) -> None:
        if self.coordinator is not None:
            self.coordinator.unsubscribe_public_quote(slot_name)
        self._slot_state[slot_name]["market_type"] = market_type
        self._slot_state[slot_name]["symbol"] = None
        self._market_type_buttons[slot_name].setText(title)
        self._pair_inputs[slot_name].clear()
        self._pair_inputs[slot_name].clear_selected_value_lock()
        self._pair_inputs[slot_name].setPlaceholderText(tr("spread.enter_symbol"))
        self._update_pair_clear_action(slot_name)
        self._clear_quotes(slot_name)
        exchange = self._slot_state[slot_name]["exchange"]
        if self.coordinator is not None and exchange:
            self.coordinator.prefetch_market_type(exchange, market_type)
        self._save_state()
        self._sync_spread_view_with_slots()
        self._emit(f"spread:market_type:{slot_name}:{market_type}")

    def _set_symbol(self, slot_name: str, symbol: str, title: str) -> None:
        self._slot_state[slot_name]["symbol"] = symbol
        pair_input = self._pair_inputs[slot_name]
        pair_input.setText(title)
        pair_input.mark_selected_value()
        self._pair_completers[slot_name].popup().hide()
        pair_input.clearFocus()
        self._update_pair_clear_action(slot_name)
        self._clear_quotes(slot_name)
        self._save_state()
        self._sync_spread_view_with_slots()
        self._emit(f"spread:instrument:{slot_name}:{symbol}")

    def _clear_symbol(self, slot_name: str) -> None:
        if self.coordinator is not None:
            self.coordinator.unsubscribe_public_quote(slot_name)
        self._slot_state[slot_name]["symbol"] = None
        pair_input = self._pair_inputs[slot_name]
        pair_input.clear()
        pair_input.clear_selected_value_lock()
        pair_input.setPlaceholderText(
            tr("spread.enter_symbol") if self._slot_state[slot_name]["market_type"] else tr("spread.choose_instrument")
        )
        self._pair_completers[slot_name].popup().hide()
        self._update_pair_clear_action(slot_name)
        self._clear_quotes(slot_name)
        self._save_state()
        self._sync_spread_view_with_slots()
        self._emit(f"spread:instrument_cleared:{slot_name}")
        self._update_pair_suggestions(slot_name, "", force_popup=True)

    def _on_instruments_loaded(self, exchange: str, market_type: str) -> None:
        for slot_name, state in self._slot_state.items():
            if state["exchange"] != exchange or state["market_type"] != market_type:
                continue
            pair_input = self._pair_inputs[slot_name]
            selected_symbol = state.get("symbol")
            if selected_symbol and self.coordinator is not None:
                pair_input.setText(self.coordinator.display_symbol(exchange, market_type, selected_symbol))
            if pair_input.hasFocus() and not pair_input.text().strip():
                self._update_pair_suggestions(slot_name, "", force_popup=True)

    def _subscribe_slot(self, slot_name: str) -> None:
        if self.coordinator is None:
            return
        state = self._slot_state[slot_name]
        exchange = state["exchange"]
        market_type = state["market_type"]
        symbol = state["symbol"]
        if not exchange or not market_type or not symbol:
            return
        self.coordinator.subscribe_public_quote(slot_name, exchange=exchange, market_type=market_type, symbol=symbol)
        self._emit(f"spread:subscribe:{slot_name}:{exchange}:{market_type}:{symbol}")

    def _clear_quotes(self, slot_name: str) -> None:
        self._pending_quotes.pop(slot_name, None)
        self._latest_public_quotes.pop(slot_name, None)
        labels = self._quote_labels.get(slot_name)
        if labels is None:
            return
        labels["bid_price_prefix"].setText(tr("spread.bid"))
        labels["ask_price_prefix"].setText(tr("spread.ask"))
        labels["bid_qty_prefix"].setText(tr("spread.qty"))
        labels["ask_qty_prefix"].setText(tr("spread.qty"))
        labels["bid_price"].setText("-")
        labels["ask_price"].setText("-")
        labels["bid_qty"].setText("- USDT")
        labels["ask_qty"].setText("- USDT")
        if not self._runtime_running:
            self._render_public_quote_spread_view()

    def _on_public_quote_received(self, slot_name: str, quote_data: object) -> None:
        if not isinstance(quote_data, dict):
            return
        self._pending_quotes[slot_name] = dict(quote_data)
        if not self._ui_quote_timer.isActive():
            self._ui_quote_timer.start()

    def _flush_pending_quotes(self) -> None:
        if not self._pending_quotes:
            self._ui_quote_timer.stop()
            return
        pending = self._pending_quotes
        self._pending_quotes = {}
        for slot_name, quote_data in pending.items():
            self._render_quote(slot_name, quote_data)

    def _render_quote(self, slot_name: str, quote_data: dict) -> None:
        labels = self._quote_labels.get(slot_name)
        if labels is None:
            return
        bid_price = quote_data.get("bid", "--")
        ask_price = quote_data.get("ask", "--")
        bid_qty = quote_data.get("bid_qty", "--")
        ask_qty = quote_data.get("ask_qty", "--")
        self._set_label_text_if_changed(labels["bid_price"], str(bid_price))
        self._set_label_text_if_changed(labels["ask_price"], str(ask_price))
        self._set_label_text_if_changed(labels["bid_qty"], f"{self._format_ui_notional_usdt(bid_price, bid_qty)} USDT")
        self._set_label_text_if_changed(labels["ask_qty"], f"{self._format_ui_notional_usdt(ask_price, ask_qty)} USDT")
        bid_decimal = self._decimal_or_none(bid_price)
        ask_decimal = self._decimal_or_none(ask_price)
        if bid_decimal is not None and ask_decimal is not None:
            self._latest_public_quotes[slot_name] = {
                "bid": bid_decimal,
                "ask": ask_decimal,
            }
        # Центр: без рантайма — всегда публичный спред; с рантаймом — только если
        # выбранная пара совпадает с активным воркером (иначе показываем новый выбор).
        if not self._runtime_running and self._pending_spread_state is None:
            self._render_public_quote_spread_view()
        elif self._runtime_running and not self._slots_match_active_runtime():
            self._render_public_quote_spread_view()

    def _build_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(4, 4, 4, 4)
        root.setSpacing(0)
        self.container = QFrame()
        self.container.setObjectName("spreadContainer")
        root.addWidget(self.container)

        layout = QVBoxLayout(self.container)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(8)

        selectors = QHBoxLayout()
        selectors.setSpacing(10)
        selectors.addWidget(self._build_leg_column("left"), 1)

        center = QFrame()
        center.setObjectName("spreadValueFrame")
        center.setProperty("edgeTone", "neutral")
        center.setMinimumWidth(250)
        center.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Expanding)
        center_outer = QVBoxLayout(center)
        center_outer.setContentsMargins(4, 1, 4, 1)
        inner = QFrame()
        inner.setObjectName("spreadValueInner")
        inner_layout = QVBoxLayout(inner)
        inner_layout.setContentsMargins(12, 6, 12, 6)
        select_btn = QPushButton()
        select_btn.setObjectName("spreadActionButton")
        select_btn.clicked.connect(self._subscribe_both)
        self._select_button = select_btn
        inner_layout.addWidget(select_btn)
        start_btn = QPushButton()
        start_btn.setObjectName("spreadActionButton")
        start_btn.clicked.connect(self._start_selected_spread_runtime)
        self._start_button = start_btn
        stop_btn = QPushButton()
        stop_btn.setObjectName("spreadActionButton")
        stop_btn.clicked.connect(self._stop_spread_runtime)
        self._stop_button = stop_btn
        inner_layout.addWidget(stop_btn)
        signal_mode_btn = QPushButton()
        signal_mode_btn.setObjectName("spreadModeButton")
        signal_mode_btn.setCheckable(True)
        signal_mode_btn.clicked.connect(self._toggle_strategy_signal_mode)
        self._signal_mode_button = signal_mode_btn
        inner_layout.addWidget(signal_mode_btn)
        simulate_entry_btn = QPushButton()
        simulate_entry_btn.setObjectName("spreadSimulationButton")
        simulate_entry_btn.setCheckable(True)
        simulate_entry_btn.clicked.connect(self._toggle_simulated_entry_window)
        self._simulate_entry_button = simulate_entry_btn
        inner_layout.addWidget(simulate_entry_btn)
        simulate_exit_btn = QPushButton()
        simulate_exit_btn.setObjectName("spreadSimulationButton")
        simulate_exit_btn.setCheckable(True)
        simulate_exit_btn.clicked.connect(self._toggle_simulated_exit_window)
        self._simulate_exit_button = simulate_exit_btn
        inner_layout.addWidget(simulate_exit_btn)
        value_label = QLabel("--")
        value_label.setObjectName("spreadValueLabel")
        value_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._spread_value_label = value_label
        inner_layout.addWidget(value_label)
        center_outer.addWidget(inner)

        selectors.addWidget(center, 0, Qt.AlignmentFlag.AlignVCenter)
        selectors.addWidget(self._build_leg_column("right"), 1)
        layout.addLayout(selectors)

        quotes = QHBoxLayout()
        quotes.setSpacing(16)
        quotes.addWidget(self._build_quote_panel("left"), 1)
        quotes.addWidget(self._build_quote_panel("right"), 1)
        layout.addLayout(quotes)

        strategy = QFrame()
        strategy.setObjectName("strategyPanel")
        strategy_layout = QVBoxLayout(strategy)
        strategy_layout.setContentsMargins(8, 6, 8, 6)
        strategy_layout.setSpacing(6)
        title = QLabel()
        title.setObjectName("strategyTitle")
        self._strategy_title_label = title
        strategy_layout.addWidget(title)
        grid = QGridLayout()
        grid.setHorizontalSpacing(6)
        fields = (
            ("spread.entry_threshold", "0.20"),
            ("spread.exit_threshold", "0.08"),
            ("spread.target_size", "10"),
            ("spread.entry_min_step_pct", "20"),
        )
        for idx, (label_key, value) in enumerate(fields):
            cell = QFrame()
            cell.setObjectName("strategyFieldCapsule")
            cell_layout = QHBoxLayout(cell)
            cell_layout.setContentsMargins(10, 4, 10, 4)
            label = QLabel()
            label.setProperty("i18n_key", label_key)
            label.setObjectName("strategyFieldInlineLabel")
            self._strategy_field_labels.append(label)
            divider = QFrame()
            divider.setObjectName("strategyFieldDivider")
            divider.setFixedWidth(1)
            edit = QLineEdit(value)
            edit.setObjectName("strategyFieldInput")
            edit.setMaximumWidth(72)
            edit.setMinimumWidth(56)
            edit.setFixedHeight(24)
            edit.setAlignment(Qt.AlignmentFlag.AlignCenter)
            edit.editingFinished.connect(self._save_state)
            self._strategy_inputs[label_key] = edit
            cell_layout.addWidget(label, 1)
            cell_layout.addWidget(divider, 0)
            cell_layout.addWidget(edit, 0)
            grid.addWidget(cell, 0, idx)
        start_btn.setFixedHeight(32)
        start_btn.setMinimumWidth(90)
        grid.addWidget(start_btn, 0, len(fields))
        strategy_layout.addLayout(grid)
        layout.addWidget(strategy)

        runtimes_scroll = QScrollArea()
        runtimes_scroll.setWidgetResizable(True)
        runtimes_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        runtimes_scroll.setObjectName("runtimesScroll")
        runtimes_container = QWidget()
        self._runtimes_layout = QVBoxLayout(runtimes_container)
        self._runtimes_layout.setContentsMargins(0, 0, 0, 0)
        self._runtimes_layout.setSpacing(8)
        self._runtimes_layout.setAlignment(Qt.AlignmentFlag.AlignTop)
        self._runtimes_layout.addStretch(1)
        runtimes_scroll.setWidget(runtimes_container)
        layout.addWidget(runtimes_scroll, 1)

    def _subscribe_both(self) -> None:
        self._subscribe_slot("left")
        self._subscribe_slot("right")
        self._emit("spread:select")

    def _start_selected_spread_runtime(self) -> None:
        self._subscribe_slot("left")
        self._subscribe_slot("right")
        self._start_spread_runtime()
        self._emit("spread:start_runtime")

    @staticmethod
    def _make_worker_id(left_exchange: str, left_symbol: str, right_exchange: str, right_symbol: str) -> str:
        return f"spread_{left_exchange}_{left_symbol}__{right_exchange}_{right_symbol}".lower()

    @staticmethod
    def _spread_threshold_percent_to_ratio(text: str) -> str:
        """Проценты из полей стратегии → доля для сравнения с calculate_spread_edges."""
        raw = str(text or "").strip().replace(",", ".")
        if not raw:
            return "0"
        try:
            pct = Decimal(raw)
        except Exception:
            return raw
        if pct == 0:
            return "0"
        # Отрицательный % → отрицательная доля (граница порога выхода на оси со знаком).
        ratio = (pct / Decimal("100")).normalize()
        # Убираем лишние нули, но не экспоненту
        return format(ratio, "f").rstrip("0").rstrip(".") or "0"

    def _current_slots_worker_id(self) -> str | None:
        """worker_id для текущего выбора в слотах; None если пара не собрана."""
        left = self._slot_state["left"]
        right = self._slot_state["right"]
        if not all((left.get("exchange"), left.get("market_type"), left.get("symbol"), right.get("exchange"), right.get("market_type"), right.get("symbol"))):
            return None
        return self._make_worker_id(
            str(left["exchange"]), str(left["symbol"]),
            str(right["exchange"]), str(right["symbol"]),
        )

    def _slots_match_active_runtime(self) -> bool:
        """True если центр должен показывать метрики активного рантайма (та же пара)."""
        if not self._runtime_running or not self._active_worker_id:
            return False
        slots_id = self._current_slots_worker_id()
        return slots_id is not None and slots_id == self._active_worker_id

    def _sync_spread_view_with_slots(self) -> None:
        """
        После смены биржи/пары: если рантайм на другой паре — центр показывает спред
        по текущим слотам (публичные котировки), а не залипший спред старого воркера.
        """
        if self._slots_match_active_runtime():
            return
        self._pending_spread_state = None
        if self._ui_spread_timer.isActive():
            self._ui_spread_timer.stop()
        if self._latest_public_quotes.get("left") and self._latest_public_quotes.get("right"):
            self._render_public_quote_spread_view()
        else:
            self._set_spread_label_idle()

    def _start_spread_runtime(self) -> None:
        if self.coordinator is None:
            return
        left = self._slot_state["left"]
        right = self._slot_state["right"]
        if not all((left["exchange"], left["market_type"], left["symbol"], right["exchange"], right["market_type"], right["symbol"])):
            self._set_spread_label_idle()
            self._update_runtime_controls()
            return

        worker_id = self._make_worker_id(
            str(left["exchange"]), str(left["symbol"]),
            str(right["exchange"]), str(right["symbol"]),
        )
        if worker_id in self._runtime_cards:
            return

        left_credentials = self._find_connected_exchange_credentials(str(left["exchange"]))
        right_credentials = self._find_connected_exchange_credentials(str(right["exchange"]))
        strategy_params = self._current_strategy_params()

        # Поля «Вход %» / «Выход %» вводятся в процентах (как на центральном лейбле спреда).
        # В рантайме сравнение идёт с abs(edge), где edge — доля (0.002 = 0.20%).
        # Без деления на 100 порог 0.20 никогда не достигается при типичных спредах.
        entry_threshold_ui = str(strategy_params.get("spread.entry_threshold") or "0")
        exit_threshold_ui = str(strategy_params.get("spread.exit_threshold") or "0")
        card_params = {
            "entry_threshold": self._spread_threshold_percent_to_ratio(entry_threshold_ui),
            "exit_threshold": self._spread_threshold_percent_to_ratio(exit_threshold_ui),
            "entry_notional_usdt": str(strategy_params.get("spread.target_size") or "0"),
            "entry_min_step_pct": str(strategy_params.get("spread.entry_min_step_pct") or "20"),
            "strategy_signal_mode": self._strategy_signal_mode,
        }

        if (
            left_credentials is not None
            and right_credentials is not None
            and str(left["market_type"]) == "perpetual"
            and str(right["market_type"]) == "perpetual"
            and self._has_active_execution_route(str(left["exchange"]))
            and self._has_active_execution_route(str(right["exchange"]))
        ):
            self.coordinator.start_spread_entry_runtime_async(
                worker_id=worker_id,
                left_exchange=str(left["exchange"]),
                left_market_type=str(left["market_type"]),
                left_symbol=str(left["symbol"]),
                left_api_key=str(left_credentials["api_key"]),
                left_api_secret=str(left_credentials["api_secret"]),
                left_api_passphrase=str(left_credentials["api_passphrase"]),
                left_account_profile=dict(left_credentials.get("account_profile") or {}),
                right_exchange=str(right["exchange"]),
                right_market_type=str(right["market_type"]),
                right_symbol=str(right["symbol"]),
                right_api_key=str(right_credentials["api_key"]),
                right_api_secret=str(right_credentials["api_secret"]),
                right_api_passphrase=str(right_credentials["api_passphrase"]),
                right_account_profile=dict(right_credentials.get("account_profile") or {}),
                entry_threshold=card_params["entry_threshold"],
                exit_threshold=card_params["exit_threshold"],
                max_quote_age_ms="2500",
                max_quote_skew_ms="0",
                entry_notional_usdt=card_params["entry_notional_usdt"],
                entry_min_step_pct=card_params["entry_min_step_pct"],
                strategy_signal_mode=card_params["strategy_signal_mode"],
            )
        else:
            self.coordinator.start_dual_quotes_runtime_async(
                worker_id=worker_id,
                left_exchange=str(left["exchange"]),
                left_market_type=str(left["market_type"]),
                left_symbol=str(left["symbol"]),
                right_exchange=str(right["exchange"]),
                right_market_type=str(right["market_type"]),
                right_symbol=str(right["symbol"]),
            )

        self._active_worker_id = worker_id
        # На карточке показываем те же числа, что в полях (%); в рантайм ушли доли.
        card_display_params = {
            **card_params,
            "entry_threshold": entry_threshold_ui,
            "exit_threshold": exit_threshold_ui,
        }
        self._add_runtime_card(
            worker_id,
            left_exchange=str(left["exchange"]),
            left_symbol=str(left["symbol"]),
            right_exchange=str(right["exchange"]),
            right_symbol=str(right["symbol"]),
            params=card_display_params,
        )

    def _add_runtime_card(
        self,
        worker_id: str,
        *,
        left_exchange: str,
        left_symbol: str,
        right_exchange: str,
        right_symbol: str,
        params: dict[str, str],
    ) -> None:
        card = RuntimeCard(
            worker_id,
            left_exchange=left_exchange,
            left_symbol=left_symbol,
            right_exchange=right_exchange,
            right_symbol=right_symbol,
            params=params,
        )
        card.stop_clicked.connect(self._stop_runtime_by_id)
        self._runtime_cards[worker_id] = card
        if self._runtimes_layout is not None:
            self._runtimes_layout.insertWidget(0, card)

    def _stop_spread_runtime(self) -> None:
        if self._active_worker_id is not None:
            self._stop_runtime_by_id(self._active_worker_id)

    def _stop_runtime_by_id(self, worker_id: str) -> None:
        if self.coordinator is not None:
            self.coordinator.stop_test_runtime(worker_id)
        card = self._runtime_cards.pop(worker_id, None)
        if card is not None:
            card.setParent(None)
            card.deleteLater()
        if self._active_worker_id == worker_id:
            remaining = list(self._runtime_cards.keys())
            self._active_worker_id = remaining[-1] if remaining else None
        if not self._runtime_cards:
            self._runtime_running = False
            self._simulated_entry_window_open = False
            self._simulated_exit_window_open = False
            self._pending_spread_state = None
            if self._ui_spread_timer.isActive():
                self._ui_spread_timer.stop()
            self._set_spread_label_idle()
        self._update_runtime_controls()
        self._emit("spread:stop_runtime")

    def _toggle_simulated_entry_window(self) -> None:
        if self.coordinator is None or self._active_worker_id is None:
            return
        enabled = bool(self._simulate_entry_button is not None and self._simulate_entry_button.isChecked())
        self.coordinator.set_simulated_entry_window_async(self._active_worker_id, enabled)
        self._emit(f"spread:simulate_entry:{'on' if enabled else 'off'}")

    def _toggle_strategy_signal_mode(self) -> None:
        enabled = bool(self._signal_mode_button is not None and self._signal_mode_button.isChecked())
        self._strategy_signal_mode = "simulated" if enabled else "market"
        if self._strategy_signal_mode != "simulated":
            self._simulated_entry_window_open = False
            self._simulated_exit_window_open = False
        self._save_state()
        if self.coordinator is not None and self._runtime_running and self._active_worker_id is not None:
            self.coordinator.set_strategy_signal_mode_async(self._active_worker_id, self._strategy_signal_mode)
        self._update_runtime_controls()
        self._emit(f"spread:signal_mode:{self._strategy_signal_mode}")

    def _toggle_simulated_exit_window(self) -> None:
        if self.coordinator is None or self._active_worker_id is None:
            return
        enabled = bool(self._simulate_exit_button is not None and self._simulate_exit_button.isChecked())
        self.coordinator.set_simulated_exit_window_async(self._active_worker_id, enabled)
        self._emit(f"spread:simulate_exit:{'on' if enabled else 'off'}")

    def _on_worker_state_updated(self, worker_id: str, state: object) -> None:
        if not isinstance(state, dict):
            return
        if worker_id not in self._runtime_cards:
            return
        is_running = str(state.get("status") or "").strip().lower() == "running"
        metrics = state.get("metrics") if isinstance(state.get("metrics"), dict) else {}
        card = self._runtime_cards.get(worker_id)
        if card is not None:
            activity_status = str(metrics.get("activity_status") or ("WAITING_ENTRY" if is_running else "STOPPED"))
            stream_health = str(metrics.get("execution_stream_health_status") or "UNKNOWN")
            card.update_status(activity_status, stream_health)
            card.update_volume(str(metrics.get("current_position_notional_usdt") or "0"))
        if worker_id != self._active_worker_id:
            return
        self._runtime_running = is_running
        self._strategy_signal_mode = "simulated" if str(metrics.get("strategy_signal_mode") or "").strip().lower() == "simulated" else "market"
        self._simulated_entry_window_open = bool(metrics.get("simulated_entry_window_open"))
        self._simulated_exit_window_open = bool(metrics.get("simulated_exit_window_open"))
        self._update_runtime_controls()
        # Пара в слотах сменилась — центр не привязываем к рантайму; карточка обновляется выше
        if not self._slots_match_active_runtime():
            self._pending_spread_state = None
            if self._ui_spread_timer.isActive():
                self._ui_spread_timer.stop()
            self._sync_spread_view_with_slots()
            return
        self._pending_spread_state = state
        if not self._ui_spread_timer.isActive():
            self._ui_spread_timer.start()

    def _on_worker_command_failed(self, worker_id: str, message: str) -> None:
        if worker_id not in self._runtime_cards:
            return
        if worker_id == self._active_worker_id:
            self._runtime_running = False
            self._simulated_entry_window_open = False
            self._simulated_exit_window_open = False
            self._update_runtime_controls()
        self._emit(f"spread:error:{message}")

    def _flush_pending_spread_state(self) -> None:
        if self._pending_spread_state is None:
            self._ui_spread_timer.stop()
            return
        # Пока пользователь смотрит другую пару — не рисуем состояние активного воркера в центре
        if self._runtime_running and self._active_worker_id and not self._slots_match_active_runtime():
            self._pending_spread_state = None
            self._ui_spread_timer.stop()
            self._sync_spread_view_with_slots()
            return
        state = self._pending_spread_state
        self._pending_spread_state = None
        self._render_spread_state(state)

    def _render_spread_state(self, state: dict) -> None:
        if self._spread_value_label is None:
            return
        view = self._build_spread_view(state)
        entry_values = self._enrich_entry_status_values(state, view.entry_values)
        if view.edge_tone == "right_cheap":
            self._set_exchange_tones(right_slot_cheap=True)
        elif view.edge_tone == "left_cheap":
            self._set_exchange_tones(right_slot_cheap=False)
        else:
            self._set_exchange_tones(None)
        self._spread_value_label.setText(view.spread_value_text)
        self._apply_entry_values(entry_values)
        metrics = state.get("metrics") if isinstance(state.get("metrics"), dict) else {}
        self._apply_execution_stream_health_tone(str(metrics.get("execution_stream_health_status") or "UNKNOWN"))

    def _set_spread_label_idle(self) -> None:
        self._set_exchange_tones(None)
        if self._spread_value_label is not None:
            self._spread_value_label.setText("--")
        self._apply_entry_values(
            {
                "spread.status.phase_value": "--",
                "spread.status.block_value": "--",
                "spread.status.cycle_value": "--",
                "spread.status.recovery_hedge_value": "--",
                "spread.status.global_hedge_value": "--",
                "spread.status.exec_connected_value": "--",
                "spread.status.exec_auth_value": "--",
                "spread.status.exec_reconnects_value": "--",
                "spread.status.exec_last_error_value": "--",
                "spread.status.left_order_value": "--",
                "spread.status.left_position_value": "--",
                "spread.status.left_notional_value": "--",
                "spread.status.left_timing_value": "--",
                "spread.status.right_order_value": "--",
                "spread.status.right_position_value": "--",
                "spread.status.right_notional_value": "--",
                "spread.status.right_timing_value": "--",
            }
        )
        self._apply_execution_stream_health_tone("UNKNOWN")

    def _render_public_quote_spread_view(self) -> None:
        if self._spread_value_label is None:
            return
        left_quote = self._latest_public_quotes.get("left")
        right_quote = self._latest_public_quotes.get("right")
        if left_quote is None or right_quote is None:
            self._set_spread_label_idle()
            return
        left_bid = left_quote.get("bid")
        left_ask = left_quote.get("ask")
        right_bid = right_quote.get("bid")
        right_ask = right_quote.get("ask")
        edge_1 = self._safe_edge(left_bid, right_ask)
        edge_2 = self._safe_edge(right_bid, left_ask)
        best_edge = None
        direction = "--"
        edge_tone: str | None = None
        if edge_1 is not None and (edge_2 is None or edge_1 >= edge_2):
            best_edge = edge_1
            direction = "LEFT_SELL_RIGHT_BUY"
            edge_tone = "right_cheap"
        elif edge_2 is not None:
            best_edge = edge_2
            direction = "LEFT_BUY_RIGHT_SELL"
            edge_tone = "left_cheap"
        self._spread_value_label.setText(self._format_spread_percent(best_edge))
        if edge_tone == "right_cheap":
            self._set_exchange_tones(right_slot_cheap=True)
        elif edge_tone == "left_cheap":
            self._set_exchange_tones(right_slot_cheap=False)
        else:
            self._set_exchange_tones(None)
        self._apply_entry_values(
            {
                "spread.status.phase_value": tr("spread.activity.waiting_entry"),
                "spread.status.block_value": "Нет",
                "spread.status.cycle_value": "--",
                "spread.status.recovery_hedge_value": "--",
                "spread.status.global_hedge_value": "--",
                "spread.status.exec_connected_value": "--",
                "spread.status.exec_auth_value": "--",
                "spread.status.exec_reconnects_value": "--",
                "spread.status.exec_last_error_value": "--",
                "spread.status.left_order_value": "--",
                "spread.status.left_position_value": "--",
                "spread.status.left_notional_value": "--",
                "spread.status.left_timing_value": "--",
                "spread.status.right_order_value": "--",
                "spread.status.right_position_value": "--",
                "spread.status.right_notional_value": "--",
                "spread.status.right_timing_value": "--",
            }
        )

    def _update_runtime_controls(self) -> None:
        ready = all(
            (
                self._slot_state["left"]["exchange"],
                self._slot_state["left"]["market_type"],
                self._slot_state["left"]["symbol"],
                self._slot_state["right"]["exchange"],
                self._slot_state["right"]["market_type"],
                self._slot_state["right"]["symbol"],
            )
        )
        current_pair_id = self._make_worker_id(
            str(self._slot_state["left"]["exchange"] or ""),
            str(self._slot_state["left"]["symbol"] or ""),
            str(self._slot_state["right"]["exchange"] or ""),
            str(self._slot_state["right"]["symbol"] or ""),
        ) if ready else ""
        pair_already_running = current_pair_id in self._runtime_cards
        sim_enabled = self._runtime_running and self._strategy_signal_mode == "simulated"
        is_simulated = self._strategy_signal_mode == "simulated"
        stop_enabled = self._active_worker_id is not None and self._runtime_running
        start_enabled = bool(ready) and not pair_already_running

        new_state: dict[str, object] = {
            "start": start_enabled,
            "stop": stop_enabled,
            "sim_entry_chk": self._simulated_entry_window_open,
            "sim_entry_en": sim_enabled,
            "sim_exit_chk": self._simulated_exit_window_open,
            "sim_exit_en": sim_enabled,
            "mode_chk": is_simulated,
            "mode": self._strategy_signal_mode,
        }
        if new_state == self._prev_controls_state:
            return
        self._prev_controls_state = new_state

        self._update_signal_mode_button_text()
        if self._select_button is not None:
            self._select_button.setEnabled(True)
        if self._start_button is not None:
            self._start_button.setEnabled(start_enabled)
        if self._stop_button is not None:
            self._stop_button.setEnabled(stop_enabled)
        if self._simulate_entry_button is not None:
            self._simulate_entry_button.blockSignals(True)
            self._simulate_entry_button.setChecked(self._simulated_entry_window_open)
            self._simulate_entry_button.blockSignals(False)
            self._simulate_entry_button.setEnabled(sim_enabled)
        if self._simulate_exit_button is not None:
            self._simulate_exit_button.blockSignals(True)
            self._simulate_exit_button.setChecked(self._simulated_exit_window_open)
            self._simulate_exit_button.blockSignals(False)
            self._simulate_exit_button.setEnabled(sim_enabled)
        if self._signal_mode_button is not None:
            self._signal_mode_button.blockSignals(True)
            self._signal_mode_button.setChecked(is_simulated)
            self._signal_mode_button.blockSignals(False)
            self._signal_mode_button.setEnabled(True)

    def _current_strategy_params(self) -> dict[str, str]:
        self._normalize_strategy_inputs()
        return {
            key: field.text().strip()
            for key, field in self._strategy_inputs.items()
        }

    def _normalize_strategy_inputs(self) -> None:
        target_field = self._strategy_inputs.get("spread.target_size")
        if target_field is not None:
            target_field.setText(self._normalize_target_size_text(target_field.text()))
        min_step_field = self._strategy_inputs.get("spread.entry_min_step_pct")
        if min_step_field is not None:
            min_step_field.setText(self._normalize_min_step_percent_text(min_step_field.text()))

    @staticmethod
    def _normalize_target_size_text(value: str) -> str:
        text = str(value or "").strip().replace(" ", "").replace(",", ".")
        try:
            normalized = Decimal(text)
        except Exception:
            normalized = Decimal("10")
        if normalized < Decimal("10"):
            normalized = Decimal("10")
        if normalized == normalized.to_integral_value():
            return str(normalized.quantize(Decimal("1")))
        return format(normalized.normalize(), "f").rstrip("0").rstrip(".")

    @staticmethod
    def _normalize_min_step_percent_text(value: str) -> str:
        text = str(value or "").strip().replace(" ", "").replace(",", ".")
        try:
            normalized = Decimal(text)
        except Exception:
            normalized = Decimal("20")
        if normalized < Decimal("10"):
            normalized = Decimal("10")
        if normalized > Decimal("100"):
            normalized = Decimal("100")
        if normalized == normalized.to_integral_value():
            return str(normalized.quantize(Decimal("1")))
        return format(normalized.normalize(), "f").rstrip("0").rstrip(".")

    @staticmethod
    def _find_connected_exchange_credentials(exchange_code: str) -> dict[str, object] | None:
        normalized = str(exchange_code or "").strip().lower()
        for card in load_exchange_cards():
            if str(card.get("exchange_code", "")).strip().lower() != normalized:
                continue
            if not bool(card.get("connected")):
                continue
            resolved = resolve_exchange_card_credentials(card) or {}
            api_key = str(resolved.get("api_key", "")).strip()
            api_secret = str(resolved.get("api_secret", "")).strip()
            if not api_key or not api_secret:
                continue
            return {
                "api_key": api_key,
                "api_secret": api_secret,
                "api_passphrase": str(resolved.get("api_passphrase", "")).strip(),
                "account_profile": dict(card.get("account_snapshot", {}).get("account_profile", {}) or {}),
            }
        return None

    def _has_active_execution_route(self, exchange_code: str) -> bool:
        state = self._transport_state_for_exchange(exchange_code)
        return bool(state.get("ws", {}).get("active")) or bool(state.get("rest", {}).get("active"))

    def _apply_entry_values(self, entry_values: dict[str, str]) -> None:
        if self._diagnostics_window is not None:
            self._diagnostics_window.apply_entry_values(entry_values)

    def _apply_execution_stream_health_tone(self, status: str) -> None:
        if self._diagnostics_window is not None:
            self._diagnostics_window.apply_execution_stream_health_tone(status)

    def _open_diagnostics_window(self) -> None:
        if self._diagnostics_window is None:
            self._diagnostics_window = DiagnosticsWindow(self)
            self._entry_status_labels = self._diagnostics_window.status_labels
        self._diagnostics_window.show()
        self._diagnostics_window.raise_()
        self._diagnostics_window.activateWindow()

    @staticmethod
    def _set_label_text_if_changed(label: QLabel, value: str) -> None:
        if label.text() == value:
            return
        label.setText(value)

    def _set_exchange_tones(self, right_slot_cheap: bool | None) -> None:
        left_button = self._exchange_buttons.get("left")
        right_button = self._exchange_buttons.get("right")
        if right_slot_cheap is None:
            self._apply_button_tone(left_button, "neutral")
            self._apply_button_tone(right_button, "neutral")
            self._apply_spread_edge_tone("neutral")
            return
        if right_slot_cheap:
            self._apply_button_tone(left_button, "expensive")
            self._apply_button_tone(right_button, "cheap")
            self._apply_spread_edge_tone("right_cheap")
        else:
            self._apply_button_tone(left_button, "cheap")
            self._apply_button_tone(right_button, "expensive")
            self._apply_spread_edge_tone("left_cheap")

    @staticmethod
    def _apply_button_tone(button: QPushButton | None, tone_role: str) -> None:
        if button is None:
            return
        if str(button.property("toneRole") or "neutral") == tone_role:
            return
        button.setProperty("toneRole", tone_role)
        button.style().unpolish(button)
        button.style().polish(button)
        button.update()

    def _apply_spread_edge_tone(self, tone: str) -> None:
        frame = self.container.findChild(QFrame, "spreadValueFrame")
        if frame is None:
            return
        if str(frame.property("edgeTone") or "neutral") == tone:
            return
        frame.setProperty("edgeTone", tone)
        frame.style().unpolish(frame)
        frame.style().polish(frame)
        frame.update()

    def _build_spread_view(self, state: dict[str, Any] | None) -> SpreadView:
        """
        Delegate spread view construction to the application-layer service.

        If for any reason the service is unavailable, fall back to a local
        implementation that mirrors the previous behaviour.
        """
        service_spread_value_text = "--"
        service_edge_tone: str | None = None
        if self._spread_service is not None:
            try:
                service_view = self._spread_service.build_view_from_worker_state(state or {})
                service_spread_value_text = service_view.spread_value_text
                service_edge_tone = service_view.edge_tone
            except Exception:
                service_spread_value_text = "--"
                service_edge_tone = None

        # Fallback: preserve previous in-widget logic.
        if not isinstance(state, dict):
            metrics: dict[str, Any] = {}
        else:
            raw_metrics = state.get("metrics")
            metrics = raw_metrics if isinstance(raw_metrics, dict) else {}

        spread_state = str(metrics.get("spread_state") or "WAITING_QUOTES")
        edge_1 = self._decimal_or_none(metrics.get("edge_1"))
        edge_2 = self._decimal_or_none(metrics.get("edge_2"))

        spread_value_text = service_spread_value_text
        edge_tone: str | None = service_edge_tone

        if spread_value_text == "--" and spread_state == "LIVE":
            active_edge = max((value for value in (edge_1, edge_2) if value is not None), default=None)
            if edge_1 is not None and (edge_2 is None or edge_1 >= edge_2):
                edge_tone = "right_cheap"
            elif edge_2 is not None:
                edge_tone = "left_cheap"
            if active_edge is not None:
                spread_value_text = self._format_spread_percent(active_edge)

        block_reason = str(metrics.get("entry_block_reason") or "").strip()
        entry_cycle_state = str(metrics.get("active_entry_cycle_state") or "").strip()
        exit_cycle_state = str(metrics.get("active_exit_cycle_state") or "").strip()
        cycle_status = " | ".join(
            value for value in (
                f"Вход: {entry_cycle_state}" if entry_cycle_state else "",
                f"Выход: {exit_cycle_state}" if exit_cycle_state else "",
            ) if value
        ) or "--"
        recovery_context = str(metrics.get("recovery_context") or "").strip()
        recovery_state = str(metrics.get("recovery_state") or "").strip()
        recovery_status = "--"
        if recovery_context or recovery_state:
            recovery_status = " / ".join(value for value in (recovery_context, recovery_state) if value)
        global_hedge_status = str(metrics.get("hedge_status") or "--")

        entry_values = {
            "spread.status.phase_value": self._format_activity_status(str(metrics.get("activity_status") or "--")),
            "spread.status.block_value": self._format_block_reason_ru(block_reason),
            "spread.status.cycle_value": cycle_status,
            "spread.status.recovery_hedge_value": recovery_status,
            "spread.status.global_hedge_value": global_hedge_status,
            "spread.status.exec_connected_value": "--",
            "spread.status.exec_auth_value": "--",
            "spread.status.exec_reconnects_value": "--",
            "spread.status.exec_last_error_value": "--",
            "spread.status.left_order_value": str(metrics.get("left_order_status") or "--"),
            "spread.status.left_position_value": "--",
            "spread.status.left_notional_value": "--",
            "spread.status.left_timing_value": "--",
            "spread.status.right_order_value": str(metrics.get("right_order_status") or "--"),
            "spread.status.right_position_value": "--",
            "spread.status.right_notional_value": "--",
            "spread.status.right_timing_value": "--",
        }
        return SpreadView(
            spread_value_text=spread_value_text,
            edge_tone=edge_tone,
            entry_values=entry_values,
        )

    def _enrich_entry_status_values(self, state: dict[str, Any] | None, entry_values: dict[str, str]) -> dict[str, str]:
        enriched = dict(entry_values)
        raw_metrics = state.get("metrics") if isinstance(state, dict) else None
        metrics = raw_metrics if isinstance(raw_metrics, dict) else {}

        left_qty = self._fact_leg_qty(metrics, "left")
        right_qty = self._fact_leg_qty(metrics, "right")
        display_left_qty = left_qty
        display_right_qty = right_qty
        if left_qty is not None and right_qty is not None:
            # Keep UI position fields synchronized by showing the exchange-confirmed
            # hedged portion common to both legs.
            hedged_common_qty = min(abs(left_qty), abs(right_qty))
            display_left_qty = hedged_common_qty
            display_right_qty = hedged_common_qty
        left_ref_price = self._resolve_leg_reference_price("left", metrics)
        right_ref_price = self._resolve_leg_reference_price("right", metrics)

        left_notional = f"{self._format_ui_notional_usdt(left_ref_price, abs(display_left_qty))} USDT" if display_left_qty is not None else "--"
        right_notional = f"{self._format_ui_notional_usdt(right_ref_price, abs(display_right_qty))} USDT" if display_right_qty is not None else "--"

        left_ack = self._format_leg_latency(metrics.get("left_ack_latency_ms"))
        left_fill = self._format_leg_latency(metrics.get("left_fill_latency_ms"))
        right_ack = self._format_leg_latency(metrics.get("right_ack_latency_ms"))
        right_fill = self._format_leg_latency(metrics.get("right_fill_latency_ms"))
        execution_stream_health = metrics.get("execution_stream_health")
        stream_snapshot = execution_stream_health if isinstance(execution_stream_health, dict) else {}

        enriched["spread.status.left_position_value"] = self._format_ui_qty(abs(display_left_qty)) if display_left_qty is not None else "--"
        enriched["spread.status.left_notional_value"] = left_notional
        enriched["spread.status.left_timing_value"] = f"Ack {left_ack} | Fill {left_fill}"
        enriched["spread.status.right_position_value"] = self._format_ui_qty(abs(display_right_qty)) if display_right_qty is not None else "--"
        enriched["spread.status.right_notional_value"] = right_notional
        enriched["spread.status.right_timing_value"] = f"Ack {right_ack} | Fill {right_fill}"
        enriched["spread.status.exec_connected_value"] = self._format_stream_health_connected(stream_snapshot)
        enriched["spread.status.exec_auth_value"] = self._format_stream_health_authenticated(stream_snapshot)
        enriched["spread.status.exec_reconnects_value"] = self._format_stream_health_reconnects(stream_snapshot)
        enriched["spread.status.exec_last_error_value"] = self._format_stream_health_last_error(stream_snapshot)
        return enriched

    def _fact_leg_qty(self, metrics: dict[str, Any], leg_name: str) -> Decimal | None:
        # Show only exchange-confirmed position size to avoid confusing
        # transient jumps from local estimated remainder fields.
        actual = self._decimal_or_none(metrics.get(f"{leg_name}_actual_position_qty"))
        return actual if actual is not None else None

    def _resolve_leg_reference_price(self, leg_name: str, metrics: dict[str, Any]) -> Decimal | None:
        quote = self._latest_public_quotes.get(leg_name)
        bid = self._decimal_or_none((quote or {}).get("bid"))
        ask = self._decimal_or_none((quote or {}).get("ask"))
        if bid is None:
            bid = self._decimal_or_none(metrics.get(f"{leg_name}_bid"))
        if ask is None:
            ask = self._decimal_or_none(metrics.get(f"{leg_name}_ask"))
        if bid is not None and ask is not None and bid > Decimal("0") and ask > Decimal("0"):
            return (bid + ask) / Decimal("2")
        if ask is not None and ask > Decimal("0"):
            return ask
        if bid is not None and bid > Decimal("0"):
            return bid
        return None

    @staticmethod
    def _format_leg_latency(value: object) -> str:
        try:
            latency = int(value)
        except (TypeError, ValueError):
            return "--"
        if latency < 0:
            return "--"
        return f"{latency}ms"

    @staticmethod
    def _decimal_or_none(value: object) -> Decimal | None:
        if value in (None, "", "-"):
            return None
        try:
            return Decimal(str(value).strip())
        except Exception:
            return None

    @staticmethod
    def _safe_edge(numerator_left: Decimal | None, denominator_right: Decimal | None) -> Decimal | None:
        if numerator_left is None or denominator_right is None or denominator_right <= Decimal("0"):
            return None
        return (numerator_left - denominator_right) / denominator_right

    @staticmethod
    def _format_spread_percent(value: Decimal | None) -> str:
        if value is None:
            return "--"
        return f"{(value * Decimal('100')):.2f}%"

    @staticmethod
    def _format_stream_health_connected(snapshot: dict[str, Any]) -> str:
        streams = snapshot.get("streams") if isinstance(snapshot.get("streams"), dict) else {}
        if not streams:
            return "--"
        parts = [f"{leg}:{'Y' if bool(item.get('connected')) else 'N'}" for leg, item in streams.items() if isinstance(item, dict)]
        return " ".join(parts) if parts else "--"

    @staticmethod
    def _format_stream_health_authenticated(snapshot: dict[str, Any]) -> str:
        streams = snapshot.get("streams") if isinstance(snapshot.get("streams"), dict) else {}
        if not streams:
            return "--"
        parts: list[str] = []
        for leg, item in streams.items():
            if not isinstance(item, dict):
                continue
            value = item.get("authenticated")
            if value is None:
                parts.append(f"{leg}:-")
            else:
                parts.append(f"{leg}:{'Y' if bool(value) else 'N'}")
        return " ".join(parts) if parts else "--"

    @staticmethod
    def _format_stream_health_reconnects(snapshot: dict[str, Any]) -> str:
        streams = snapshot.get("streams") if isinstance(snapshot.get("streams"), dict) else {}
        if not streams:
            return "--"
        parts = [f"{leg}:{int(item.get('reconnect_attempts_total') or 0)}" for leg, item in streams.items() if isinstance(item, dict)]
        return " ".join(parts) if parts else "--"

    @staticmethod
    def _format_stream_health_last_error(snapshot: dict[str, Any]) -> str:
        streams = snapshot.get("streams") if isinstance(snapshot.get("streams"), dict) else {}
        if not streams:
            return "--"
        errors: list[str] = []
        for leg, item in streams.items():
            if not isinstance(item, dict):
                continue
            message = str(item.get("last_error") or "").strip()
            if message:
                errors.append(f"{leg}:{message}")
        return " | ".join(errors) if errors else "--"

    @staticmethod
    def _format_activity_status(value: str) -> str:
        mapping = {
            "STOPPED": tr("spread.activity.stopped"),
            "WAITING_ENTRY": tr("spread.activity.waiting_entry"),
            "ENTERING": tr("spread.activity.entering"),
            "WAITING_EXIT": tr("spread.activity.waiting_exit"),
            "EXITING": tr("spread.activity.exiting"),
            "REBALANCING": tr("spread.activity.rebalancing"),
            "RESTORE_HEDGE": tr("spread.activity.restore_hedge"),
            "EMERGENCY_CLOSE": tr("spread.activity.emergency_close"),
            "RECOVERY": tr("spread.activity.recovery"),
            "FAILED": tr("spread.activity.failed"),
        }
        return mapping.get(str(value or "").strip().upper(), str(value or "--"))

    @staticmethod
    def _format_block_reason_ru(reason: str | None) -> str:
        normalized = str(reason or "").strip().upper()
        if not normalized:
            return "Нет"
        mapping = {
            "SIMULATED_ENTRY_WINDOW_CLOSED": "Нет",
            "SIMULATED_EXIT_WINDOW_CLOSED": "Нет",
            "POSITION_DIRECTION_MISMATCH": "Направление сигнала не совпадает с текущей позицией",
            "EXIT_PRIORITY_ACTIVE": "Выход в приоритете, вход временно заблокирован",
            "ENTRY_COOLDOWN": "Пауза после входа (cooldown)",
            "ENTRY_CYCLE_ACTIVE": "Активен цикл исполнения",
            "ENTRY_RECOVERY_ACTIVE": "Активно восстановление входа",
            "EXIT_RECOVERY_ACTIVE": "Активно восстановление выхода",
            "HEDGE_PROTECTION_ACTIVE": "Активна защита хеджа",
            "ENTRY_WAIT_ORDER_SETTLE": "Ожидание подтверждения ордеров",
            "RUNTIME_RECONCILING": "Идёт синхронизация состояния",
            "WAITING_QUOTES": "Ожидание котировок",
            "BELOW_ENTRY_THRESHOLD": "Эдж ниже порога входа",
            "INVALID_ENTRY_THRESHOLD": "Некорректный порог входа",
            "POSITION_CAP_REACHED": "Достигнут лимит позиции",
            "MARGIN_LIMIT_REACHED": "Достигнут лимит по марже",
            "STALE_QUOTES": "Котировки устарели",
            "QUOTE_SKEW_TOO_HIGH": "Слишком большой рассинхрон котировок",
            "INSUFFICIENT_LIQUIDITY": "Недостаточно ликвидности в стакане",
        }
        return mapping.get(normalized, normalized)

    def retranslate_ui(self) -> None:
        for slot_name, button in self._exchange_buttons.items():
            if not self._slot_state[slot_name]["exchange"]:
                button.setText(tr("spread.choose_exchange"))
        for slot_name, button in self._market_type_buttons.items():
            if not self._slot_state[slot_name]["market_type"]:
                button.setText(tr("spread.choose_type"))
        for slot_name, pair_input in self._pair_inputs.items():
            pair_input.setPlaceholderText(
                tr("spread.enter_symbol") if self._slot_state[slot_name]["market_type"] else tr("spread.choose_instrument")
            )
        for slot_name in self._quote_labels:
            self._clear_quotes(slot_name)
        if self._select_button is not None:
            self._select_button.setText(tr("spread.select"))
        if self._start_button is not None:
            self._start_button.setText(tr("spread.start"))
        if self._stop_button is not None:
            self._stop_button.setText(tr("spread.stop"))
        self._update_signal_mode_button_text()
        if self._simulate_entry_button is not None:
            self._simulate_entry_button.setText(tr("spread.simulate_entry"))
        if self._simulate_exit_button is not None:
            self._simulate_exit_button.setText(tr("spread.simulate_exit"))
        if self._strategy_title_label is not None:
            self._strategy_title_label.setText(tr("spread.strategy_params"))
        for label in self._strategy_field_labels:
            label.setText(tr(str(label.property("i18n_key"))))
        self._update_runtime_controls()

    def _update_signal_mode_button_text(self) -> None:
        if self._signal_mode_button is None:
            return
        if self._strategy_signal_mode == "simulated":
            self._signal_mode_button.setText(tr("spread.signal_mode_simulated"))
        else:
            self._signal_mode_button.setText(tr("spread.signal_mode_market"))

    def apply_theme(self) -> None:
        c_surface = theme_color("surface")
        c_window = theme_color("window_bg")
        c_border = theme_color("border")
        c_primary = theme_color("text_primary")
        c_muted = theme_color("text_muted")
        c_alt = theme_color("surface_alt")
        c_accent = theme_color("accent")
        c_success = theme_color("success")
        c_danger = theme_color("danger")
        c_cheap_border = self._rgba(c_success, 0.76)
        c_cheap_tone = self._rgba(c_success, 0.20)
        c_cheap_hover = self._rgba(c_success, 0.30)
        c_exp_border = self._rgba(c_danger, 0.76)
        c_exp_tone = self._rgba(c_danger, 0.18)
        c_exp_hover = self._rgba(c_danger, 0.28)
        self.container.setStyleSheet(
            f"""
            QFrame#spreadContainer {{
                background: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1, stop: 0 {self._rgba(c_alt, 0.96)}, stop: 1 {self._rgba(c_window, 0.98)});
                border: 1px solid {self._rgba(c_border, 0.58)};
                border-radius: 12px;
            }}
            QPushButton#exchangeSelector {{
                background: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1, stop: 0 {self._rgba(c_accent, 0.18)}, stop: 0.50 {self._rgba(c_alt, 0.95)}, stop: 1 {c_surface});
                color: {c_primary};
                border: 1px solid {self._rgba(c_accent, 0.52)};
                border-radius: 22px;
                min-height: 40px;
                font-size: 12px;
                font-weight: 700;
                padding: 6px 12px;
                text-align: center;
            }}
            QPushButton#subSelector {{
                background: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1, stop: 0 {self._rgba(c_accent, 0.16)}, stop: 0.50 {self._rgba(c_alt, 0.95)}, stop: 1 {c_surface});
                color: {c_primary};
                border: 1px solid {self._rgba(c_accent, 0.48)};
                border-radius: 18px;
                min-height: 32px;
                font-size: 11px;
                font-weight: 700;
                padding: 4px 10px;
                text-align: center;
            }}
            QPushButton#exchangeSelector:hover, QPushButton#subSelector:hover {{
                background: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1, stop: 0 {self._rgba(c_accent, 0.26)}, stop: 0.50 {self._rgba(c_alt, 0.98)}, stop: 1 {self._rgba(c_surface, 0.96)});
                border: 1px solid {self._rgba(c_accent, 0.70)};
            }}
            QPushButton#exchangeSelector[toneRole="cheap"] {{
                border: 1px solid {c_cheap_border};
                background: qlineargradient(
                    x1: 0, y1: 0, x2: 0, y2: 1,
                    stop: 0 {c_cheap_tone},
                    stop: 0.50 {self._rgba(c_alt, 0.95)},
                    stop: 1 {c_surface}
                );
            }}
            QPushButton#exchangeSelector[toneRole="cheap"]:hover {{
                border: 1px solid {c_success};
                background-color: {c_cheap_hover};
            }}
            QPushButton#exchangeSelector[toneRole="expensive"] {{
                border: 1px solid {c_exp_border};
                background: qlineargradient(
                    x1: 0, y1: 0, x2: 0, y2: 1,
                    stop: 0 {c_exp_tone},
                    stop: 0.50 {self._rgba(c_alt, 0.95)},
                    stop: 1 {c_surface}
                );
            }}
            QPushButton#exchangeSelector[toneRole="expensive"]:hover {{
                border: 1px solid {c_danger};
                background-color: {c_exp_hover};
            }}
            QPushButton#exchangeSelector:pressed, QPushButton#subSelector:pressed {{
                background-color: {self._rgba(c_surface, 0.92)};
                border: 1px solid {self._rgba(c_accent, 0.82)};
            }}
            QLineEdit#pairSelector {{
                background-color: {c_alt};
                color: {c_primary};
                border: 1px solid {self._rgba(c_accent, 0.48)};
                border-radius: 12px;
                min-height: 24px;
                padding: 4px 8px;
                font-size: 11px;
                font-weight: 600;
            }}
            QLineEdit#pairSelector:hover {{
                background-color: {self._rgba(c_surface, 0.98)};
                border: 1px solid {self._rgba(c_accent, 0.70)};
            }}
            QLineEdit#pairSelector:focus {{
                background-color: {self._rgba(c_surface, 0.98)};
                border: 1px solid {self._rgba(c_accent, 0.82)};
            }}
            QWidget#transportWidget {{
                background: transparent;
                border: none;
            }}
            QLabel#transportDot {{
                color: {c_muted};
                font-size: 15px;
                font-weight: 900;
                min-width: 10px;
                max-width: 10px;
                background: transparent;
                border: none;
                padding: 0;
            }}
            QLabel#transportDot[activeState="active"] {{
                color: #18e06f;
            }}
            QLabel#transportDot[activeState="inactive"] {{
                color: {c_muted};
            }}
            QPushButton#transportBadge {{
                background-color: transparent;
                color: {c_muted};
                border: 1px solid {self._rgba(c_border, 0.72)};
                border-radius: 9px;
                padding: 0 10px;
                font-size: 11px;
                font-weight: 700;
                min-height: 20px;
            }}
            QPushButton#transportBadge:hover:!disabled {{
                background-color: {self._rgba(c_alt, 0.88)};
            }}
            QPushButton#transportBadge[activeState="active"] {{
                color: {c_primary};
                border: 1px solid {self._rgba(c_accent, 0.95)};
            }}
            QPushButton#transportBadge[activeState="inactive"] {{
                color: {c_muted};
                border: 1px solid {self._rgba(c_border, 0.72)};
            }}
            QPushButton#transportBadge:disabled {{
                background-color: {self._rgba(c_alt, 0.62)};
                color: {c_muted};
                border: 1px solid {self._rgba(c_border, 0.62)};
            }}
            QWidget#quotePanel {{
                background-color: transparent;
                border: none;
            }}
            QFrame#quoteSideCapsule {{
                background: qlineargradient(
                    x1: 0, y1: 0, x2: 0, y2: 1,
                    stop: 0 {self._rgba(c_alt, 0.92)},
                    stop: 1 {self._rgba(c_surface, 0.98)}
                );
                border: 1px solid {self._rgba(c_border, 0.58)};
                border-radius: 12px;
                min-height: 34px;
            }}
            QFrame#quoteMidDivider {{
                background-color: {self._rgba(c_border, 0.42)};
                border: none;
                min-height: 16px;
                max-height: 16px;
            }}
            QFrame#spreadValueFrame {{
                background-color: {self._rgba(c_surface, 0.96)};
                border-top: 1px solid {self._rgba(c_border, 0.62)};
                border-bottom: 1px solid {self._rgba(c_border, 0.62)};
                border-left: 3px solid {self._rgba(c_success, 0.92)};
                border-right: 3px solid {self._rgba(c_danger, 0.92)};
                border-radius: 14px;
            }}
            QFrame#spreadValueFrame[edgeTone="neutral"] {{
                border-left: 3px solid {self._rgba(c_border, 0.76)};
                border-right: 3px solid {self._rgba(c_border, 0.76)};
            }}
            QFrame#spreadValueFrame[edgeTone="left_cheap"] {{
                border-left: 3px solid {self._rgba(c_success, 0.92)};
                border-right: 3px solid {self._rgba(c_danger, 0.92)};
            }}
            QFrame#spreadValueFrame[edgeTone="right_cheap"] {{
                border-left: 3px solid {self._rgba(c_danger, 0.92)};
                border-right: 3px solid {self._rgba(c_success, 0.92)};
            }}
            QFrame#spreadValueInner {{
                background-color: {self._rgba(c_alt, 0.95)};
                border: none;
                border-radius: 12px;
            }}
            QPushButton#spreadActionButton {{
                background-color: {self._rgba(c_surface, 0.84)};
                color: {c_primary};
                border: 1px solid {self._rgba(c_border, 0.74)};
                border-radius: 14px;
                font-size: 20px;
                font-weight: 700;
                padding: 6px 12px;
            }}
            QPushButton#spreadActionButton:hover {{
                background-color: {self._rgba(c_alt, 0.98)};
                border: 1px solid {self._rgba(c_accent, 0.72)};
            }}
            QPushButton#spreadActionButton:pressed {{
                background-color: {self._rgba(c_surface, 0.90)};
                border: 1px solid {self._rgba(c_accent, 0.82)};
            }}
            QPushButton#spreadModeButton {{
                background-color: {self._rgba(c_alt, 0.94)};
                color: {c_primary};
                border: 1px solid {self._rgba(c_border, 0.72)};
                border-radius: 10px;
                font-size: 11px;
                font-weight: 800;
                padding: 4px 10px;
            }}
            QPushButton#spreadModeButton:hover {{
                background-color: {self._rgba(c_surface, 0.98)};
                border: 1px solid {self._rgba(c_accent, 0.76)};
            }}
            QPushButton#spreadModeButton:checked {{
                background-color: {self._rgba(c_success, 0.18)};
                border: 1px solid {self._rgba(c_success, 0.84)};
            }}
            QPushButton#spreadSimulationButton {{
                background-color: {self._rgba(c_alt, 0.92)};
                color: {c_primary};
                border: 1px solid {self._rgba(c_accent, 0.54)};
                border-radius: 10px;
                font-size: 11px;
                font-weight: 700;
                padding: 4px 10px;
            }}
            QPushButton#spreadSimulationButton:hover {{
                background-color: {self._rgba(c_surface, 0.98)};
                border: 1px solid {self._rgba(c_accent, 0.78)};
            }}
            QPushButton#spreadSimulationButton:pressed {{
                background-color: {self._rgba(c_surface, 0.90)};
                border: 1px solid {self._rgba(c_accent, 0.88)};
            }}
            QPushButton#spreadSimulationButton:checked {{
                background-color: {self._rgba(c_success, 0.18)};
                border: 1px solid {self._rgba(c_success, 0.82)};
            }}
            QLabel#spreadValueLabel {{
                color: {c_primary};
                background-color: transparent;
                border: none;
                font-size: 56px;
                font-weight: 800;
            }}
            QLabel#bidLabelText {{
                color: {theme_color('success')};
                font-size: 11px;
                font-weight: 700;
            }}
            QLabel#askLabelText {{
                color: {theme_color('danger')};
                font-size: 11px;
                font-weight: 700;
            }}
            QLabel#quoteQtyLabelText {{
                color: {c_primary};
                font-size: 11px;
                font-weight: 700;
                padding-left: 1px;
            }}
            QLabel#quoteValueText {{
                color: {c_primary};
                font-size: 11px;
                font-weight: 700;
                font-family: "Consolas";
                padding-right: 6px;
            }}
            QFrame#strategyPanel {{
                background-color: {self._rgba(c_surface, 0.72)};
                border: 1px solid {self._rgba(c_border, 0.48)};
                border-radius: 10px;
            }}
            QLabel#strategyTitle {{
                color: {c_primary};
                font-size: 11px;
                font-weight: 700;
                padding-left: 4px;
            }}
            QFrame#strategyFieldCapsule {{
                background: qlineargradient(x1: 1, y1: 0, x2: 0, y2: 0, stop: 0 {self._rgba(c_surface, 0.70)}, stop: 1 {self._rgba(c_alt, 0.94)});
                border: 1px solid {self._rgba(c_border, 0.60)};
                border-radius: 8px;
            }}
            QLabel#strategyFieldInlineLabel {{
                color: {c_muted};
                font-size: 10px;
                font-weight: 600;
                padding-left: 4px;
            }}
            QFrame#strategyFieldDivider {{
                background-color: {self._rgba(c_border, 0.62)};
                border: none;
                min-height: 14px;
                max-height: 14px;
            }}
            QLineEdit#strategyFieldInput {{
                background-color: transparent;
                color: {c_primary};
                border: 1px solid {self._rgba(c_border, 0.68)};
                border-radius: 9px;
                min-height: 20px;
                padding: 0 10px;
                font-size: 12px;
                font-weight: 700;
            }}
            QLineEdit#strategyFieldInput:hover {{
                background-color: {self._rgba(c_alt, 0.88)};
                border: 1px solid {self._rgba(c_accent, 0.78)};
            }}
            QLineEdit#strategyFieldInput:focus {{
                background-color: {self._rgba(c_alt, 0.94)};
                border: 1px solid {self._rgba(c_accent, 0.88)};
            }}
            QScrollArea#runtimesScroll {{
                background: transparent;
                border: none;
            }}
            QScrollArea#runtimesScroll > QWidget > QWidget {{
                background: transparent;
            }}
            """
        )
        for card in self._runtime_cards.values():
            card.apply_theme()
        if self._diagnostics_window is not None:
            self._diagnostics_window.apply_theme()
