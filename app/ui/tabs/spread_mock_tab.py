from __future__ import annotations

import json
from pathlib import Path

from PySide6.QtCore import QPoint, QSize, QStringListModel, Qt, QTimer, Signal
from PySide6.QtGui import QAction, QColor, QFocusEvent, QIcon, QMouseEvent, QPainter, QPen, QPixmap
from PySide6.QtWidgets import QApplication, QCompleter, QFrame, QGridLayout, QHBoxLayout, QLabel, QLineEdit, QMenu, QPushButton, QSizePolicy, QStyle, QVBoxLayout, QWidget

from app.ui.exchange_store import load_exchange_cards
from app.ui.i18n import tr
from app.ui.theme import theme_color
from app.ui.widgets.exchange_badge import build_exchange_icon


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
        self._strategy_field_labels: list[QLabel] = []
        self._spread_value_label: QLabel | None = None
        self._select_button: QPushButton | None = None
        self._strategy_title_label: QLabel | None = None
        self._ui_quote_timer = QTimer(self)
        self._ui_quote_timer.setInterval(50)
        self._ui_quote_timer.timeout.connect(self._flush_pending_quotes)
        self._build_ui()
        app = QApplication.instance()
        if app is not None:
            app.installEventFilter(self)
        self.apply_theme()
        self.retranslate_ui()
        if self.coordinator is not None:
            self.coordinator.public_quote_received.connect(self._on_public_quote_received)
            self.coordinator.public_quote_error.connect(lambda message: self._emit(f"spread:error:{message}"))
            self.coordinator.instruments_loaded.connect(self._on_instruments_loaded)
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

    def _save_state(self) -> None:
        payload = {
            "spread_slots": {
                slot_name: {
                    "exchange": state["exchange"],
                    "market_type": state["market_type"],
                    "symbol": state["symbol"],
                }
                for slot_name, state in self._slot_state.items()
            }
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
        if not isinstance(slots, dict):
            return {}
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
        return result

    def _restore_saved_state(self) -> None:
        if self.coordinator is None:
            return
        saved = self._load_saved_state()
        if not saved:
            return
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
        labels["bid_price"].setText(str(bid_price))
        labels["ask_price"].setText(str(ask_price))
        labels["bid_qty"].setText(f"{self._format_ui_notional_usdt(bid_price, bid_qty)} USDT")
        labels["ask_qty"].setText(f"{self._format_ui_notional_usdt(ask_price, ask_qty)} USDT")
        if self._spread_value_label is not None:
            left = self._slot_state["left"]["symbol"]
            right = self._slot_state["right"]["symbol"]
            self._spread_value_label.setText("LIVE" if left and right else "--")

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
            ("spread.target_size", "100"),
            ("spread.step_size", "20"),
            ("spread.max_slippage", "0.02"),
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
            edit = QPushButton(value)
            edit.setObjectName("strategyFieldInputButton")
            edit.setMaximumWidth(72)
            edit.setMinimumWidth(56)
            edit.setFixedHeight(24)
            cell_layout.addWidget(label, 1)
            cell_layout.addWidget(divider, 0)
            cell_layout.addWidget(edit, 0)
            grid.addWidget(cell, 0, idx)
        strategy_layout.addLayout(grid)
        layout.addWidget(strategy)
        layout.addStretch(1)

    def _subscribe_both(self) -> None:
        self._subscribe_slot("left")
        self._subscribe_slot("right")
        self._emit("spread:select")

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
        if self._strategy_title_label is not None:
            self._strategy_title_label.setText(tr("spread.strategy_params"))
        for label in self._strategy_field_labels:
            label.setText(tr(str(label.property("i18n_key"))))

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
            QPushButton#strategyFieldInputButton {{
                background-color: transparent;
                color: {c_primary};
                border: 1px solid {self._rgba(c_border, 0.68)};
                border-radius: 9px;
                min-height: 20px;
                padding: 0 10px;
                font-size: 12px;
                font-weight: 700;
                text-align: right;
            }}
            QPushButton#strategyFieldInputButton:hover {{
                background-color: {self._rgba(c_alt, 0.88)};
                border: 1px solid {self._rgba(c_accent, 0.78)};
            }}
            QPushButton#strategyFieldInputButton:pressed {{
                background-color: {self._rgba(c_alt, 0.94)};
                border: 1px solid {self._rgba(c_accent, 0.88)};
            }}
            """
        )

