from __future__ import annotations

from PySide6.QtCore import QPoint, QSize, QStringListModel, Qt, Signal
from PySide6.QtGui import QFocusEvent, QIcon, QMouseEvent
from PySide6.QtWidgets import QCompleter, QFrame, QGridLayout, QHBoxLayout, QLabel, QLineEdit, QMenu, QPushButton, QSizePolicy, QVBoxLayout, QWidget

from ui.i18n import tr
from ui.theme import theme_color
from ui.widgets.exchange_badge import build_exchange_icon


class PairLineEdit(QLineEdit):
    first_edit_started = Signal()
    field_focused = Signal()
    field_clicked = Signal()

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self._clear_on_next_focus = False

    def mark_selected_value(self) -> None:
        self._clear_on_next_focus = True

    def focusInEvent(self, event: QFocusEvent) -> None:
        super().focusInEvent(event)
        self.field_focused.emit()
        if self._clear_on_next_focus:
            self.clear()
            self._clear_on_next_focus = False
            self.first_edit_started.emit()

    def mousePressEvent(self, event: QMouseEvent) -> None:
        super().mousePressEvent(event)
        self.field_clicked.emit()


class SpreadMockTab(QWidget):
    action_triggered = Signal(str)

    def __init__(self, coordinator=None, parent=None) -> None:
        super().__init__(parent)
        self.coordinator = coordinator
        self._slot_state = {
            "left": {"exchange": None, "market_type": None, "symbol": None},
            "right": {"exchange": None, "market_type": None, "symbol": None},
        }
        self._exchange_buttons: dict[str, QPushButton] = {}
        self._market_type_buttons: dict[str, QPushButton] = {}
        self._pair_inputs: dict[str, PairLineEdit] = {}
        self._pair_models: dict[str, QStringListModel] = {}
        self._pair_completers: dict[str, QCompleter] = {}
        self._quote_labels: dict[str, dict[str, QLabel]] = {}
        self._strategy_field_labels: list[QLabel] = []
        self._spread_value_label: QLabel | None = None
        self._select_button: QPushButton | None = None
        self._strategy_title_label: QLabel | None = None
        self._build_ui()
        self.apply_theme()
        self.retranslate_ui()
        if self.coordinator is not None:
            self.coordinator.public_quote_received.connect(self._on_public_quote_received)
            self.coordinator.public_quote_error.connect(lambda message: self._emit(f"spread:error:{message}"))

    @staticmethod
    def _rgba(hex_color: str, alpha: float) -> str:
        color = str(hex_color or "").strip()
        if color.startswith("#") and len(color) == 7:
            r = int(color[1:3], 16)
            g = int(color[3:5], 16)
            b = int(color[5:7], 16)
            return f"rgba({r}, {g}, {b}, {max(0.0, min(1.0, alpha)):.3f})"
        return color

    def _emit(self, name: str) -> None:
        self.action_triggered.emit(name)

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
        pair_input.field_focused.connect(lambda slot=slot_name: self._show_top_instruments(slot))
        pair_input.field_clicked.connect(lambda slot=slot_name: self._show_top_instruments(slot))

        pair_model = QStringListModel(self)
        pair_completer = QCompleter(pair_model, self)
        pair_completer.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
        pair_completer.setCompletionMode(QCompleter.CompletionMode.PopupCompletion)
        pair_completer.setFilterMode(Qt.MatchFlag.MatchContains)
        pair_completer.setMaxVisibleItems(5)
        pair_completer.activated.connect(lambda text, slot=slot_name: self._set_symbol(slot, text, text))
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
        self._pair_models[slot_name] = pair_model
        self._pair_completers[slot_name] = pair_completer

        col.addWidget(exchange_btn, 0, Qt.AlignmentFlag.AlignCenter)
        col.addWidget(market_type_btn, 0, Qt.AlignmentFlag.AlignCenter)
        col.addWidget(pair_input, 0, Qt.AlignmentFlag.AlignCenter)
        return box

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
            price = QLabel(tr("spread.bid") if side_name == "bid" else tr("spread.ask"))
            price.setObjectName("bidPriceText" if side_name == "bid" else "askPriceText")
            price.setAlignment(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignLeft)
            price.setContentsMargins(8, 0, 8, 0)
            divider = QFrame()
            divider.setObjectName("quoteMidDivider")
            divider.setFixedWidth(1)
            qty = QLabel(tr("spread.qty"))
            qty.setObjectName("quoteQtyText")
            qty.setAlignment(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignRight)
            qty.setContentsMargins(8, 0, 8, 0)
            labels[f"{side_name}_price"] = price
            labels[f"{side_name}_qty"] = qty
            capsule_layout.addWidget(price, 1)
            capsule_layout.addWidget(divider, 0)
            capsule_layout.addWidget(qty, 1)
            row.addWidget(capsule, 1)
        self._quote_labels[slot_name] = labels
        return panel

    def _open_exchange_menu(self, slot_name: str) -> None:
        if self.coordinator is None:
            return
        items = [(code, title, code) for code, title in self.coordinator.available_exchanges()]
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
        self._update_pair_suggestions(slot_name, text, force_popup=True)

    def _update_pair_suggestions(self, slot_name: str, text: str, force_popup: bool = False) -> None:
        state = self._slot_state[slot_name]
        if self.coordinator is None:
            return
        exchange = state["exchange"]
        market_type = state["market_type"]
        if not exchange or not market_type:
            return
        query = str(text or "").strip().upper()
        symbols = self.coordinator.list_instruments(exchange, market_type)
        if query:
            starts_with = [symbol for symbol in symbols if symbol.startswith(query)]
            contains = [symbol for symbol in symbols if query in symbol and not symbol.startswith(query)]
            filtered = [*starts_with, *contains][:5]
        else:
            filtered = symbols[:5]

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
        symbol = pair_input.text().strip().upper()
        if symbol:
            self._set_symbol(slot_name, symbol, symbol)

    def _set_exchange(self, slot_name: str, exchange: str, title: str) -> None:
        self._slot_state[slot_name]["exchange"] = exchange
        self._slot_state[slot_name]["market_type"] = None
        self._slot_state[slot_name]["symbol"] = None
        self._apply_exchange_button_state(slot_name, exchange, title)
        self._market_type_buttons[slot_name].setText(tr("spread.choose_type"))
        self._pair_inputs[slot_name].clear()
        self._pair_inputs[slot_name].setPlaceholderText(tr("spread.choose_instrument"))
        self._clear_quotes(slot_name)
        if self.coordinator is not None:
            self.coordinator.prefetch_exchange_instruments(exchange)
        self._emit(f"spread:exchange:{slot_name}:{exchange}")

    def _set_market_type(self, slot_name: str, market_type: str, title: str) -> None:
        self._slot_state[slot_name]["market_type"] = market_type
        self._slot_state[slot_name]["symbol"] = None
        self._market_type_buttons[slot_name].setText(title)
        self._pair_inputs[slot_name].clear()
        self._pair_inputs[slot_name].setPlaceholderText(tr("spread.enter_symbol"))
        self._clear_quotes(slot_name)
        self._emit(f"spread:market_type:{slot_name}:{market_type}")

    def _set_symbol(self, slot_name: str, symbol: str, title: str) -> None:
        self._slot_state[slot_name]["symbol"] = symbol
        pair_input = self._pair_inputs[slot_name]
        pair_input.setText(title)
        pair_input.mark_selected_value()
        self._clear_quotes(slot_name)
        self._emit(f"spread:instrument:{slot_name}:{symbol}")
        self._subscribe_slot(slot_name)

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
        labels = self._quote_labels.get(slot_name)
        if labels is None:
            return
        labels["bid_price"].setText(tr("spread.bid"))
        labels["ask_price"].setText(tr("spread.ask"))
        labels["bid_qty"].setText(tr("spread.qty"))
        labels["ask_qty"].setText(tr("spread.qty"))

    def _on_public_quote_received(self, slot_name: str, quote_data: object) -> None:
        if not isinstance(quote_data, dict):
            return
        labels = self._quote_labels.get(slot_name)
        if labels is None:
            return
        bid_price = quote_data.get("bid", "--")
        ask_price = quote_data.get("ask", "--")
        bid_qty = quote_data.get("bid_qty", "--")
        ask_qty = quote_data.get("ask_qty", "--")
        labels["bid_price"].setText(tr("spread.bid_value", value=bid_price))
        labels["ask_price"].setText(tr("spread.ask_value", value=ask_price))
        labels["bid_qty"].setText(tr("spread.qty_value", value=self._format_ui_notional_usdt(bid_price, bid_qty)))
        labels["ask_qty"].setText(tr("spread.qty_value", value=self._format_ui_notional_usdt(ask_price, ask_qty)))
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
            cell_layout.setContentsMargins(8, 3, 8, 3)
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
            self._strategy_title_label.setText(tr("spread.strategy"))
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
            QLabel#bidPriceText {{
                color: {theme_color('success')};
                font-size: 11px;
                font-weight: 700;
                font-family: Consolas, "Courier New", monospace;
            }}
            QLabel#askPriceText {{
                color: {theme_color('danger')};
                font-size: 11px;
                font-weight: 700;
                font-family: Consolas, "Courier New", monospace;
            }}
            QLabel#quoteQtyText {{
                color: {c_primary};
                font-size: 11px;
                font-weight: 700;
                padding-right: 1px;
                font-family: Consolas, "Courier New", monospace;
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
            }}
            QFrame#strategyFieldCapsule {{
                background: qlineargradient(x1: 1, y1: 0, x2: 0, y2: 0, stop: 0 {self._rgba(c_surface, 0.70)}, stop: 1 {self._rgba(c_alt, 0.94)});
                border: 1px solid {self._rgba(c_border, 0.68)};
                border-radius: 8px;
            }}
            QLabel#strategyFieldInlineLabel {{
                color: {c_muted};
                font-size: 10px;
                font-weight: 600;
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
                border: none;
                min-height: 22px;
                padding: 0 2px;
                font-size: 13px;
                font-weight: 700;
                text-align: right;
            }}
            QPushButton#strategyFieldInputButton:hover {{
                color: {c_accent};
            }}
            QPushButton#strategyFieldInputButton:pressed {{
                color: {self._rgba(c_accent, 0.82)};
            }}
            """
        )
