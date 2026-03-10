from __future__ import annotations

import time

from PySide6.QtCore import Qt, QStringListModel
from PySide6.QtGui import QFont
from PySide6.QtWidgets import (
    QComboBox,
    QCompleter,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPlainTextEdit,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

from app.ui.exchange_store import load_exchange_cards, resolve_exchange_card_credentials
from app.ui.i18n import tr
from app.ui.theme import button_style, theme_color


class RuntimeTestTabPartsMixin:
    def _build_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(10)

        hero = QFrame()
        hero.setObjectName("runtimeHero")
        hero_layout = QVBoxLayout(hero)
        hero_layout.setContentsMargins(14, 14, 14, 14)
        hero_layout.setSpacing(10)

        title = QLabel()
        title.setObjectName("runtimeTitle")
        self.title_label = title
        hero_layout.addWidget(title)

        subtitle = QLabel()
        subtitle.setObjectName("runtimeSubtitle")
        subtitle.setWordWrap(True)
        self.subtitle_label = subtitle
        hero_layout.addWidget(subtitle)

        selector_row = QHBoxLayout()
        selector_row.setSpacing(10)
        self.exchange_select = QComboBox()
        self.exchange_select.setObjectName("runtimeSelect")
        self.exchange_select.currentIndexChanged.connect(self._on_exchange_changed)
        self.route_select = QComboBox()
        self.route_select.setObjectName("runtimeSelect")
        self.market_type_capsule = QLabel()
        self.market_type_capsule.setObjectName("runtimeCapsule")
        self.symbol_input = QLineEdit()
        self.symbol_input.setObjectName("runtimeInput")
        self.symbol_input.textEdited.connect(self._on_symbol_edited)
        self.symbol_input.returnPressed.connect(self._hide_completer)
        selector_row.addWidget(self.exchange_select, 0)
        selector_row.addWidget(self.route_select, 0)
        selector_row.addWidget(self.market_type_capsule, 0)
        selector_row.addWidget(self.symbol_input, 1)
        hero_layout.addLayout(selector_row)

        transport_mock = QWidget()
        transport_mock.setObjectName("runtimeTransportMock")
        transport_mock_layout = QHBoxLayout(transport_mock)
        transport_mock_layout.setContentsMargins(0, 2, 0, 0)
        transport_mock_layout.setSpacing(18)
        transport_mock_layout.addWidget(
            self._build_transport_side_mock(
                active_route="WS",
                inactive_route="REST",
                inactive_reason="REST для Binance пока не подключен",
                active_kind="primary",
            ),
            0,
        )
        transport_mock_layout.addWidget(
            self._build_transport_side_mock(
                active_route="REST",
                inactive_route="WS",
                inactive_reason="WS для Bitget требует аккаунт UTA",
                active_kind="warning",
            ),
            0,
        )
        transport_mock_layout.addStretch(1)
        hero_layout.addWidget(transport_mock)

        controls_row = QHBoxLayout()
        controls_row.setSpacing(8)
        self.notional_input = QLineEdit("10")
        self.notional_input.setObjectName("runtimeInput")
        self.notional_input.setFixedWidth(100)
        self.start_btn = QPushButton()
        self.stop_btn = QPushButton()
        self.buy_btn = QPushButton("BUY")
        self.sell_btn = QPushButton("SELL")
        self.start_btn.clicked.connect(self._start_runtime)
        self.stop_btn.clicked.connect(self._stop_runtime)
        self.buy_btn.clicked.connect(lambda: self._submit_order("BUY"))
        self.sell_btn.clicked.connect(lambda: self._submit_order("SELL"))
        controls_row.addWidget(self.notional_input, 0)
        controls_row.addWidget(self.start_btn, 0)
        controls_row.addWidget(self.stop_btn, 0)
        controls_row.addWidget(self.buy_btn, 0)
        controls_row.addWidget(self.sell_btn, 0)
        controls_row.addStretch(1)
        hero_layout.addLayout(controls_row)
        root.addWidget(hero)

        stats = QFrame()
        stats.setObjectName("runtimeStats")
        stats_layout = QGridLayout(stats)
        stats_layout.setContentsMargins(12, 12, 12, 12)
        stats_layout.setHorizontalSpacing(10)
        stats_layout.setVerticalSpacing(10)
        self.status_value = self._make_value_label()
        self.bid_value = self._make_value_label()
        self.ask_value = self._make_value_label()
        self.order_status_value = self._make_value_label()
        self.execution_value = self._make_value_label()
        self.fill_value = self._make_value_label()
        self.ack_latency_value = self._make_value_label()
        self.click_to_send_latency_value = self._make_value_label()
        self.send_to_ack_latency_value = self._make_value_label()
        self.first_event_latency_value = self._make_value_label()
        self.send_to_first_event_latency_value = self._make_value_label()
        self.fill_latency_value = self._make_value_label()
        self.send_to_fill_latency_value = self._make_value_label()
        self._add_stat(stats_layout, 0, tr("runtime.status"), self.status_value)
        self._add_stat(stats_layout, 1, tr("runtime.bid"), self.bid_value)
        self._add_stat(stats_layout, 2, tr("runtime.ask"), self.ask_value)
        self._add_stat(stats_layout, 3, tr("runtime.order_status"), self.order_status_value)
        self._add_stat(stats_layout, 4, tr("runtime.execution"), self.execution_value)
        self._add_stat(stats_layout, 5, tr("runtime.fill"), self.fill_value)
        self._add_stat(stats_layout, 6, tr("runtime.ack_latency"), self.ack_latency_value)
        self._add_stat(stats_layout, 7, tr("runtime.click_to_send_latency"), self.click_to_send_latency_value)
        self._add_stat(stats_layout, 8, tr("runtime.send_to_ack_latency"), self.send_to_ack_latency_value)
        self._add_stat(stats_layout, 9, tr("runtime.first_event_latency"), self.first_event_latency_value)
        self._add_stat(stats_layout, 10, tr("runtime.send_to_first_event_latency"), self.send_to_first_event_latency_value)
        self._add_stat(stats_layout, 11, tr("runtime.fill_latency"), self.fill_latency_value)
        self._add_stat(stats_layout, 12, tr("runtime.send_to_fill_latency"), self.send_to_fill_latency_value)
        root.addWidget(stats)

        self.log_output = QPlainTextEdit()
        self.log_output.setReadOnly(True)
        self.log_output.setObjectName("runtimeLog")
        root.addWidget(self.log_output, 1)

        self._symbol_model = QStringListModel(self)
        self._symbol_completer = QCompleter(self._symbol_model, self)
        self._symbol_completer.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
        self._symbol_completer.setFilterMode(Qt.MatchFlag.MatchContains)
        self._symbol_completer.setCompletionMode(QCompleter.CompletionMode.PopupCompletion)
        self._symbol_completer.activated.connect(self._on_symbol_selected)
        self.symbol_input.setCompleter(self._symbol_completer)
        self._update_running_state(False)

    def _build_transport_side_mock(self, *, active_route: str, inactive_route: str, inactive_reason: str, active_kind: str) -> QWidget:
        frame = QWidget()
        frame.setObjectName("runtimeMockSide")
        layout = QHBoxLayout(frame)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)

        segmented = QFrame()
        segmented.setObjectName("runtimeMockSegment")
        segmented_layout = QHBoxLayout(segmented)
        segmented_layout.setContentsMargins(0, 0, 0, 0)
        segmented_layout.setSpacing(8)

        active_button = QPushButton(active_route)
        active_button.setCursor(Qt.CursorShape.PointingHandCursor)
        active_button.setProperty("mockRole", "active")
        active_button.setFixedHeight(20)
        active_button.setMinimumWidth(0)
        active_dot = QLabel("•")
        active_dot.setObjectName("runtimeMockRouteDot")
        active_dot.setProperty("mockDotState", "active")
        inactive_button = QPushButton(inactive_route)
        inactive_button.setProperty("mockRole", "inactive")
        inactive_button.setToolTip(inactive_reason)
        inactive_button.setEnabled(False)
        inactive_button.setFixedHeight(20)
        inactive_button.setMinimumWidth(0)
        inactive_dot = QLabel("•")
        inactive_dot.setObjectName("runtimeMockRouteDot")
        inactive_dot.setProperty("mockDotState", "inactive")

        active_wrap = QHBoxLayout()
        active_wrap.setContentsMargins(0, 0, 0, 0)
        active_wrap.setSpacing(4)
        active_wrap.addWidget(active_dot, 0, Qt.AlignmentFlag.AlignVCenter)
        active_wrap.addWidget(active_button, 0)

        inactive_wrap = QHBoxLayout()
        inactive_wrap.setContentsMargins(0, 0, 0, 0)
        inactive_wrap.setSpacing(4)
        inactive_wrap.addWidget(inactive_dot, 0, Qt.AlignmentFlag.AlignVCenter)
        inactive_wrap.addWidget(inactive_button, 0)

        segmented_layout.addLayout(active_wrap, 0)
        segmented_layout.addLayout(inactive_wrap, 0)
        layout.addWidget(segmented)

        self._transport_mock_controls.append(
            {
                "active_button": active_button,
                "inactive_button": inactive_button,
                "active_dot": active_dot,
                "inactive_dot": inactive_dot,
                "active_kind": active_kind,
            }
        )
        return frame

    def _bind_coordinator(self) -> None:
        if self.coordinator is None:
            return
        self.coordinator.instruments_loaded.connect(self._on_instruments_loaded)
        self.coordinator.worker_state_updated.connect(self._on_worker_state_updated)
        self.coordinator.worker_event_received.connect(self._on_worker_event_received)
        self.coordinator.worker_command_failed.connect(self._on_worker_command_failed)

    def _on_instruments_loaded(self, exchange: str, market_type: str) -> None:
        if exchange != self._current_exchange_code() or market_type != "perpetual" or self.coordinator is None:
            return
        items = self.coordinator.list_instrument_items(exchange, market_type)
        self._display_to_symbol = {
            str(item.get("display", "")).strip().upper(): str(item.get("symbol", "")).strip().upper()
            for item in items
        }
        self._symbols = [str(item.get("display", "")).strip().upper() for item in items if str(item.get("display", "")).strip()]
        self._symbol_model.setStringList(self._symbols)
        self._append_log(f"instruments loaded: {exchange} perpetual ({len(self._symbols)})")

    def _on_symbol_edited(self, text: str) -> None:
        normalized = str(text or "").strip().upper()
        if not normalized:
            filtered = self._symbols[:10]
        else:
            starts = [symbol for symbol in self._symbols if symbol.startswith(normalized)]
            contains = [symbol for symbol in self._symbols if normalized in symbol and symbol not in starts]
            filtered = [*starts, *contains][:10]
        self._symbol_model.setStringList(filtered)
        self._symbol_completer.complete()

    def _on_symbol_selected(self, symbol: str) -> None:
        self.symbol_input.setText(str(symbol or "").strip().upper())
        self._hide_completer()

    def _hide_completer(self) -> None:
        self._symbol_completer.popup().hide()

    def _start_runtime(self) -> None:
        if self.coordinator is None:
            return
        credentials = self._load_connected_exchange_credentials(self._current_exchange_code())
        if credentials is None:
            self._append_log("ERROR: no connected exchange credentials found")
            return
        symbol = self.symbol_input.text().strip().upper()
        if not symbol:
            self._append_log("ERROR: instrument symbol is required")
            return
        self.coordinator.start_test_runtime_async(
            worker_id=self.worker_id,
            exchange=self._current_exchange_code(),
            market_type="perpetual",
            symbol=symbol,
            api_key=str(credentials.get("api_key", "")),
            api_secret=str(credentials.get("api_secret", "")),
            api_passphrase=str(credentials.get("api_passphrase", "")),
            account_profile=self._runtime_account_profile(credentials),
            target_notional=self.notional_input.text().strip() or "10",
        )
        self.action_triggered.emit("runtime:start")

    def _stop_runtime(self) -> None:
        if self.coordinator is None:
            return
        self.coordinator.stop_test_runtime(self.worker_id)
        self.action_triggered.emit("runtime:stop")

    def _submit_order(self, side: str) -> None:
        if self.coordinator is None:
            return
        self.coordinator.submit_test_order_async(self.worker_id, side, submitted_at_ms=int(time.time() * 1000))
        self.action_triggered.emit(f"runtime:order:{side}")

    def _on_worker_state_updated(self, worker_id: str, state: object) -> None:
        if worker_id != self.worker_id or not isinstance(state, dict):
            return
        self._pending_state = dict(state)
        if not self._ui_state_timer.isActive():
            self._ui_state_timer.start()

    def _flush_pending_state(self) -> None:
        if self._pending_state is None:
            self._ui_state_timer.stop()
            return
        state = self._pending_state
        self._pending_state = None
        self._render_worker_state(state)

    def _render_worker_state(self, state: dict) -> None:
        metrics = state.get("metrics", {}) if isinstance(state.get("metrics"), dict) else {}
        self.status_value.setText(str(state.get("status", "-")))
        self.bid_value.setText(str(metrics.get("bid") or "-"))
        self.ask_value.setText(str(metrics.get("ask") or "-"))
        self.order_status_value.setText(str(metrics.get("last_order_status") or metrics.get("last_order_ack_status") or "-"))
        self.execution_value.setText(str(metrics.get("last_execution_type") or "-"))
        self.fill_value.setText(str(metrics.get("last_fill_qty") or "-"))
        self.ack_latency_value.setText(self._format_latency(metrics.get("last_ack_latency_ms")))
        self.click_to_send_latency_value.setText(self._format_latency(metrics.get("last_click_to_send_latency_ms")))
        self.send_to_ack_latency_value.setText(self._format_latency(metrics.get("last_send_to_ack_latency_ms")))
        self.first_event_latency_value.setText(self._format_latency(metrics.get("last_first_event_latency_ms")))
        self.send_to_first_event_latency_value.setText(self._format_latency(metrics.get("last_send_to_first_event_latency_ms")))
        self.fill_latency_value.setText(self._format_latency(metrics.get("last_fill_latency_ms")))
        self.send_to_fill_latency_value.setText(self._format_latency(metrics.get("last_send_to_fill_latency_ms")))
        if state.get("last_error"):
            self._append_log(f"ERROR: {state.get('last_error')}")
        self._update_running_state(str(state.get("status", "")) == "running")

    def _on_worker_event_received(self, worker_id: str, event: object) -> None:
        if worker_id != self.worker_id or not isinstance(event, dict):
            return
        event_type = str(event.get("event_type", "event"))
        if event_type in {"runtime_started", "runtime_stopped", "runtime_error", "order_ack_received", "execution_event_received", "order_failed"}:
            self._append_log(f"{event_type}: {event.get('payload', {})}")

    def _on_worker_command_failed(self, worker_id: str, message: str) -> None:
        if worker_id != self.worker_id:
            return
        self._append_log(f"ERROR: {message}")

    def _update_running_state(self, running: bool) -> None:
        self._running = bool(running)
        self.start_btn.setEnabled(not self._running)
        self.stop_btn.setEnabled(self._running)
        self.buy_btn.setEnabled(self._running)
        self.sell_btn.setEnabled(self._running)

    def _append_log(self, line: str) -> None:
        self.log_output.appendPlainText(str(line))

    @staticmethod
    def _format_latency(value: object) -> str:
        if value in (None, "", "-"):
            return "-"
        try:
            return f"{int(value)} ms"
        except (TypeError, ValueError):
            return str(value)

    def _populate_exchange_options(self) -> None:
        if self.coordinator is None:
            items = [("binance", "Binance")]
        else:
            items = list(self.coordinator.available_execution_exchanges())
        self._exchange_items = items
        self.exchange_select.blockSignals(True)
        self.exchange_select.clear()
        for code, title in items:
            self.exchange_select.addItem(title, code)
        self.exchange_select.blockSignals(False)
        self._populate_route_options()

    def _current_exchange_code(self) -> str:
        return str(self.exchange_select.currentData() or "binance").strip().lower()

    def _prefetch_current_exchange(self) -> None:
        if self.coordinator is None:
            return
        self.coordinator.prefetch_market_type(self._current_exchange_code(), "perpetual")

    def _on_exchange_changed(self, _index: int) -> None:
        self._display_to_symbol = {}
        self._symbols = []
        self._symbol_model.setStringList([])
        self.symbol_input.clear()
        self._populate_route_options()
        self._prefetch_current_exchange()

    @staticmethod
    def _load_connected_exchange_credentials(exchange_code: str) -> dict | None:
        for card in load_exchange_cards():
            if str(card.get("exchange_code", "")).strip().lower() != str(exchange_code or "").strip().lower():
                continue
            if not bool(card.get("connected")):
                continue
            resolved = resolve_exchange_card_credentials(card) or {}
            api_key = str(resolved.get("api_key", "")).strip()
            api_secret = str(resolved.get("api_secret", "")).strip()
            if api_key and api_secret:
                return {
                    "api_key": api_key,
                    "api_secret": api_secret,
                    "api_passphrase": str(resolved.get("api_passphrase", "")).strip(),
                    "account_profile": dict(card.get("account_snapshot", {}).get("account_profile", {}) or {}),
                }
        return None

    def _populate_route_options(self) -> None:
        exchange_code = self._current_exchange_code()
        credentials = self._load_connected_exchange_credentials(exchange_code) or {}
        account_profile = dict(credentials.get("account_profile", {}) or {})
        if self.coordinator is not None:
            routes = list(self.coordinator.available_execution_routes(exchange_code, account_profile))
        else:
            routes = [("binance_usdm_trade_ws", "WS")]
        self.route_select.blockSignals(True)
        self.route_select.clear()
        for route_code, title in routes:
            self.route_select.addItem(title, route_code)
        self.route_select.setEnabled(len(routes) > 1)
        self.route_select.blockSignals(False)

    def _runtime_account_profile(self, credentials: dict) -> dict:
        profile = dict(credentials.get("account_profile", {}) or {})
        selected_route = str(self.route_select.currentData() or "").strip()
        if selected_route:
            profile["selected_execution_route"] = selected_route
        return profile

    @staticmethod
    def _make_value_label() -> QLabel:
        label = QLabel("-")
        label.setObjectName("runtimeValue")
        mono = QFont("Consolas")
        mono.setPointSize(10)
        mono.setWeight(QFont.Weight.DemiBold)
        label.setFont(mono)
        return label

    @staticmethod
    def _add_stat(layout: QGridLayout, row: int, title: str, value: QLabel) -> None:
        title_label = QLabel(title)
        title_label.setObjectName("runtimeStatTitle")
        layout.addWidget(title_label, row, 0)
        layout.addWidget(value, row, 1)

    def retranslate_ui(self) -> None:
        self.title_label.setText(tr("runtime.title"))
        self.subtitle_label.setText(tr("runtime.subtitle"))
        self.market_type_capsule.setText(tr("runtime.market_type"))
        self.symbol_input.setPlaceholderText(tr("runtime.symbol_placeholder"))
        self.notional_input.setPlaceholderText(tr("runtime.notional"))
        self.start_btn.setText(tr("runtime.start"))
        self.stop_btn.setText(tr("runtime.stop"))
        self.buy_btn.setText(tr("runtime.buy"))
        self.sell_btn.setText(tr("runtime.sell"))

    def apply_theme(self) -> None:
        self.setStyleSheet(
            f"""
            QFrame#runtimeHero, QFrame#runtimeStats {{
                background-color: {theme_color('surface')};
                border: 1px solid {theme_color('border')};
                border-radius: 16px;
            }}
            QWidget#runtimeTransportMock {{
                background: transparent;
                border: none;
            }}
            QWidget#runtimeMockSide {{
                background: transparent;
                border: none;
            }}
            QFrame#runtimeMockSegment {{
                background: transparent;
                border: none;
            }}
            QLabel#runtimeTitle {{
                color: {theme_color('text_primary')};
                font-size: 22px;
                font-weight: 800;
            }}
            QLabel#runtimeSubtitle {{
                color: {theme_color('text_muted')};
                font-size: 12px;
            }}
            QLabel#runtimeCapsule {{
                background-color: {theme_color('surface_alt')};
                border: 1px solid {theme_color('border')};
                border-radius: 10px;
                padding: 7px 12px;
                font-weight: 700;
                color: {theme_color('text_primary')};
            }}
            QLabel#runtimeMockRouteDot {{
                font-size: 15px;
                font-weight: 900;
                min-width: 10px;
                max-width: 10px;
                background: transparent;
                border: none;
                padding: 0px;
            }}
            QLabel#runtimeMockRouteDot[mockDotState="active"] {{
                color: #18e06f;
            }}
            QLabel#runtimeMockRouteDot[mockDotState="inactive"] {{
                color: {theme_color('text_muted')};
            }}
            QLineEdit#runtimeInput {{
                background-color: {theme_color('window_bg')};
                color: {theme_color('text_primary')};
                border: 1px solid {theme_color('border')};
                border-radius: 10px;
                padding: 7px 10px;
            }}
            QComboBox#runtimeSelect {{
                background-color: {theme_color('surface_alt')};
                color: {theme_color('text_primary')};
                border: 1px solid {theme_color('border')};
                border-radius: 10px;
                padding: 6px 10px;
                font-weight: 700;
                min-width: 112px;
            }}
            QComboBox#runtimeSelect::drop-down {{
                width: 18px;
                border: none;
            }}
            QComboBox#runtimeSelect QAbstractItemView {{
                background-color: {theme_color('surface')};
                color: {theme_color('text_primary')};
                border: 1px solid {theme_color('border')};
                selection-background-color: {theme_color('surface_alt')};
            }}
            QLabel#runtimeStatTitle {{
                color: {theme_color('text_muted')};
                font-size: 11px;
                font-weight: 700;
            }}
            QLabel#runtimeValue {{
                color: {theme_color('text_primary')};
                font-size: 12px;
                padding: 5px 8px;
                background-color: {theme_color('window_bg')};
                border: 1px solid {theme_color('border')};
                border-radius: 10px;
            }}
            QPlainTextEdit#runtimeLog {{
                background-color: {theme_color('surface')};
                color: {theme_color('text_primary')};
                border: 1px solid {theme_color('border')};
                border-radius: 14px;
                padding: 8px;
            }}
            """
        )
        self.start_btn.setStyleSheet(button_style("primary"))
        self.stop_btn.setStyleSheet(button_style("secondary"))
        self.buy_btn.setStyleSheet(button_style("success"))
        self.sell_btn.setStyleSheet(button_style("warning"))
        for control in self._transport_mock_controls:
            active_button = control["active_button"]
            inactive_button = control["inactive_button"]
            active_kind = str(control["active_kind"])
            accent = theme_color("accent")
            success = theme_color("success")
            warning = theme_color("warning")
            active_border = accent
            if active_kind == "warning":
                active_border = warning
            elif active_kind == "success":
                active_border = success
            active_button.setStyleSheet(
                f"""
                QPushButton {{
                    background-color: transparent;
                    color: {theme_color('text_primary')};
                    border: 1px solid {active_border};
                    border-radius: 9px;
                    padding: 0px 10px;
                    font-size: 11px;
                    font-weight: 700;
                }}
                QPushButton:hover {{
                    background-color: {theme_color('surface_alt')};
                    border: 1px solid {active_border};
                }}
                QPushButton:pressed {{
                    background-color: {theme_color('surface_alt')};
                    border: 1px solid {active_border};
                }}
                """
            )
            inactive_button.setStyleSheet(
                f"""
                QPushButton {{
                    background-color: transparent;
                    color: {theme_color('text_muted')};
                    border: 1px solid {theme_color('border')};
                    border-radius: 9px;
                    padding: 0px 10px;
                    font-size: 11px;
                    font-weight: 700;
                }}
                QPushButton:disabled {{
                    background-color: {theme_color('surface_alt')};
                    color: {theme_color('text_muted')};
                    border: 1px solid {theme_color('border')};
                }}
                """
            )
