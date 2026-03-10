from __future__ import annotations

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


class DualQuotesRuntimeTabPartsMixin:
    def _build_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(10)

        hero = QFrame()
        hero.setObjectName("dualHero")
        hero_layout = QVBoxLayout(hero)
        hero_layout.setContentsMargins(14, 14, 14, 14)
        hero_layout.setSpacing(10)

        self.title_label = QLabel()
        self.title_label.setObjectName("dualTitle")
        hero_layout.addWidget(self.title_label)

        self.subtitle_label = QLabel()
        self.subtitle_label.setObjectName("dualSubtitle")
        self.subtitle_label.setWordWrap(True)
        hero_layout.addWidget(self.subtitle_label)

        selector_row = QHBoxLayout()
        selector_row.setSpacing(14)
        selector_row.addWidget(self._build_leg_selector("left", "Binance"), 1)
        selector_row.addWidget(self._build_leg_selector("right", "Bitget"), 1)
        hero_layout.addLayout(selector_row)

        controls = QHBoxLayout()
        controls.setSpacing(8)
        self.start_btn = QPushButton()
        self.start_spread_btn = QPushButton()
        self.stop_btn = QPushButton()
        self.stop_spread_btn = QPushButton()
        self.send_both_btn = QPushButton()
        self.start_btn.clicked.connect(self._start_runtime)
        self.start_spread_btn.clicked.connect(self._start_spread_entry_runtime)
        self.stop_btn.clicked.connect(self._stop_runtime)
        self.stop_spread_btn.clicked.connect(self._stop_runtime)
        self.send_both_btn.clicked.connect(self._send_both_legs)
        controls.addWidget(self.start_btn, 0)
        controls.addWidget(self.start_spread_btn, 0)
        controls.addWidget(self.stop_btn, 0)
        controls.addWidget(self.stop_spread_btn, 0)
        controls.addWidget(self.send_both_btn, 0)
        controls.addStretch(1)
        hero_layout.addLayout(controls)
        root.addWidget(hero)

        exec_frame = QFrame()
        exec_frame.setObjectName("dualExecFrame")
        exec_layout = QGridLayout(exec_frame)
        exec_layout.setContentsMargins(12, 12, 12, 12)
        exec_layout.setHorizontalSpacing(10)
        exec_layout.setVerticalSpacing(10)

        self.left_side_select = QComboBox()
        self.left_side_select.setObjectName("dualSelect")
        self.left_side_select.addItems(["BUY", "SELL"])
        self.right_side_select = QComboBox()
        self.right_side_select.setObjectName("dualSelect")
        self.right_side_select.addItems(["BUY", "SELL"])
        self.left_qty_input = QLineEdit("0.001")
        self.left_qty_input.setObjectName("dualInput")
        self.right_qty_input = QLineEdit("0.001")
        self.right_qty_input.setObjectName("dualInput")
        self.left_price_mode_capsule = QLabel(tr("dual.price_mode"))
        self.left_price_mode_capsule.setObjectName("dualCapsule")
        self.right_price_mode_capsule = QLabel(tr("dual.price_mode"))
        self.right_price_mode_capsule.setObjectName("dualCapsule")
        self.entry_threshold_input = QLineEdit("0.0005")
        self.entry_threshold_input.setObjectName("dualInput")
        self.max_quote_age_input = QLineEdit("500")
        self.max_quote_age_input.setObjectName("dualInput")
        self.max_quote_skew_input = QLineEdit("250")
        self.max_quote_skew_input.setObjectName("dualInput")

        self._add_control(exec_layout, 0, tr("dual.left_side"), self.left_side_select)
        self._add_control(exec_layout, 1, tr("dual.left_qty"), self.left_qty_input)
        self._add_control(exec_layout, 2, tr("dual.left_price_mode"), self.left_price_mode_capsule)
        self._add_control(exec_layout, 3, tr("dual.right_side"), self.right_side_select)
        self._add_control(exec_layout, 4, tr("dual.right_qty"), self.right_qty_input)
        self._add_control(exec_layout, 5, tr("dual.right_price_mode"), self.right_price_mode_capsule)
        self._add_control(exec_layout, 6, tr("dual.entry_threshold"), self.entry_threshold_input)
        self._add_control(exec_layout, 7, tr("dual.max_quote_age"), self.max_quote_age_input)
        self._add_control(exec_layout, 8, tr("dual.max_quote_skew"), self.max_quote_skew_input)
        root.addWidget(exec_frame)

        stats = QFrame()
        stats.setObjectName("dualStats")
        stats_layout = QGridLayout(stats)
        stats_layout.setContentsMargins(12, 12, 12, 12)
        stats_layout.setHorizontalSpacing(10)
        stats_layout.setVerticalSpacing(10)

        self.runtime_status_value = self._make_value_label()
        self.spread_state_value = self._make_value_label()
        self.left_bid_value = self._make_value_label()
        self.left_ask_value = self._make_value_label()
        self.right_bid_value = self._make_value_label()
        self.right_ask_value = self._make_value_label()
        self.left_age_value = self._make_value_label()
        self.right_age_value = self._make_value_label()
        self.edge_1_value = self._make_value_label()
        self.edge_2_value = self._make_value_label()
        self.left_order_status_value = self._make_value_label()
        self.right_order_status_value = self._make_value_label()
        self.left_ack_latency_value = self._make_value_label()
        self.right_ack_latency_value = self._make_value_label()
        self.left_fill_latency_value = self._make_value_label()
        self.right_fill_latency_value = self._make_value_label()
        self.left_filled_qty_value = self._make_value_label()
        self.right_filled_qty_value = self._make_value_label()
        self.dual_exec_status_value = self._make_value_label()
        self.active_edge_value = self._make_value_label()
        self.entry_direction_value = self._make_value_label()
        self.entry_block_reason_value = self._make_value_label()
        self.entry_count_value = self._make_value_label()
        self.last_entry_ts_value = self._make_value_label()
        self.exec_stream_connected_value = self._make_value_label()
        self.exec_stream_authenticated_value = self._make_value_label()
        self.exec_stream_reconnects_value = self._make_value_label()
        self.exec_stream_last_error_value = self._make_value_label()

        self._add_stat(stats_layout, 0, tr("dual.status"), self.runtime_status_value)
        self._add_stat(stats_layout, 1, tr("dual.spread_state"), self.spread_state_value)
        self._add_stat(stats_layout, 2, tr("dual.left_bid"), self.left_bid_value)
        self._add_stat(stats_layout, 3, tr("dual.left_ask"), self.left_ask_value)
        self._add_stat(stats_layout, 4, tr("dual.right_bid"), self.right_bid_value)
        self._add_stat(stats_layout, 5, tr("dual.right_ask"), self.right_ask_value)
        self._add_stat(stats_layout, 6, tr("dual.left_age"), self.left_age_value)
        self._add_stat(stats_layout, 7, tr("dual.right_age"), self.right_age_value)
        self._add_stat(stats_layout, 8, tr("dual.edge_1"), self.edge_1_value)
        self._add_stat(stats_layout, 9, tr("dual.edge_2"), self.edge_2_value)
        self._add_stat(stats_layout, 10, tr("dual.left_order_status"), self.left_order_status_value)
        self._add_stat(stats_layout, 11, tr("dual.right_order_status"), self.right_order_status_value)
        self._add_stat(stats_layout, 12, tr("dual.left_ack_latency"), self.left_ack_latency_value)
        self._add_stat(stats_layout, 13, tr("dual.right_ack_latency"), self.right_ack_latency_value)
        self._add_stat(stats_layout, 14, tr("dual.left_fill_latency"), self.left_fill_latency_value)
        self._add_stat(stats_layout, 15, tr("dual.right_fill_latency"), self.right_fill_latency_value)
        self._add_stat(stats_layout, 16, tr("dual.left_filled_qty"), self.left_filled_qty_value)
        self._add_stat(stats_layout, 17, tr("dual.right_filled_qty"), self.right_filled_qty_value)
        self._add_stat(stats_layout, 18, tr("dual.exec_status"), self.dual_exec_status_value)
        self._add_stat(stats_layout, 19, tr("dual.active_edge"), self.active_edge_value)
        self._add_stat(stats_layout, 20, tr("dual.entry_direction"), self.entry_direction_value)
        self._add_stat(stats_layout, 21, tr("dual.entry_block_reason"), self.entry_block_reason_value)
        self._add_stat(stats_layout, 22, tr("dual.entry_count"), self.entry_count_value)
        self._add_stat(stats_layout, 23, tr("dual.last_entry_ts"), self.last_entry_ts_value)
        self._add_stat(stats_layout, 24, "Exec stream connected", self.exec_stream_connected_value)
        self._add_stat(stats_layout, 25, "Exec stream auth", self.exec_stream_authenticated_value)
        self._add_stat(stats_layout, 26, "Exec stream reconnects", self.exec_stream_reconnects_value)
        self._add_stat(stats_layout, 27, "Exec stream last error", self.exec_stream_last_error_value)
        root.addWidget(stats)

        self.log_output = QPlainTextEdit()
        self.log_output.setReadOnly(True)
        self.log_output.setObjectName("dualLog")
        root.addWidget(self.log_output, 1)

        self.left_model = QStringListModel(self)
        self.left_completer = QCompleter(self.left_model, self)
        self.left_completer.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
        self.left_completer.setFilterMode(Qt.MatchFlag.MatchContains)
        self.left_completer.setCompletionMode(QCompleter.CompletionMode.PopupCompletion)
        self.left_completer.activated.connect(self._on_left_symbol_selected)
        self.left_symbol_input.setCompleter(self.left_completer)

        self.right_model = QStringListModel(self)
        self.right_completer = QCompleter(self.right_model, self)
        self.right_completer.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
        self.right_completer.setFilterMode(Qt.MatchFlag.MatchContains)
        self.right_completer.setCompletionMode(QCompleter.CompletionMode.PopupCompletion)
        self.right_completer.activated.connect(self._on_right_symbol_selected)
        self.right_symbol_input.setCompleter(self.right_completer)
        self._update_running_state(False)

    def _build_leg_selector(self, slot_name: str, exchange_title: str) -> QWidget:
        frame = QFrame()
        frame.setObjectName("dualLegSelector")
        layout = QVBoxLayout(frame)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(8)

        title = QLabel(exchange_title)
        title.setObjectName("dualLegTitle")
        layout.addWidget(title)

        market_type = QLabel(tr("dual.market_type"))
        market_type.setObjectName("dualCapsule")
        layout.addWidget(market_type, 0, Qt.AlignmentFlag.AlignLeft)

        input_field = QLineEdit()
        input_field.setObjectName("dualInput")
        input_field.textEdited.connect(self._on_left_symbol_edited if slot_name == "left" else self._on_right_symbol_edited)
        input_field.returnPressed.connect(self._hide_completers)
        layout.addWidget(input_field)

        if slot_name == "left":
            self.left_symbol_input = input_field
        else:
            self.right_symbol_input = input_field
        return frame

    def _bind_coordinator(self) -> None:
        if self.coordinator is None:
            return
        self.coordinator.instruments_loaded.connect(self._on_instruments_loaded)
        self.coordinator.worker_state_updated.connect(self._on_worker_state_updated)
        self.coordinator.worker_event_received.connect(self._on_worker_event_received)
        self.coordinator.worker_command_failed.connect(self._on_worker_command_failed)

    def _prefetch(self) -> None:
        if self.coordinator is None:
            return
        self.coordinator.prefetch_market_type("binance", "perpetual")
        self.coordinator.prefetch_market_type("bitget", "perpetual")

    def _on_instruments_loaded(self, exchange: str, market_type: str) -> None:
        if market_type != "perpetual" or self.coordinator is None:
            return
        if exchange == "binance":
            items = self.coordinator.list_instrument_items("binance", "perpetual")
            self._left_display_to_symbol = {
                str(item.get("display", "")).strip().upper(): str(item.get("symbol", "")).strip().upper()
                for item in items
            }
            self._left_symbols = [str(item.get("display", "")).strip().upper() for item in items if str(item.get("display", "")).strip()]
            self.left_model.setStringList(self._left_symbols)
            self._append_log(f"instruments loaded: {exchange} perpetual ({len(self._left_symbols)})")
            return
        if exchange == "bitget":
            items = self.coordinator.list_instrument_items("bitget", "perpetual")
            self._right_display_to_symbol = {
                str(item.get("display", "")).strip().upper(): str(item.get("symbol", "")).strip().upper()
                for item in items
            }
            self._right_symbols = [str(item.get("display", "")).strip().upper() for item in items if str(item.get("display", "")).strip()]
            self.right_model.setStringList(self._right_symbols)
            self._append_log(f"instruments loaded: {exchange} perpetual ({len(self._right_symbols)})")

    def _on_left_symbol_edited(self, text: str) -> None:
        self._filter_symbols(text, self._left_symbols, self.left_model, self.left_completer)

    def _on_right_symbol_edited(self, text: str) -> None:
        self._filter_symbols(text, self._right_symbols, self.right_model, self.right_completer)

    @staticmethod
    def _filter_symbols(text: str, symbols: list[str], model: QStringListModel, completer: QCompleter) -> None:
        normalized = str(text or "").strip().upper()
        if not normalized:
            filtered = symbols[:10]
        else:
            starts = [symbol for symbol in symbols if symbol.startswith(normalized)]
            contains = [symbol for symbol in symbols if normalized in symbol and symbol not in starts]
            filtered = [*starts, *contains][:10]
        model.setStringList(filtered)
        completer.complete()

    def _on_left_symbol_selected(self, symbol: str) -> None:
        self.left_symbol_input.setText(str(symbol or "").strip().upper())
        self._hide_completers()

    def _on_right_symbol_selected(self, symbol: str) -> None:
        self.right_symbol_input.setText(str(symbol or "").strip().upper())
        self._hide_completers()

    def _hide_completers(self) -> None:
        self.left_completer.popup().hide()
        self.right_completer.popup().hide()

    def _start_runtime(self) -> None:
        if self.coordinator is None:
            return
        left_symbol = self.left_symbol_input.text().strip().upper()
        right_symbol = self.right_symbol_input.text().strip().upper()
        if not left_symbol or not right_symbol:
            self._append_log("ERROR: both symbols are required")
            return
        self.coordinator.start_dual_quotes_runtime_async(
            worker_id=self.worker_id,
            left_exchange="binance",
            left_market_type="perpetual",
            left_symbol=left_symbol,
            right_exchange="bitget",
            right_market_type="perpetual",
            right_symbol=right_symbol,
        )
        self.action_triggered.emit("dual_runtime:start")

    def _start_spread_entry_runtime(self) -> None:
        if self.coordinator is None:
            return
        left_credentials = self._load_connected_exchange_credentials("binance")
        right_credentials = self._load_connected_exchange_credentials("bitget")
        if left_credentials is None or right_credentials is None:
            self._append_log("ERROR: both connected exchange credentials are required")
            return
        left_symbol = self.left_symbol_input.text().strip().upper()
        right_symbol = self.right_symbol_input.text().strip().upper()
        if not left_symbol or not right_symbol:
            self._append_log("ERROR: both symbols are required")
            return
        self.coordinator.start_spread_entry_runtime_async(
            worker_id=self.worker_id,
            left_exchange="binance",
            left_market_type="perpetual",
            left_symbol=left_symbol,
            left_api_key=str(left_credentials.get("api_key", "")),
            left_api_secret=str(left_credentials.get("api_secret", "")),
            left_api_passphrase=str(left_credentials.get("api_passphrase", "")),
            left_account_profile=self._runtime_account_profile("binance", left_credentials),
            right_exchange="bitget",
            right_market_type="perpetual",
            right_symbol=right_symbol,
            right_api_key=str(right_credentials.get("api_key", "")),
            right_api_secret=str(right_credentials.get("api_secret", "")),
            right_api_passphrase=str(right_credentials.get("api_passphrase", "")),
            right_account_profile=self._runtime_account_profile("bitget", right_credentials),
            entry_threshold=self.entry_threshold_input.text().strip() or "0.0005",
            exit_threshold="0",
            max_quote_age_ms=self.max_quote_age_input.text().strip() or "500",
            max_quote_skew_ms=self.max_quote_skew_input.text().strip() or "250",
            left_qty=self.left_qty_input.text().strip() or "0.001",
            right_qty=self.right_qty_input.text().strip() or "0.001",
        )
        self.action_triggered.emit("dual_runtime:start_spread_entry")

    def _stop_runtime(self) -> None:
        if self.coordinator is None:
            return
        self.coordinator.stop_test_runtime(self.worker_id)
        self.action_triggered.emit("dual_runtime:stop")

    def _send_both_legs(self) -> None:
        if self.coordinator is None:
            return
        self.coordinator.submit_dual_test_orders_async(
            worker_id=self.worker_id,
            left_side=self.left_side_select.currentText(),
            left_qty=self.left_qty_input.text().strip(),
            right_side=self.right_side_select.currentText(),
            right_qty=self.right_qty_input.text().strip(),
        )
        self.action_triggered.emit("dual_runtime:send_both")

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
        self._render_state(state)

    def _render_state(self, state: dict) -> None:
        metrics = state.get("metrics", {}) if isinstance(state.get("metrics"), dict) else {}
        left_bid = metrics.get("left_bid") or metrics.get("bid")
        left_ask = metrics.get("left_ask") or metrics.get("ask")
        self.runtime_status_value.setText(str(state.get("status", "-")))
        self.spread_state_value.setText(str(metrics.get("spread_state") or "-"))
        self.left_bid_value.setText(str(left_bid or "-"))
        self.left_ask_value.setText(str(left_ask or "-"))
        self.right_bid_value.setText(str(metrics.get("right_bid") or "-"))
        self.right_ask_value.setText(str(metrics.get("right_ask") or "-"))
        self.left_age_value.setText(self._format_age(metrics.get("left_quote_age_ms")))
        self.right_age_value.setText(self._format_age(metrics.get("right_quote_age_ms")))
        self.edge_1_value.setText(str(metrics.get("edge_1") or "-"))
        self.edge_2_value.setText(str(metrics.get("edge_2") or "-"))
        self.left_order_status_value.setText(str(metrics.get("left_order_status") or "-"))
        self.right_order_status_value.setText(str(metrics.get("right_order_status") or "-"))
        self.left_ack_latency_value.setText(self._format_age(metrics.get("left_ack_latency_ms")))
        self.right_ack_latency_value.setText(self._format_age(metrics.get("right_ack_latency_ms")))
        self.left_fill_latency_value.setText(self._format_age(metrics.get("left_fill_latency_ms")))
        self.right_fill_latency_value.setText(self._format_age(metrics.get("right_fill_latency_ms")))
        self.left_filled_qty_value.setText(str(metrics.get("left_filled_qty") or "-"))
        self.right_filled_qty_value.setText(str(metrics.get("right_filled_qty") or "-"))
        self.dual_exec_status_value.setText(str(metrics.get("dual_exec_status") or "-"))
        self.active_edge_value.setText(str(metrics.get("active_edge") or "-"))
        self.entry_direction_value.setText(str(metrics.get("entry_direction") or "-"))
        self.entry_block_reason_value.setText(str(metrics.get("entry_block_reason") or "-"))
        self.entry_count_value.setText(str(metrics.get("entry_count") or "-"))
        self.last_entry_ts_value.setText(self._format_timestamp(metrics.get("last_entry_ts")))
        stream_health = metrics.get("execution_stream_health") if isinstance(metrics.get("execution_stream_health"), dict) else {}
        self.exec_stream_connected_value.setText(self._format_stream_health_connected(stream_health))
        self.exec_stream_authenticated_value.setText(self._format_stream_health_authenticated(stream_health))
        self.exec_stream_reconnects_value.setText(self._format_stream_health_reconnects(stream_health))
        self.exec_stream_last_error_value.setText(self._format_stream_health_last_error(stream_health))
        self._apply_execution_stream_health_tone(str(metrics.get("execution_stream_health_status") or "UNKNOWN"))
        if state.get("last_error"):
            self._append_log(f"ERROR: {state.get('last_error')}")
        self._update_running_state(str(state.get("status", "")) == "running")

    def _on_worker_event_received(self, worker_id: str, event: object) -> None:
        if worker_id != self.worker_id or not isinstance(event, dict):
            return
        event_type = str(event.get("event_type", "event"))
        payload = event.get("payload", {})
        if event_type in {
            "runtime_started",
            "runtime_stopped",
            "runtime_error",
            "dual_exec_started",
            "left_order_ack",
            "right_order_ack",
            "left_order_event",
            "right_order_event",
            "left_order_filled",
            "right_order_filled",
            "dual_exec_done",
            "dual_exec_failed",
            "entry_signal_detected",
            "entry_blocked",
            "entry_started",
            "entry_left_ack",
            "entry_right_ack",
            "entry_left_event",
            "entry_right_event",
            "entry_left_fill",
            "entry_right_fill",
            "entry_done",
            "entry_failed",
            "execution_stream_health_updated",
            "execution_stream_health_warning",
        }:
            self._append_log(f"{event_type}: {payload}")

    def _on_worker_command_failed(self, worker_id: str, message: str) -> None:
        if worker_id != self.worker_id:
            return
        self._append_log(f"ERROR: {message}")

    def _update_running_state(self, running: bool) -> None:
        self._running = bool(running)
        self.start_btn.setEnabled(not self._running)
        self.start_spread_btn.setEnabled(not self._running)
        self.stop_btn.setEnabled(self._running)
        self.stop_spread_btn.setEnabled(self._running)
        self.send_both_btn.setEnabled(self._running)

    def _append_log(self, line: str) -> None:
        self.log_output.appendPlainText(str(line))

    @staticmethod
    def _format_age(value: object) -> str:
        if value in (None, "", "-"):
            return "-"
        try:
            return f"{int(value)} ms"
        except (TypeError, ValueError):
            return str(value)

    @staticmethod
    def _format_timestamp(value: object) -> str:
        if value in (None, "", "-"):
            return "-"
        try:
            return str(int(value))
        except (TypeError, ValueError):
            return str(value)

    @staticmethod
    def _format_stream_health_connected(snapshot: dict) -> str:
        streams = snapshot.get("streams") if isinstance(snapshot.get("streams"), dict) else {}
        if not streams:
            return "-"
        parts = [f"{leg}:{'Y' if bool(item.get('connected')) else 'N'}" for leg, item in streams.items() if isinstance(item, dict)]
        return " ".join(parts) if parts else "-"

    @staticmethod
    def _format_stream_health_authenticated(snapshot: dict) -> str:
        streams = snapshot.get("streams") if isinstance(snapshot.get("streams"), dict) else {}
        if not streams:
            return "-"
        parts: list[str] = []
        for leg, item in streams.items():
            if not isinstance(item, dict):
                continue
            value = item.get("authenticated")
            if value is None:
                parts.append(f"{leg}:-")
            else:
                parts.append(f"{leg}:{'Y' if bool(value) else 'N'}")
        return " ".join(parts) if parts else "-"

    @staticmethod
    def _format_stream_health_reconnects(snapshot: dict) -> str:
        streams = snapshot.get("streams") if isinstance(snapshot.get("streams"), dict) else {}
        if not streams:
            return "-"
        parts = [f"{leg}:{int(item.get('reconnect_attempts_total') or 0)}" for leg, item in streams.items() if isinstance(item, dict)]
        return " ".join(parts) if parts else "-"

    @staticmethod
    def _format_stream_health_last_error(snapshot: dict) -> str:
        streams = snapshot.get("streams") if isinstance(snapshot.get("streams"), dict) else {}
        if not streams:
            return "-"
        errors: list[str] = []
        for leg, item in streams.items():
            if not isinstance(item, dict):
                continue
            message = str(item.get("last_error") or "").strip()
            if message:
                errors.append(f"{leg}:{message}")
        return " | ".join(errors) if errors else "-"

    def _apply_execution_stream_health_tone(self, status: str) -> None:
        tone = self._health_status_to_tone(status)
        for label in (
            self.exec_stream_connected_value,
            self.exec_stream_authenticated_value,
            self.exec_stream_reconnects_value,
            self.exec_stream_last_error_value,
        ):
            if str(label.property("healthTone") or "unknown") == tone:
                continue
            label.setProperty("healthTone", tone)
            label.style().unpolish(label)
            label.style().polish(label)
            label.update()

    @staticmethod
    def _health_status_to_tone(status: str) -> str:
        normalized = str(status or "").strip().upper()
        if normalized == "HEALTHY":
            return "healthy"
        if normalized == "DEGRADED":
            return "degraded"
        if normalized == "DISCONNECTED":
            return "disconnected"
        return "unknown"

    @staticmethod
    def _make_value_label() -> QLabel:
        label = QLabel("-")
        label.setObjectName("dualValue")
        mono = QFont("Consolas")
        mono.setPointSize(10)
        mono.setWeight(QFont.Weight.DemiBold)
        label.setFont(mono)
        return label

    @staticmethod
    def _add_stat(layout: QGridLayout, row: int, title: str, value: QLabel) -> None:
        title_label = QLabel(title)
        title_label.setObjectName("dualStatTitle")
        layout.addWidget(title_label, row, 0)
        layout.addWidget(value, row, 1)

    @staticmethod
    def _add_control(layout: QGridLayout, row: int, title: str, widget: QWidget) -> None:
        title_label = QLabel(title)
        title_label.setObjectName("dualStatTitle")
        layout.addWidget(title_label, row, 0)
        layout.addWidget(widget, row, 1)

    def retranslate_ui(self) -> None:
        self.title_label.setText(tr("dual.title"))
        self.subtitle_label.setText(tr("dual.subtitle"))
        self.left_symbol_input.setPlaceholderText(tr("dual.left_placeholder"))
        self.right_symbol_input.setPlaceholderText(tr("dual.right_placeholder"))
        self.left_price_mode_capsule.setText(tr("dual.price_mode"))
        self.right_price_mode_capsule.setText(tr("dual.price_mode"))
        self.start_btn.setText(tr("dual.start"))
        self.start_spread_btn.setText(tr("dual.start_spread_entry"))
        self.stop_btn.setText(tr("dual.stop"))
        self.stop_spread_btn.setText(tr("dual.stop_spread_entry"))
        self.send_both_btn.setText(tr("dual.send_both"))

    def apply_theme(self) -> None:
        self.setStyleSheet(
            f"""
            QFrame#dualHero, QFrame#dualStats, QFrame#dualLegSelector, QFrame#dualExecFrame {{
                background-color: {theme_color('surface')};
                border: 1px solid {theme_color('border')};
                border-radius: 16px;
            }}
            QLabel#dualTitle {{
                color: {theme_color('text_primary')};
                font-size: 22px;
                font-weight: 800;
            }}
            QLabel#dualSubtitle {{
                color: {theme_color('text_muted')};
                font-size: 12px;
            }}
            QLabel#dualLegTitle {{
                color: {theme_color('text_primary')};
                font-size: 13px;
                font-weight: 700;
            }}
            QLabel#dualCapsule {{
                background-color: {theme_color('surface_alt')};
                color: {theme_color('text_primary')};
                border: 1px solid {theme_color('border')};
                border-radius: 10px;
                padding: 6px 10px;
                font-weight: 700;
            }}
            QLineEdit#dualInput {{
                background-color: {theme_color('window_bg')};
                color: {theme_color('text_primary')};
                border: 1px solid {theme_color('border')};
                border-radius: 10px;
                padding: 7px 10px;
            }}
            QComboBox#dualSelect {{
                background-color: {theme_color('surface_alt')};
                color: {theme_color('text_primary')};
                border: 1px solid {theme_color('border')};
                border-radius: 10px;
                padding: 6px 10px;
                min-width: 92px;
                font-weight: 700;
            }}
            QComboBox#dualSelect::drop-down {{
                width: 18px;
                border: none;
            }}
            QLabel#dualStatTitle {{
                color: {theme_color('text_muted')};
                font-size: 11px;
                font-weight: 700;
            }}
            QLabel#dualValue {{
                color: {theme_color('text_primary')};
                font-size: 12px;
                padding: 5px 8px;
                background-color: {theme_color('window_bg')};
                border: 1px solid {theme_color('border')};
                border-radius: 10px;
            }}
            QLabel#dualValue[healthTone="healthy"] {{
                color: {theme_color('success')};
            }}
            QLabel#dualValue[healthTone="degraded"] {{
                color: {theme_color('warning')};
            }}
            QLabel#dualValue[healthTone="disconnected"] {{
                color: {theme_color('danger')};
            }}
            QLabel#dualValue[healthTone="unknown"] {{
                color: {theme_color('text_muted')};
            }}
            QPlainTextEdit#dualLog {{
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
        self.send_both_btn.setStyleSheet(button_style("warning"))

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

    @staticmethod
    def _runtime_account_profile(exchange_code: str, credentials: dict) -> dict:
        profile = dict(credentials.get("account_profile", {}) or {})
        if str(exchange_code or "").strip().lower() == "binance":
            profile.setdefault("selected_execution_route", "binance_usdm_trade_ws")
        elif str(exchange_code or "").strip().lower() == "bitget":
            profile.setdefault("selected_execution_route", "bitget_linear_rest_probe")
            profile.setdefault("preferred_execution_route", "bitget_linear_rest_probe")
        return profile
