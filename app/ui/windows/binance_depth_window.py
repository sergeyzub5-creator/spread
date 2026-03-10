from __future__ import annotations

import json
import threading
import time
from decimal import Decimal, InvalidOperation
from typing import Any

from PySide6.QtCore import QObject, Signal, Qt
from PySide6.QtWidgets import QComboBox, QGridLayout, QHBoxLayout, QLabel, QLineEdit, QPushButton, QTableWidget, QTableWidgetItem, QVBoxLayout, QWidget

from app.core.logging.logger_factory import get_logger
from app.ui.i18n import tr

try:
    import websocket
except ImportError:  # pragma: no cover
    websocket = None


class _DepthWorker(QObject):
    snapshot_received = Signal(dict)
    status_changed = Signal(str)

    def __init__(self) -> None:
        super().__init__()
        self._logger = get_logger("ui.binance_depth")
        self._lock = threading.RLock()
        self._thread: threading.Thread | None = None
        self._running = False
        self._market = "spot"
        self._symbol = "BTCUSDT"
        self._ws_app = None

    def start(self, *, market: str, symbol: str) -> None:
        with self._lock:
            self._market = str(market or "spot").strip().lower()
            self._symbol = str(symbol or "BTCUSDT").strip().upper()
            if self._running:
                self._restart_locked()
                return
            self._running = True
            self._thread = threading.Thread(target=self._run_loop, name="binance-depth20", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        with self._lock:
            self._running = False
            ws_app = self._ws_app
        if ws_app is not None:
            try:
                ws_app.close()
            except Exception:
                pass

    def _restart_locked(self) -> None:
        ws_app = self._ws_app
        if ws_app is not None:
            try:
                ws_app.close()
            except Exception:
                pass

    def _stream_url(self) -> str:
        market = str(self._market).lower()
        symbol = str(self._symbol).lower()
        stream_name = f"{symbol}@depth20@100ms"
        if market == "usdm":
            return f"wss://fstream.binance.com/ws/{stream_name}"
        return f"wss://stream.binance.com:9443/ws/{stream_name}"

    def _run_loop(self) -> None:
        if websocket is None:
            self.status_changed.emit(tr("depth.status.no_ws_lib"))
            return
        backoff_seconds = 1.0
        while True:
            with self._lock:
                if not self._running:
                    return
                stream_url = self._stream_url()
            self.status_changed.emit(tr("depth.status.connecting").format(url=stream_url))
            ws_app = websocket.WebSocketApp(
                stream_url,
                on_open=lambda ws: self._on_open(),
                on_message=lambda ws, message: self._on_message(message),
                on_error=lambda ws, error: self._on_error(error),
                on_close=lambda ws, code, msg: self._on_close(code, msg),
            )
            with self._lock:
                self._ws_app = ws_app
            try:
                ws_app.run_forever()
            except Exception as exc:  # pragma: no cover
                self._logger.error("depth ws loop crashed: %s", exc)
            with self._lock:
                if not self._running:
                    return
            self.status_changed.emit(tr("depth.status.reconnect").format(seconds=int(backoff_seconds)))
            time.sleep(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2.0, 10.0)

    def _on_open(self) -> None:
        self.status_changed.emit(tr("depth.status.connected").format(symbol=self._symbol))

    def _on_message(self, message: str) -> None:
        try:
            payload = json.loads(message)
        except Exception:
            return
        if not isinstance(payload, dict):
            return
        bids = payload.get("bids")
        asks = payload.get("asks")
        if not isinstance(bids, list) or not isinstance(asks, list):
            bids = payload.get("b")
            asks = payload.get("a")
        if not isinstance(bids, list) or not isinstance(asks, list):
            return
        self.snapshot_received.emit(
            {
                "symbol": str(payload.get("s") or self._symbol).upper(),
                "bids": bids[:20],
                "asks": asks[:20],
                "ts_ms": int(time.time() * 1000),
            }
        )

    def _on_error(self, error: Any) -> None:
        self.status_changed.emit(tr("depth.status.error").format(error=str(error)))

    def _on_close(self, code: Any, msg: Any) -> None:
        self.status_changed.emit(tr("depth.status.closed").format(code=str(code), message=str(msg or "")))


class BinanceDepthWindow(QWidget):
    def __init__(self) -> None:
        super().__init__()
        self._worker = _DepthWorker()
        self._connected = False
        self.setWindowTitle(tr("depth.window_title"))
        self.resize(900, 620)
        self._build_ui()
        self._wire_signals()

    def _build_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(10, 10, 10, 10)
        root.setSpacing(8)

        top = QGridLayout()
        top.setHorizontalSpacing(8)
        top.setVerticalSpacing(6)

        top.addWidget(QLabel(tr("depth.market")), 0, 0)
        self.market_combo = QComboBox()
        self.market_combo.addItem("Binance Spot", "spot")
        self.market_combo.addItem("Binance USD-M Futures", "usdm")
        top.addWidget(self.market_combo, 0, 1)

        top.addWidget(QLabel(tr("depth.symbol")), 0, 2)
        self.symbol_edit = QLineEdit("BTCUSDT")
        self.symbol_edit.setPlaceholderText("BTCUSDT")
        top.addWidget(self.symbol_edit, 0, 3)

        self.connect_btn = QPushButton(tr("depth.connect"))
        top.addWidget(self.connect_btn, 0, 4)

        self.status_label = QLabel(tr("depth.status.idle"))
        self.status_label.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        top.addWidget(self.status_label, 1, 0, 1, 5)
        root.addLayout(top)

        table_wrap = QHBoxLayout()
        table_wrap.setSpacing(8)

        self.table = QTableWidget(20, 8)
        self.table.setHorizontalHeaderLabels(
            [
                tr("depth.bid_qty"),
                tr("depth.bid_price"),
                tr("depth.bid_notional_usdt"),
                tr("depth.bid_delta_pct"),
                tr("depth.ask_price"),
                tr("depth.ask_qty"),
                tr("depth.ask_notional_usdt"),
                tr("depth.ask_delta_pct"),
            ]
        )
        self.table.verticalHeader().setVisible(False)
        self.table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.table.setSelectionMode(QTableWidget.SelectionMode.NoSelection)
        self.table.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        table_wrap.addWidget(self.table)
        root.addLayout(table_wrap)

    def _wire_signals(self) -> None:
        self.connect_btn.clicked.connect(self._on_connect_clicked)
        self._worker.snapshot_received.connect(self._on_snapshot)
        self._worker.status_changed.connect(self._on_status)

    def _on_connect_clicked(self) -> None:
        if self._connected:
            self._worker.stop()
            self._connected = False
            self.connect_btn.setText(tr("depth.connect"))
            self.status_label.setText(tr("depth.status.idle"))
            return
        market = str(self.market_combo.currentData() or "spot")
        symbol = str(self.symbol_edit.text() or "").strip().upper()
        if not symbol:
            self.status_label.setText(tr("depth.status.invalid_symbol"))
            return
        self._clear_table()
        self._worker.start(market=market, symbol=symbol)
        self._connected = True
        self.connect_btn.setText(tr("depth.disconnect"))

    def _on_status(self, text: str) -> None:
        self.status_label.setText(text)

    def _on_snapshot(self, data: dict) -> None:
        bids = data.get("bids", [])
        asks = data.get("asks", [])
        best_bid_price = self._to_decimal(bids[0][0]) if isinstance(bids, list) and bids else None
        best_ask_price = self._to_decimal(asks[0][0]) if isinstance(asks, list) and asks else None
        for row in range(20):
            bid_row = bids[row] if row < len(bids) else None
            ask_row = asks[row] if row < len(asks) else None
            bid_price = str(bid_row[0]) if isinstance(bid_row, list) and len(bid_row) >= 2 else ""
            bid_qty = str(bid_row[1]) if isinstance(bid_row, list) and len(bid_row) >= 2 else ""
            ask_price = str(ask_row[0]) if isinstance(ask_row, list) and len(ask_row) >= 2 else ""
            ask_qty = str(ask_row[1]) if isinstance(ask_row, list) and len(ask_row) >= 2 else ""
            self._set_cell(row, 0, bid_qty)
            self._set_cell(row, 1, bid_price)
            self._set_cell(row, 2, self._level_notional_usdt(price_text=bid_price, qty_text=bid_qty))
            self._set_cell(row, 3, self._level_delta_pct(level_price=bid_price, best_price=best_bid_price))
            self._set_cell(row, 4, ask_price)
            self._set_cell(row, 5, ask_qty)
            self._set_cell(row, 6, self._level_notional_usdt(price_text=ask_price, qty_text=ask_qty))
            self._set_cell(row, 7, self._level_delta_pct(level_price=ask_price, best_price=best_ask_price))
        symbol = str(data.get("symbol") or "").upper()
        self.status_label.setText(tr("depth.status.streaming").format(symbol=symbol or "N/A"))

    def _set_cell(self, row: int, col: int, value: str) -> None:
        item = self.table.item(row, col)
        if item is None:
            item = QTableWidgetItem()
            if col in {1, 2, 3, 0}:
                item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
            self.table.setItem(row, col, item)
        item.setText(value)

    def _clear_table(self) -> None:
        for row in range(20):
            for col in range(8):
                self._set_cell(row, col, "")

    @staticmethod
    def _to_decimal(value: object) -> Decimal | None:
        try:
            return Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError):
            return None

    def _level_delta_pct(self, *, level_price: str, best_price: Decimal | None) -> str:
        if best_price is None or best_price <= 0:
            return ""
        level = self._to_decimal(level_price)
        if level is None:
            return ""
        delta_pct = ((level - best_price) / best_price) * Decimal("100")
        return f"{delta_pct:.4f}%"

    def _level_notional_usdt(self, *, price_text: str, qty_text: str) -> str:
        price = self._to_decimal(price_text)
        qty = self._to_decimal(qty_text)
        if price is None or qty is None:
            return ""
        notional = price * qty
        return f"{notional:.4f}"

    def closeEvent(self, event) -> None:
        self._worker.stop()
        self._connected = False
        super().closeEvent(event)
