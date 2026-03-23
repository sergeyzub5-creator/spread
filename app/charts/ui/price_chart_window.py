from __future__ import annotations

from PySide6.QtCore import QPoint, QSize, Qt, Signal, QTimer
from PySide6.QtGui import QFont
from PySide6.QtWidgets import (
    QApplication,
    QFrame,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMenu,
    QPushButton,
    QHeaderView,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
    QSizePolicy,
)

from app.charts.exchanges import available_chart_market_types
from app.charts.exchanges.catalog import get_chart_exchange_meta
from app.charts.market_types import CHART_INSTRUMENT_TYPE_LABELS, ChartInstrumentType, chart_market_type_menu_items
from app.charts.services import ChartLiveRefreshWorker
from app.charts.ui.helpers import (
    CHART_EXCHANGE_MENU_ITEMS,
    build_local_exchange_icon,
    format_volume_threshold,
    parse_daily_volume_threshold,
)
from app.charts.ui.market_table import ChartMarketTable
from app.charts.ui.price_chart_widget import PriceChartWidget
from app.charts.ui.price_chart_window_logic import PriceChartWindowLogicMixin


class PriceChartWindow(QWidget, PriceChartWindowLogicMixin):
    instruments_loaded = Signal(int, object, str)
    price_updates_loaded = Signal(str, object, str)
    funding_updates_loaded = Signal(str, str, object, str)
    spread_history_loaded = Signal(int, str, object, str)

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self.setObjectName("priceChartWindow")
        self.setWindowTitle("Price Chart")
        self.resize(1500, 760)
        self._daily_volume_threshold: int | None = parse_daily_volume_threshold("10M")
        self._slot_state = {
            "left": {"exchange": "binance", "market_type": ChartInstrumentType.PERPETUAL.value, "symbol": None},
            "right": {"exchange": "bybit", "market_type": ChartInstrumentType.PERPETUAL.value, "symbol": None},
        }
        self._exchange_buttons: dict[str, QPushButton] = {}
        self._market_type_buttons: dict[str, QPushButton] = {}
        self._cached_symbols_by_selection: dict[str, list[str]] = {}
        self._bookmark_order_by_selection: dict[str, list[str]] = {}
        self._live_rows_by_selection: dict[str, list[dict[str, str | int | None]]] = {}
        self._visible_row_symbols: list[str] = []
        self._row_index_by_symbol: dict[str, int] = {}
        self._display_rows: list[dict[str, str | int | None]] = []
        self._rendered_data_rows: set[int] = set()
        self._lazy_render_buffer = 20
        self._hover_row_index = -1
        self._selected_row_symbol: str | None = None
        self._selected_history_source: dict[str, str] | None = None
        self._bookmark_drag_symbol: str | None = None
        self._bookmark_drag_row: int = -1
        self._bookmark_drag_start_pos: QPoint | None = None
        self._bookmark_drag_active = False
        self._sort_buttons: dict[str, QPushButton] = {}
        self._header_row_index = 0
        self._load_revision = 0
        self._sort_mode = "spread"
        self._sort_descending = True
        self._pending_reorder_rows: list[dict[str, str | int | None]] | None = None
        self._last_background_priority_signature: tuple[str, str, str, str, tuple[str, ...]] | None = None
        self._history_load_revision = 0
        self._history_prepend_revision = 0
        self._history_limit = 5000
        self._history_session_cache: dict[tuple[str, str, str, str, str, str, int], list[object]] = {}
        self._history_session_cache_ts_ms: dict[tuple[str, str, str, str, str, str, int], int] = {}
        self._history_pending_before_ms: dict[tuple[str, str, str, str, str, str, int], int] = {}
        self._build_ui()
        self._apply_theme()
        app = QApplication.instance()
        if app is not None:
            app.installEventFilter(self)
        self.instruments_loaded.connect(self._on_instruments_loaded)
        self.price_updates_loaded.connect(self._on_price_updates_loaded)
        self.funding_updates_loaded.connect(self._on_funding_updates_loaded)
        self.spread_history_loaded.connect(self._on_spread_history_loaded)
        self.chart_widget.olderHistoryRequested.connect(self._request_older_spread_history)
        self.refresh_button.clicked.connect(self._refresh_instruments)
        self._countdown_timer = QTimer(self)
        self._countdown_timer.setInterval(1000)
        self._countdown_timer.timeout.connect(self._tick_funding_timers)
        self._countdown_timer.start()
        self._reorder_timer = QTimer(self)
        self._reorder_timer.setSingleShot(True)
        self._reorder_timer.setInterval(350)
        self._reorder_timer.timeout.connect(self._apply_pending_reorder)
        self._live_refresh_worker = ChartLiveRefreshWorker(
            on_price=lambda cache_key, updates, error: self.price_updates_loaded.emit(cache_key, updates, error),
            on_funding=lambda cache_key, side, updates, error: self.funding_updates_loaded.emit(cache_key, side, updates, error),
        )
        self._live_refresh_worker.start()
        self._restore_settings()
        self._apply_background_config()
        self._render_cached_instruments()

    def closeEvent(self, event) -> None:
        app = QApplication.instance()
        if app is not None:
            app.removeEventFilter(self)
        self._live_refresh_worker.stop()
        super().closeEvent(event)

    def resizeEvent(self, event) -> None:
        self._sync_sidebar_constraints()
        super().resizeEvent(event)
        self._render_visible_rows_window()

    def eventFilter(self, watched, event):
        if watched is self.market_table.viewport():
            if event.type() == event.Type.MouseButtonPress:
                row_index = self.market_table.rowAt(event.pos().y())
                column_index = self.market_table.columnAt(event.pos().x())
                symbol = self._row_symbol(row_index)
                self._bookmark_drag_start_pos = event.pos()
                self._bookmark_drag_active = False
                if row_index > self._header_row_index and column_index != 0 and self._is_bookmarked(symbol):
                    self._bookmark_drag_symbol = symbol
                    self._bookmark_drag_row = row_index
                else:
                    self._bookmark_drag_symbol = None
                    self._bookmark_drag_row = -1
            elif event.type() == event.Type.MouseMove:
                row_index = self.market_table.rowAt(event.pos().y())
                if row_index == self._header_row_index:
                    row_index = -1
                if row_index != self._hover_row_index:
                    previous = self._hover_row_index
                    self._hover_row_index = row_index
                    if previous >= 0:
                        self._apply_row_visual_state(previous)
                    if row_index >= 0:
                        self._apply_row_visual_state(row_index)
                if (
                    self._bookmark_drag_symbol
                    and self._bookmark_drag_start_pos is not None
                    and (event.pos() - self._bookmark_drag_start_pos).manhattanLength() >= QApplication.startDragDistance()
                ):
                    self._bookmark_drag_active = True
                    self.market_table.viewport().setCursor(Qt.CursorShape.ClosedHandCursor)
            elif event.type() == event.Type.MouseButtonRelease:
                if self._bookmark_drag_active and self._bookmark_drag_symbol:
                    target_row = self.market_table.rowAt(event.pos().y())
                    self._move_bookmark_symbol(self._bookmark_drag_symbol, target_row)
                self._bookmark_drag_symbol = None
                self._bookmark_drag_row = -1
                self._bookmark_drag_start_pos = None
                self._bookmark_drag_active = False
                self.market_table.viewport().setCursor(Qt.CursorShape.ArrowCursor)
            elif event.type() == event.Type.Leave:
                if self._hover_row_index >= 0:
                    previous = self._hover_row_index
                    self._hover_row_index = -1
                    self._apply_row_visual_state(previous)
        return super().eventFilter(watched, event)

    def _build_ui(self) -> None:
        root = QHBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        splitter = QSplitter(Qt.Orientation.Horizontal, self)
        splitter.setObjectName("chartRootSplitter")
        splitter.setHandleWidth(6)
        splitter.setChildrenCollapsible(False)
        root.addWidget(splitter, 1)

        left_shell = QFrame()
        left_shell.setObjectName("chartMainShell")
        left_layout = QVBoxLayout(left_shell)
        left_layout.setContentsMargins(0, 0, 0, 0)
        left_layout.setSpacing(0)
        left_layout.addWidget(self._build_chart_body(), 1)
        splitter.addWidget(left_shell)

        self.sidebar = self._build_sidebar()
        self.sidebar.setMinimumWidth(320)
        splitter.addWidget(self.sidebar)
        splitter.setStretchFactor(0, 1)
        splitter.setStretchFactor(1, 0)
        splitter.setSizes([1050, 450])
        self._splitter = splitter
        self._sync_sidebar_constraints()

    def _sync_sidebar_constraints(self) -> None:
        if not hasattr(self, "sidebar"):
            return
        max_sidebar_width = max(420, self.width() // 2)
        self.sidebar.setMaximumWidth(max_sidebar_width)

    def _build_chart_body(self) -> QWidget:
        frame = QFrame()
        frame.setObjectName("chartBody")
        layout = QHBoxLayout(frame)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        self.chart_widget = PriceChartWidget(self)
        layout.addWidget(self.chart_widget, 1)
        return frame

    def _build_sidebar(self) -> QWidget:
        frame = QFrame()
        frame.setObjectName("chartSidebar")
        frame.setFixedWidth(300)
        layout = QVBoxLayout(frame)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        top = QFrame()
        top.setObjectName("chartSidebarTop")
        top_layout = QHBoxLayout(top)
        top_layout.setContentsMargins(8, 8, 8, 8)
        top_layout.setSpacing(6)
        top_layout.addWidget(self._build_exchange_capsule("left", "Binance", "binance"), 1)
        top_layout.addWidget(self._build_exchange_capsule("right", "Bybit", "bybit"), 1)
        layout.addWidget(top, 0)

        selectors = QFrame()
        selectors.setObjectName("chartSelectorsRow")
        selectors_layout = QHBoxLayout(selectors)
        selectors_layout.setContentsMargins(8, 8, 8, 6)
        selectors_layout.setSpacing(6)
        selectors_layout.addWidget(self._build_selector_column("left"), 1)
        selectors_layout.addWidget(self._build_selector_column("right"), 1)
        layout.addWidget(selectors, 0)

        volume_row = QFrame()
        volume_row.setObjectName("chartVolumeRow")
        volume_layout = QHBoxLayout(volume_row)
        volume_layout.setContentsMargins(8, 6, 8, 4)
        volume_layout.setSpacing(4)

        volume_label = QFrame()
        volume_label.setObjectName("chartVolumeLabelCapsule")
        volume_label_layout = QHBoxLayout(volume_label)
        volume_label_layout.setContentsMargins(10, 4, 10, 4)
        volume_label_layout.setSpacing(0)
        volume_label_text = QLabel("Фильтр объёма")
        volume_label_text.setObjectName("chartVolumeLabelText")
        volume_label_layout.addWidget(volume_label_text, 0)
        volume_layout.addWidget(volume_label, 0)

        self.volume_edit = QLineEdit()
        self.volume_edit.setObjectName("chartVolumeEdit")
        self.volume_edit.setPlaceholderText("10M")
        self.volume_edit.setText(format_volume_threshold(self._daily_volume_threshold))
        self.volume_edit.setFixedWidth(64)
        volume_layout.addWidget(self.volume_edit, 0)

        self.refresh_button = QPushButton("Обновить")
        self.refresh_button.setObjectName("chartRefreshButton")
        volume_layout.addWidget(self.refresh_button, 0)
        self.loaded_count_label = QLabel("Загружено пар: 0")
        self.loaded_count_label.setObjectName("chartLoadedCountLabel")
        volume_layout.addWidget(self.loaded_count_label, 0)
        volume_layout.addStretch(1)
        layout.addWidget(volume_row, 0)

        list_wrap = QFrame()
        list_wrap.setObjectName("chartInstrumentWrap")
        list_layout = QVBoxLayout(list_wrap)
        list_layout.setContentsMargins(0, 8, 0, 0)
        list_layout.setSpacing(6)

        self.market_table = ChartMarketTable(0, 5)
        self.market_table.setObjectName("chartMarketTable")
        self.market_table.setHorizontalHeaderLabels(["", "Ставка", "Инструмент", "Спред %", "Ставка"])
        self.market_table.horizontalHeader().setVisible(False)
        self.market_table.verticalHeader().setVisible(False)
        self.market_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.market_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.market_table.setSelectionMode(QTableWidget.SelectionMode.NoSelection)
        self.market_table.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        self.market_table.setMouseTracking(True)
        self.market_table.viewport().setMouseTracking(True)
        self.market_table.viewport().installEventFilter(self)
        self.market_table.viewport().setCursor(Qt.CursorShape.ArrowCursor)
        self.market_table.cellPressed.connect(self._on_table_cell_pressed)
        self.market_table.verticalScrollBar().valueChanged.connect(lambda _value: self._render_visible_rows_window())
        list_layout.addWidget(self.market_table, 1)
        layout.addWidget(list_wrap, 1)
        return frame

    def _build_selector_column(self, slot_name: str) -> QWidget:
        column = QWidget()
        column.setObjectName("chartSelectorColumn")
        column_layout = QVBoxLayout(column)
        column_layout.setContentsMargins(0, 0, 0, 0)
        column_layout.setSpacing(6)

        type_button = QPushButton(CHART_INSTRUMENT_TYPE_LABELS[ChartInstrumentType.PERPETUAL])
        type_button.setObjectName("chartTypeSelector")
        type_button.setCursor(Qt.CursorShape.ArrowCursor)
        type_button.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        type_button.clicked.connect(lambda _checked=False, slot=slot_name: self._open_market_type_menu(slot))
        self._market_type_buttons[slot_name] = type_button
        column_layout.addWidget(type_button, 0)
        return column

    def _build_exchange_capsule(self, slot_name: str, title: str, exchange_code: str) -> QWidget:
        button = QPushButton(title)
        button.setObjectName("chartExchangeCapsule")
        button.setProperty("selected", True)
        button.setCursor(Qt.CursorShape.ArrowCursor)
        button.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        button.setIcon(build_local_exchange_icon(exchange_code, size=15))
        button.setIconSize(QSize(15, 15))
        button.clicked.connect(lambda _checked=False, slot=slot_name: self._open_exchange_menu(slot))
        self._exchange_buttons[slot_name] = button
        return button

    def _build_sort_capsule(self, sort_mode: str, title: str) -> QWidget:
        button = QPushButton(title)
        button.setObjectName("chartSortCapsule")
        button.setMinimumWidth(0)
        button.setFixedHeight(24)
        if sort_mode in ("left_funding", "spread", "right_funding"):
            button.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
            button.setCursor(Qt.CursorShape.ArrowCursor)
            button.clicked.connect(lambda _checked=False, mode=sort_mode: self._set_sort(mode))
        else:
            button.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
            button.setCursor(Qt.CursorShape.ArrowCursor)
            button.setEnabled(False)
        button.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        self._sort_buttons[sort_mode] = button
        return button

    def _install_sort_capsule_row(self) -> None:
        self.market_table.setRowCount(max(1, self.market_table.rowCount()))
        self.market_table.setRowHeight(self._header_row_index, 34)
        self._sort_buttons = {}
        for column_index in range(self.market_table.columnCount()):
            self.market_table.removeCellWidget(self._header_row_index, column_index)
            item = self._table_item(self._header_row_index, column_index)
            item.setText("")
            item.setFlags(Qt.ItemFlag.ItemIsEnabled)
        cells = (
            (1, "left_funding", "Ставка"),
            (2, "instrument", "Инструмент"),
            (3, "spread", "Спред %"),
            (4, "right_funding", "Ставка"),
        )
        for column_index, sort_mode, title in cells:
            button = self._build_sort_capsule(sort_mode, title)
            self.market_table.setCellWidget(self._header_row_index, column_index, button)

    def _build_menu(self) -> QMenu:
        menu = QMenu(self)
        menu.setStyleSheet(
            """
            QMenu {
                background: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                    stop: 0 rgba(27, 30, 37, 250),
                    stop: 1 rgba(17, 18, 20, 252));
                color: #eef2f8;
                border: 1px solid rgba(78, 124, 255, 120);
                border-radius: 10px;
                padding: 5px;
                font-size: 11px;
                font-weight: 700;
            }
            QMenu::item {
                padding: 7px 10px;
                border-radius: 7px;
                margin: 2px 0;
            }
            QMenu::item:selected {
                background-color: rgba(78, 124, 255, 42);
                border: 1px solid rgba(78, 124, 255, 120);
            }
            """
        )
        return menu

    def _open_exchange_menu(self, slot_name: str) -> None:
        button = self._exchange_buttons[slot_name]
        menu = self._build_menu()
        for exchange_code, title in CHART_EXCHANGE_MENU_ITEMS:
            action = menu.addAction(title)
            action.setIcon(build_local_exchange_icon(exchange_code, size=18))
            action.triggered.connect(
                lambda _checked=False, slot=slot_name, selected=exchange_code, selected_title=title: self._set_exchange(
                    slot, selected, selected_title
                )
            )
        menu.setFixedWidth(button.width())
        menu.exec(button.mapToGlobal(QPoint(0, button.height())))

    def _set_exchange(self, slot_name: str, exchange_code: str, title: str) -> None:
        self._slot_state[slot_name]["exchange"] = exchange_code
        button = self._exchange_buttons[slot_name]
        button.setText(title)
        button.setIcon(build_local_exchange_icon(exchange_code, size=15))
        button.setIconSize(QSize(15, 15))
        allowed_types = available_chart_market_types(exchange_code)
        current_market_type = str(self._slot_state[slot_name]["market_type"] or "")
        if current_market_type not in allowed_types and allowed_types:
            fallback = allowed_types[0]
            self._slot_state[slot_name]["market_type"] = fallback
            self._market_type_buttons[slot_name].setText(CHART_INSTRUMENT_TYPE_LABELS[ChartInstrumentType(fallback)])
        self._selected_row_symbol = None
        self._selected_history_source = None
        self.chart_widget.set_status_text("Выберите инструмент")
        self._persist_settings()
        self._apply_background_config()
        self._render_cached_instruments()

    def _open_market_type_menu(self, slot_name: str) -> None:
        button = self._market_type_buttons[slot_name]
        menu = self._build_menu()
        allowed_types = set(available_chart_market_types(self._slot_state[slot_name]["exchange"]))
        items = [(value, title) for value, title in chart_market_type_menu_items() if not allowed_types or value in allowed_types]
        for value, title in items:
            action = menu.addAction(title)
            action.triggered.connect(
                lambda _checked=False, slot=slot_name, selected=value, selected_title=title: self._set_market_type(
                    slot, selected, selected_title
                )
            )
        menu.setFixedWidth(button.width())
        menu.exec(button.mapToGlobal(QPoint(0, button.height())))

    def _set_market_type(self, slot_name: str, market_type: str, title: str) -> None:
        self._slot_state[slot_name]["market_type"] = market_type
        self._market_type_buttons[slot_name].setText(title)
        self._selected_row_symbol = None
        self._selected_history_source = None
        self.chart_widget.set_status_text("Выберите инструмент")
        self._persist_settings()
        self._apply_background_config()
        self._render_cached_instruments()

    def _apply_theme(self) -> None:
        self.setStyleSheet(
            """
            QWidget#priceChartWindow {
                background-color: #0b0b0c;
                color: #e7eaef;
            }
            QSplitter#chartRootSplitter::handle {
                background-color: #1a1d22;
                border-left: 1px solid #2b3038;
                border-right: 1px solid #070708;
            }
            QFrame#chartMainShell,
            QFrame#chartBody {
                background-color: #0b0b0c;
            }
            QFrame#chartSidebar {
                background-color: #111214;
                border-left: 1px solid #343a45;
            }
            QFrame#chartSidebarTop,
            QFrame#chartSelectorsRow {
                background-color: #111214;
            }
            QFrame#chartVolumeRow,
            QFrame#chartInstrumentWrap {
                background-color: #111214;
            }
            QFrame#chartVolumeLabelCapsule {
                background-color: #151922;
                border: 1px solid #2d3442;
                border-radius: 6px;
            }
            QLabel#chartVolumeLabelText {
                color: #dfe5ef;
                font-size: 9px;
                font-weight: 600;
            }
            QPushButton#chartExchangeCapsule {
                min-height: 34px;
                background-color: #151922;
                color: #dfe5ef;
                border: 1px solid #2d3442;
                border-radius: 17px;
                padding: 0 8px;
                font-size: 11px;
                font-weight: 600;
                text-align: center;
            }
            QPushButton#chartExchangeCapsule[selected="true"] {
                background-color: #1c2740;
                border: 1px solid #4e7cff;
            }
            QPushButton#chartExchangeCapsule:hover {
                background-color: #1b2130;
                border-color: #5b84ff;
            }
            QPushButton#chartTypeSelector {
                min-height: 30px;
                background-color: #151922;
                color: #eef2f8;
                border: 1px solid #2d3442;
                border-radius: 10px;
                padding: 0 8px;
                font-size: 11px;
                font-weight: 700;
                text-align: left;
            }
            QPushButton#chartTypeSelector:hover {
                background-color: #1b2130;
                border-color: #4e7cff;
            }
            QPushButton#chartSortCapsule {
                min-height: 24px;
                background-color: #151922;
                color: #8b91a1;
                border: 1px solid #2d3442;
                border-radius: 12px;
                padding: 0 7px;
                font-size: 9px;
                font-weight: 600;
                text-align: center;
            }
            QPushButton#chartSortCapsule:hover {
                background-color: #1b2130;
                border-color: #5b84ff;
                color: #dfe5ef;
            }
            QPushButton#chartSortCapsule[selected="true"] {
                background-color: #1c2740;
                border: 1px solid #4e7cff;
                color: #eef2f8;
            }
            QPushButton#chartSortCapsule:disabled {
                color: #a7afbd;
                background-color: #151922;
                border: 1px solid #2d3442;
            }
            QLineEdit#chartVolumeEdit {
                min-height: 26px;
                padding: 0 6px;
                background-color: #12151b;
                color: #eef2f8;
                border: 1px solid #313744;
                border-radius: 6px;
                selection-background-color: #2950b8;
                font-size: 10px;
            }
            QPushButton#chartRefreshButton {
                min-height: 26px;
                padding: 0 9px;
                background-color: #2d4ea0;
                color: #f7f9fd;
                border: 1px solid #5d82da;
                border-radius: 13px;
                font-size: 9px;
                font-weight: 700;
            }
            QPushButton#chartRefreshButton:hover {
                background-color: #365dbd;
            }
            QPushButton#chartRefreshButton:pressed {
                background-color: #27458f;
            }
            QLabel#chartLoadedCountLabel {
                color: #aeb7c5;
                font-size: 9px;
                font-weight: 600;
            }
            QTableWidget#chartMarketTable {
                background-color: #111214;
                color: #d5dae2;
                border: none;
                gridline-color: #23272f;
            }
            QTableWidget#chartMarketTable::item {
                padding: 6px 8px;
                font-size: 10px;
                background-color: transparent;
                border: none;
            }
            QTableWidget#chartMarketTable::item:hover {
                background-color: transparent;
                border: none;
            }
            QTableWidget#chartMarketTable::item:selected {
                background-color: transparent;
                border: none;
            }
            QFrame#chartFundingCell {
                background-color: transparent;
            }
            QFrame#chartBookmarkCell {
                background-color: transparent;
            }
            QFrame#chartInstrumentCell {
                background-color: transparent;
            }
            QFrame#chartSpreadCell {
                background-color: transparent;
            }
            QLabel#chartBookmarkStarLabel {
                color: #697181;
                font-size: 11px;
                font-weight: 700;
            }
            QLabel#chartInstrumentSymbolLabel {
                color: #d5dae2;
                font-size: 10px;
                font-weight: 700;
            }
            QLabel#chartInstrumentSpreadLabel {
                color: #8b91a1;
                font-size: 9px;
                font-weight: 600;
            }
            QLabel#chartFundingRateLabel {
                color: #d5dae2;
                font-size: 10px;
                font-weight: 700;
            }
            QLabel#chartFundingTimerLabel {
                color: #8b91a1;
                font-size: 9px;
                font-weight: 600;
            }
            QHeaderView::section {
                background-color: #111214;
                color: #8b91a1;
                border: none;
                border-bottom: 1px solid #2a2e36;
                padding: 8px 8px;
                font-size: 11px;
                font-weight: 600;
            }
            """
        )

        title_font = QFont("Segoe UI")
        title_font.setPointSize(12)
        title_font.setBold(True)
        self.setFont(title_font)

        self.market_table.setColumnWidth(0, 28)
        self.market_table.setColumnWidth(1, 96)
        self.market_table.setColumnWidth(2, 132)
        self.market_table.setColumnWidth(3, 90)
        self.market_table.setColumnWidth(4, 96)
        self.market_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Fixed)
        self.market_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Fixed)
        self.market_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
        self.market_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeMode.Fixed)
        self.market_table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeMode.Fixed)
        self._install_sort_capsule_row()
        self._refresh_sort_headers()

