from __future__ import annotations

import threading
from decimal import Decimal

from PySide6.QtCore import QObject, Signal

from app.core.accounts.binance_account_connector import BinanceAccountConnector
from app.core.accounts.bitget_account_connector import BitgetAccountConnector
from app.core.accounts.bybit_account_connector import BybitAccountConnector
from app.core.instruments.bitget_linear_loader import BitgetLinearInstrumentLoader
from app.core.instruments.bitget_spot_loader import BitgetSpotInstrumentLoader
from app.core.events.bus import EventBus
from app.core.instruments.binance_spot_loader import BinanceSpotInstrumentLoader
from app.core.instruments.binance_usdm_loader import BinanceUsdmInstrumentLoader
from app.core.instruments.bybit_linear_loader import BybitLinearInstrumentLoader
from app.core.instruments.bybit_spot_loader import BybitSpotInstrumentLoader
from app.core.instruments.market_cap_ranker import MarketCapRanker
from app.core.instruments.registry import InstrumentRegistry
from app.core.logging.logger_factory import get_logger
from app.core.market_data.bitget_linear_connector import BitgetLinearPublicConnector
from app.core.market_data.bitget_linear_normalizer import BitgetLinearQuoteNormalizer
from app.core.market_data.bitget_spot_connector import BitgetSpotPublicConnector
from app.core.market_data.bitget_spot_normalizer import BitgetSpotQuoteNormalizer
from app.core.market_data.binance_spot_connector import BinanceSpotPublicConnector
from app.core.market_data.binance_spot_normalizer import BinanceSpotQuoteNormalizer
from app.core.market_data.binance_usdm_connector import BinanceUsdmPublicConnector
from app.core.market_data.binance_usdm_normalizer import BinanceUsdmQuoteNormalizer
from app.core.market_data.bybit_linear_connector import BybitLinearPublicConnector
from app.core.market_data.bybit_linear_normalizer import BybitLinearQuoteNormalizer
from app.core.market_data.bybit_spot_connector import BybitSpotPublicConnector
from app.core.market_data.bybit_spot_normalizer import BybitSpotQuoteNormalizer
from app.core.market_data.service import MarketDataService
from app.core.models.account import ExchangeCredentials
from app.core.models.market_data import QuoteL1
from app.core.models.instrument_types import UI_INSTRUMENT_TYPE_LABELS, UiInstrumentType
from app.core.models.workers import WorkerEvent, WorkerState, WorkerTask
from app.core.workers.manager import WorkerManager


class _RestAccountMonitor:
    def __init__(
        self,
        credentials: ExchangeCredentials,
        connector,
        *,
        poll_interval_seconds: float = 1.0,
    ) -> None:
        self._credentials = credentials
        self._connector = connector
        self._logger = get_logger("accounts.rest_monitor")
        self._poll_interval_seconds = float(poll_interval_seconds)
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._refresh_in_flight = False
        self._lock = threading.Lock()

    def start(self, on_snapshot, on_error) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop_event.clear()
            self._thread = threading.Thread(
                target=self._run_loop,
                args=(on_snapshot, on_error),
                name=f"{self._credentials.exchange}-account-monitor-poll",
                daemon=True,
            )
            self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()

    def _run_loop(self, on_snapshot, on_error) -> None:
        self._logger.info(
            "%s account rest monitor loop started | interval_seconds=%s",
            self._credentials.exchange,
            self._poll_interval_seconds,
        )
        while not self._stop_event.is_set():
            self._trigger_refresh(on_snapshot, on_error)
            if self._stop_event.wait(self._poll_interval_seconds):
                break
        self._logger.info("%s account rest monitor loop stopped", self._credentials.exchange)

    def _trigger_refresh(self, on_snapshot, on_error) -> None:
        with self._lock:
            if self._stop_event.is_set() or self._refresh_in_flight:
                return
            self._refresh_in_flight = True

        try:
            snapshot = self._connector.connect(self._credentials)
            on_snapshot(snapshot)
        except Exception as exc:
            self._logger.warning("%s account monitor refresh failed: %s", self._credentials.exchange, exc)
            on_error(str(exc))
        finally:
            with self._lock:
                self._refresh_in_flight = False


class UiCoordinator(QObject):
    """Thin bridge between GUI widgets and backend services."""

    public_quote_received = Signal(str, object)
    public_quote_error = Signal(str)
    instruments_loaded = Signal(str, str)
    instruments_load_failed = Signal(str, str, str)
    exchange_connect_started = Signal(str, str)
    exchange_connect_succeeded = Signal(str, str, object)
    exchange_connect_failed = Signal(str, str, str)
    exchange_snapshot_updated = Signal(str, str, object)
    exchange_snapshot_update_failed = Signal(str, str, str)
    exchange_close_positions_started = Signal(str, str)
    exchange_close_positions_succeeded = Signal(str, str, object)
    exchange_close_positions_failed = Signal(str, str, str)
    worker_state_updated = Signal(str, object)
    worker_event_received = Signal(str, object)
    worker_command_failed = Signal(str, str)

    def __init__(
        self,
        instrument_registry: InstrumentRegistry,
        market_data_service: MarketDataService,
        worker_manager: WorkerManager,
        event_bus: EventBus,
    ) -> None:
        super().__init__()
        self.instrument_registry = instrument_registry
        self.market_data_service = market_data_service
        self.worker_manager = worker_manager
        self.event_bus = event_bus
        self._binance_spot_loader = BinanceSpotInstrumentLoader()
        self._binance_loader = BinanceUsdmInstrumentLoader()
        self._bitget_linear_loader = BitgetLinearInstrumentLoader()
        self._bitget_spot_loader = BitgetSpotInstrumentLoader()
        self._bybit_spot_loader = BybitSpotInstrumentLoader()
        self._bybit_linear_loader = BybitLinearInstrumentLoader()
        self._market_cap_ranker = MarketCapRanker()
        self._binance_account_connector = BinanceAccountConnector()
        self._bitget_account_connector = BitgetAccountConnector()
        self._bybit_account_connector = BybitAccountConnector()
        self._binance_spot_loaded = False
        self._binance_perp_loaded = False
        self._bitget_perp_loaded = False
        self._bitget_spot_loaded = False
        self._bybit_spot_loaded = False
        self._bybit_perp_loaded = False
        self._loading_market_types: set[tuple[str, str]] = set()
        self._load_lock = threading.Lock()
        self._subscriptions: dict[str, tuple[object, object]] = {}
        self._account_monitors: dict[str, _RestAccountMonitor] = {}
        self._shutdown = False
        self._logger = get_logger("ui.coordinator")
        self.event_bus.subscribe("worker_state", self._on_worker_state)
        self.event_bus.subscribe("worker_events", self._on_worker_event)

    def bootstrap(self) -> None:
        self.market_data_service.register_exchange_transport(
            transport_key="binance:spot",
            connector=BinanceSpotPublicConnector(),
            normalizer=BinanceSpotQuoteNormalizer(),
        )
        self.market_data_service.register_exchange_transport(
            transport_key="binance:linear_perp",
            connector=BinanceUsdmPublicConnector(),
            normalizer=BinanceUsdmQuoteNormalizer(),
        )
        self.market_data_service.register_exchange_transport(
            transport_key="bitget:spot",
            connector=BitgetSpotPublicConnector(),
            normalizer=BitgetSpotQuoteNormalizer(),
        )
        self.market_data_service.register_exchange_transport(
            transport_key="bitget:linear_perp",
            connector=BitgetLinearPublicConnector(),
            normalizer=BitgetLinearQuoteNormalizer(),
        )
        self.market_data_service.register_exchange_transport(
            transport_key="bybit:linear_perp",
            connector=BybitLinearPublicConnector(),
            normalizer=BybitLinearQuoteNormalizer(),
        )
        self.market_data_service.register_exchange_transport(
            transport_key="bybit:spot",
            connector=BybitSpotPublicConnector(),
            normalizer=BybitSpotQuoteNormalizer(),
        )

    def available_quote_exchanges(self) -> list[tuple[str, str]]:
        return [("binance", "Binance"), ("bitget", "Bitget"), ("bybit", "Bybit")]

    def available_account_exchanges(self) -> list[tuple[str, str]]:
        return [("binance", "Binance"), ("bitget", "Bitget"), ("bybit", "Bybit")]

    def available_execution_exchanges(self) -> list[tuple[str, str]]:
        return [("binance", "Binance"), ("bitget", "Bitget"), ("bybit", "Bybit")]

    def available_execution_routes(
        self,
        exchange: str,
        account_profile: dict | None = None,
    ) -> list[tuple[str, str]]:
        normalized_exchange = str(exchange or "").strip().lower()
        profile = dict(account_profile or {})
        if normalized_exchange == "bitget":
            routes = [("bitget_linear_rest_probe", "REST"), ("bitget_linear_trade_ws", "WS")]
            preferred = str(profile.get("selected_execution_route") or profile.get("preferred_execution_route") or "").strip()
            if preferred == "bitget_linear_trade_ws":
                return [("bitget_linear_trade_ws", "WS"), ("bitget_linear_rest_probe", "REST")]
            return routes
        if normalized_exchange == "binance":
            return [("binance_usdm_trade_ws", "WS")]
        if normalized_exchange == "bybit":
            return [("bybit_linear_trade_ws", "WS")]
        return []

    def available_exchanges(self) -> list[tuple[str, str]]:
        return self.available_account_exchanges()

    def list_market_types(self, exchange: str) -> list[tuple[str, str]]:
        if exchange == "binance":
            return [
                (UiInstrumentType.SPOT.value, UI_INSTRUMENT_TYPE_LABELS[UiInstrumentType.SPOT]),
                (UiInstrumentType.PERPETUAL.value, UI_INSTRUMENT_TYPE_LABELS[UiInstrumentType.PERPETUAL]),
            ]
        if exchange == "bitget":
            return [
                (UiInstrumentType.SPOT.value, UI_INSTRUMENT_TYPE_LABELS[UiInstrumentType.SPOT]),
                (UiInstrumentType.PERPETUAL.value, UI_INSTRUMENT_TYPE_LABELS[UiInstrumentType.PERPETUAL]),
            ]
        if exchange == "bybit":
            return [
                (UiInstrumentType.SPOT.value, UI_INSTRUMENT_TYPE_LABELS[UiInstrumentType.SPOT]),
                (UiInstrumentType.PERPETUAL.value, UI_INSTRUMENT_TYPE_LABELS[UiInstrumentType.PERPETUAL]),
            ]
        return []

    def list_instruments(self, exchange: str, market_type: str) -> list[str]:
        return [item["display"] for item in self.list_instrument_items(exchange, market_type)]

    def list_instrument_items(self, exchange: str, market_type: str) -> list[dict[str, str]]:
        if not self._is_market_type_loaded(str(exchange or "").strip().lower(), str(market_type or "").strip().lower()):
            self._prefetch_market_type(exchange, market_type)
            return []
        instruments = self._market_cap_ranker.rank(self.instrument_registry.list_by_ui_market_type(exchange, market_type))
        return [
            {
                "symbol": instrument.symbol,
                "display": self._display_symbol(instrument),
                "quote_asset": str(instrument.spec.quote_asset or instrument.spec.settle_asset or "").strip().upper(),
            }
            for instrument in instruments
        ]

    def resolve_instrument_symbol(self, exchange: str, market_type: str, value: str) -> str:
        normalized_value = str(value or "").strip().upper()
        if not normalized_value:
            return ""
        for item in self.list_instrument_items(exchange, market_type):
            if str(item.get("symbol", "")).strip().upper() == normalized_value:
                return str(item.get("symbol", "")).strip().upper()
            if str(item.get("display", "")).strip().upper() == normalized_value:
                return str(item.get("symbol", "")).strip().upper()
        return normalized_value

    def display_symbol(self, exchange: str, market_type: str, symbol: str) -> str:
        resolved_symbol = self.resolve_instrument_symbol(exchange, market_type, symbol)
        instrument = self.instrument_registry.find_by_ui_symbol(exchange, symbol=resolved_symbol, ui_market_type=market_type)
        if instrument is None:
            return str(symbol or "").strip().upper()
        return self._display_symbol(instrument)

    def subscribe_public_quote(self, slot_name: str, exchange: str, market_type: str, symbol: str) -> None:
        if not self._is_market_type_loaded(str(exchange or "").strip().lower(), str(market_type or "").strip().lower()):
            self._prefetch_market_type(exchange, market_type)
            self.public_quote_error.emit(f"{exchange} instruments are still loading")
            return

        instrument = self.instrument_registry.find_by_ui_symbol(exchange, symbol=symbol, ui_market_type=market_type)
        if instrument is None:
            self.public_quote_error.emit(f"Instrument not found: {symbol}")
            return

        previous = self._subscriptions.get(slot_name)
        if previous is not None:
            previous_instrument, previous_callback = previous
            self.market_data_service.unsubscribe_l1(previous_instrument, previous_callback)

        def _handle_quote(quote: QuoteL1) -> None:
            self.public_quote_received.emit(slot_name, quote.to_dict())

        self._subscriptions[slot_name] = (instrument, _handle_quote)
        self.market_data_service.subscribe_l1(instrument, _handle_quote)

    def unsubscribe_public_quote(self, slot_name: str) -> None:
        previous = self._subscriptions.pop(slot_name, None)
        if previous is None:
            return
        instrument, callback = previous
        self.market_data_service.unsubscribe_l1(instrument, callback)

    def connect_exchange_async(self, request_id: str, exchange: str, params: dict) -> None:
        exchange_code = str(exchange or "").strip().lower()
        self.exchange_connect_started.emit(request_id, exchange_code)

        def _run() -> None:
            try:
                snapshot = self._connect_exchange(exchange_code, params)
                self.exchange_connect_succeeded.emit(request_id, exchange_code, snapshot.to_dict())
            except Exception as exc:
                self.exchange_connect_failed.emit(request_id, exchange_code, str(exc))

        threading.Thread(
            target=_run,
            name=f"exchange-connect-{exchange_code}",
            daemon=True,
        ).start()

    def close_exchange_positions_async(self, request_id: str, exchange: str, params: dict) -> None:
        exchange_code = str(exchange or "").strip().lower()
        self.exchange_close_positions_started.emit(request_id, exchange_code)

        def _run() -> None:
            try:
                result = self._close_exchange_positions(exchange_code, params)
                self.exchange_close_positions_succeeded.emit(request_id, exchange_code, result.to_dict())
            except Exception as exc:
                self.exchange_close_positions_failed.emit(request_id, exchange_code, str(exc))

        threading.Thread(
            target=_run,
            name=f"exchange-close-positions-{exchange_code}",
            daemon=True,
        ).start()

    def start_exchange_monitor(self, monitor_id: str, exchange: str, params: dict) -> None:
        if self._shutdown:
            return
        exchange_code = str(exchange or "").strip().lower()
        self.stop_exchange_monitor(monitor_id)
        if exchange_code not in {"binance", "bitget", "bybit"}:
            return

        credentials = ExchangeCredentials(
            exchange=exchange_code,
            api_key=str(params.get("api_key", "")),
            api_secret=str(params.get("api_secret", "")),
            api_passphrase=str(params.get("api_passphrase", "")),
        )
        monitor = _RestAccountMonitor(credentials, self._account_connector_for_exchange(exchange_code))
        self._account_monitors[monitor_id] = monitor

        def _on_snapshot(snapshot) -> None:
            if self._shutdown:
                return
            try:
                self.exchange_snapshot_updated.emit(monitor_id, exchange_code, snapshot.to_dict())
            except RuntimeError:
                self.stop_exchange_monitor(monitor_id)

        def _on_error(message: str) -> None:
            if self._shutdown:
                return
            try:
                self.exchange_snapshot_update_failed.emit(monitor_id, exchange_code, message)
            except RuntimeError:
                self.stop_exchange_monitor(monitor_id)

        def _run() -> None:
            try:
                monitor.start(_on_snapshot, _on_error)
                self._logger.info("exchange monitor started | exchange=%s | monitor_id=%s", exchange_code, monitor_id)
            except Exception as exc:
                self._account_monitors.pop(monitor_id, None)
                if self._shutdown:
                    return
                try:
                    self.exchange_snapshot_update_failed.emit(monitor_id, exchange_code, str(exc))
                except RuntimeError:
                    self.stop_exchange_monitor(monitor_id)

        threading.Thread(target=_run, name=f"exchange-monitor-{exchange_code}", daemon=True).start()

    def stop_exchange_monitor(self, monitor_id: str) -> None:
        monitor = self._account_monitors.pop(monitor_id, None)
        if monitor is None:
            return
        monitor.stop()
        self._logger.info("exchange monitor stopped | monitor_id=%s", monitor_id)

    def stop_all_exchange_monitors(self) -> None:
        for monitor_id in list(self._account_monitors.keys()):
            self.stop_exchange_monitor(monitor_id)

    def shutdown(self) -> None:
        self._shutdown = True
        for slot_name in list(self._subscriptions.keys()):
            self.unsubscribe_public_quote(slot_name)
        self.stop_all_exchange_monitors()
        self.worker_manager.shutdown()
        self.market_data_service.shutdown()

    def start_test_runtime_async(
        self,
        *,
        worker_id: str,
        exchange: str,
        market_type: str,
        symbol: str,
        api_key: str,
        api_secret: str,
        api_passphrase: str = "",
        account_profile: dict | None = None,
        target_notional: str = "10",
    ) -> None:
        self._logger.info(
            "ui start test runtime requested | worker_id=%s | exchange=%s | market_type=%s | symbol=%s | target_notional=%s",
            worker_id,
            exchange,
            market_type,
            symbol,
            target_notional,
        )
        def _run() -> None:
            try:
                instrument = self._resolve_instrument(exchange, market_type, symbol)
                task = WorkerTask(
                    worker_id=worker_id,
                    left_instrument=instrument,
                    right_instrument=instrument,
                    entry_threshold=Decimal("0"),
                    exit_threshold=Decimal("0"),
                    target_notional=Decimal(str(target_notional or "10")),
                    step_notional=Decimal(str(target_notional or "10")),
                    execution_mode="test_manual",
                    run_mode="single_instrument_test",
                    execution_credentials=ExchangeCredentials(
                        exchange=str(exchange or "").strip().lower(),
                        api_key=str(api_key or "").strip(),
                        api_secret=str(api_secret or "").strip(),
                        api_passphrase=str(api_passphrase or "").strip(),
                        account_profile=dict(account_profile or {}),
                    ),
                )
                self.worker_manager.start_worker(task)
            except Exception as exc:
                self._logger.error("worker start failed | worker_id=%s | error=%s", worker_id, exc)
                self.worker_command_failed.emit(worker_id, str(exc))

        threading.Thread(target=_run, name=f"worker-start-{worker_id}", daemon=True).start()

    def stop_test_runtime(self, worker_id: str) -> None:
        try:
            self.worker_manager.stop_worker(worker_id)
        except Exception as exc:
            self.worker_command_failed.emit(worker_id, str(exc))

    def start_dual_quotes_runtime_async(
        self,
        *,
        worker_id: str,
        left_exchange: str,
        left_market_type: str,
        left_symbol: str,
        right_exchange: str,
        right_market_type: str,
        right_symbol: str,
    ) -> None:
        self._logger.info(
            "ui start dual quotes runtime requested | worker_id=%s | left=%s:%s:%s | right=%s:%s:%s",
            worker_id,
            left_exchange,
            left_market_type,
            left_symbol,
            right_exchange,
            right_market_type,
            right_symbol,
        )

        def _run() -> None:
            try:
                left_instrument = self._resolve_instrument(left_exchange, left_market_type, left_symbol)
                right_instrument = self._resolve_instrument(right_exchange, right_market_type, right_symbol)
                task = WorkerTask(
                    worker_id=worker_id,
                    left_instrument=left_instrument,
                    right_instrument=right_instrument,
                    entry_threshold=Decimal("0"),
                    exit_threshold=Decimal("0"),
                    target_notional=Decimal("0"),
                    step_notional=Decimal("0"),
                    execution_mode="quotes_only",
                    run_mode="dual_exchange_quotes",
                    execution_credentials=None,
                )
                self.worker_manager.start_worker(task)
            except Exception as exc:
                self._logger.error("dual quotes worker start failed | worker_id=%s | error=%s", worker_id, exc)
                self.worker_command_failed.emit(worker_id, str(exc))

        threading.Thread(target=_run, name=f"dual-worker-start-{worker_id}", daemon=True).start()

    def start_dual_execution_runtime_async(
        self,
        *,
        worker_id: str,
        left_exchange: str,
        left_market_type: str,
        left_symbol: str,
        left_api_key: str,
        left_api_secret: str,
        left_api_passphrase: str = "",
        left_account_profile: dict | None = None,
        right_exchange: str,
        right_market_type: str,
        right_symbol: str,
        right_api_key: str,
        right_api_secret: str,
        right_api_passphrase: str = "",
        right_account_profile: dict | None = None,
    ) -> None:
        self._logger.info(
            "ui start dual execution runtime requested | worker_id=%s | left=%s:%s:%s | right=%s:%s:%s",
            worker_id,
            left_exchange,
            left_market_type,
            left_symbol,
            right_exchange,
            right_market_type,
            right_symbol,
        )

        def _run() -> None:
            try:
                left_instrument = self._resolve_instrument(left_exchange, left_market_type, left_symbol)
                right_instrument = self._resolve_instrument(right_exchange, right_market_type, right_symbol)
                task = WorkerTask(
                    worker_id=worker_id,
                    left_instrument=left_instrument,
                    right_instrument=right_instrument,
                    entry_threshold=Decimal("0"),
                    exit_threshold=Decimal("0"),
                    target_notional=Decimal("0"),
                    step_notional=Decimal("0"),
                    execution_mode="manual_dual_test",
                    run_mode="dual_exchange_test_execution",
                    execution_credentials=None,
                    left_execution_credentials=ExchangeCredentials(
                        exchange=str(left_exchange or "").strip().lower(),
                        api_key=str(left_api_key or "").strip(),
                        api_secret=str(left_api_secret or "").strip(),
                        api_passphrase=str(left_api_passphrase or "").strip(),
                        account_profile=dict(left_account_profile or {}),
                    ),
                    right_execution_credentials=ExchangeCredentials(
                        exchange=str(right_exchange or "").strip().lower(),
                        api_key=str(right_api_key or "").strip(),
                        api_secret=str(right_api_secret or "").strip(),
                        api_passphrase=str(right_api_passphrase or "").strip(),
                        account_profile=dict(right_account_profile or {}),
                    ),
                )
                self.worker_manager.start_worker(task)
            except Exception as exc:
                self._logger.error("dual execution worker start failed | worker_id=%s | error=%s", worker_id, exc)
                self.worker_command_failed.emit(worker_id, str(exc))

        threading.Thread(target=_run, name=f"dual-exec-worker-start-{worker_id}", daemon=True).start()

    def start_spread_entry_runtime_async(
        self,
        *,
        worker_id: str,
        left_exchange: str,
        left_market_type: str,
        left_symbol: str,
        left_api_key: str,
        left_api_secret: str,
        left_api_passphrase: str = "",
        left_account_profile: dict | None = None,
        right_exchange: str,
        right_market_type: str,
        right_symbol: str,
        right_api_key: str,
        right_api_secret: str,
        right_api_passphrase: str = "",
        right_account_profile: dict | None = None,
        entry_threshold: str,
        max_quote_age_ms: str,
        max_quote_skew_ms: str,
        left_qty: str,
        right_qty: str,
    ) -> None:
        self._logger.info(
            "ui start spread entry runtime requested | worker_id=%s | left=%s:%s:%s | right=%s:%s:%s | entry_threshold=%s",
            worker_id,
            left_exchange,
            left_market_type,
            left_symbol,
            right_exchange,
            right_market_type,
            right_symbol,
            entry_threshold,
        )

        def _run() -> None:
            try:
                left_instrument = self._resolve_instrument(left_exchange, left_market_type, left_symbol)
                right_instrument = self._resolve_instrument(right_exchange, right_market_type, right_symbol)
                task = WorkerTask(
                    worker_id=worker_id,
                    left_instrument=left_instrument,
                    right_instrument=right_instrument,
                    entry_threshold=Decimal(str(entry_threshold or "0")),
                    exit_threshold=Decimal("0"),
                    target_notional=Decimal("0"),
                    step_notional=Decimal("0"),
                    execution_mode="spread_entry_execution",
                    run_mode="spread_entry_execution",
                    execution_credentials=None,
                    left_execution_credentials=ExchangeCredentials(
                        exchange=str(left_exchange or "").strip().lower(),
                        api_key=str(left_api_key or "").strip(),
                        api_secret=str(left_api_secret or "").strip(),
                        api_passphrase=str(left_api_passphrase or "").strip(),
                        account_profile=dict(left_account_profile or {}),
                    ),
                    right_execution_credentials=ExchangeCredentials(
                        exchange=str(right_exchange or "").strip().lower(),
                        api_key=str(right_api_key or "").strip(),
                        api_secret=str(right_api_secret or "").strip(),
                        api_passphrase=str(right_api_passphrase or "").strip(),
                        account_profile=dict(right_account_profile or {}),
                    ),
                    runtime_params={
                        "entry_threshold": str(entry_threshold or "0"),
                        "max_quote_age_ms": str(max_quote_age_ms or "0"),
                        "max_quote_skew_ms": str(max_quote_skew_ms or "0"),
                        "left_qty": str(left_qty or "0"),
                        "right_qty": str(right_qty or "0"),
                        "left_price_mode": "top_of_book",
                        "right_price_mode": "top_of_book",
                    },
                )
                self.worker_manager.start_worker(task)
            except Exception as exc:
                self._logger.error("spread entry worker start failed | worker_id=%s | error=%s", worker_id, exc)
                self.worker_command_failed.emit(worker_id, str(exc))

        threading.Thread(target=_run, name=f"spread-entry-worker-start-{worker_id}", daemon=True).start()

    def submit_test_order_async(self, worker_id: str, side: str, submitted_at_ms: int | None = None) -> None:
        self._logger.info(
            "ui submit test order requested | worker_id=%s | side=%s | submitted_at_ms=%s",
            worker_id,
            side,
            submitted_at_ms,
        )
        def _run() -> None:
            try:
                self.worker_manager.submit_test_order(worker_id, side, submitted_at_ms=submitted_at_ms)
            except Exception as exc:
                self._logger.error("worker order failed | worker_id=%s | side=%s | error=%s", worker_id, side, exc)
                self.worker_command_failed.emit(worker_id, str(exc))

        threading.Thread(target=_run, name=f"worker-order-{worker_id}-{side}", daemon=True).start()

    def submit_dual_test_orders_async(
        self,
        *,
        worker_id: str,
        left_side: str,
        right_side: str,
        left_qty: str,
        right_qty: str,
        left_price_mode: str = "top_of_book",
        right_price_mode: str = "top_of_book",
        submitted_at_ms: int | None = None,
    ) -> None:
        self._logger.info(
            "ui submit dual test orders requested | worker_id=%s | left=%s:%s | right=%s:%s",
            worker_id,
            left_side,
            left_qty,
            right_side,
            right_qty,
        )

        def _run() -> None:
            try:
                self.worker_manager.submit_dual_test_orders(
                    worker_id,
                    left_side=left_side,
                    right_side=right_side,
                    left_qty=left_qty,
                    right_qty=right_qty,
                    left_price_mode=left_price_mode,
                    right_price_mode=right_price_mode,
                    submitted_at_ms=submitted_at_ms,
                )
            except Exception as exc:
                self._logger.error("dual worker order failed | worker_id=%s | error=%s", worker_id, exc)
                self.worker_command_failed.emit(worker_id, str(exc))

        threading.Thread(target=_run, name=f"dual-worker-order-{worker_id}", daemon=True).start()

    def prefetch_exchange_instruments(self, exchange: str) -> None:
        for market_type, _title in self.list_market_types(exchange):
            self._prefetch_market_type(exchange, market_type)

    def prefetch_market_type(self, exchange: str, market_type: str) -> None:
        self._prefetch_market_type(exchange, market_type)

    def _connect_exchange(self, exchange: str, params: dict):
        credentials = ExchangeCredentials(
            exchange=exchange,
            api_key=str(params.get("api_key", "")),
            api_secret=str(params.get("api_secret", "")),
            api_passphrase=str(params.get("api_passphrase", "")),
        )
        if exchange == "binance":
            return self._binance_account_connector.connect(credentials)
        if exchange == "bitget":
            return self._bitget_account_connector.connect(credentials)
        if exchange == "bybit":
            return self._bybit_account_connector.connect(credentials)
        raise ValueError(f"Unsupported exchange: {exchange}")

    def _close_exchange_positions(self, exchange: str, params: dict):
        credentials = ExchangeCredentials(
            exchange=exchange,
            api_key=str(params.get("api_key", "")),
            api_secret=str(params.get("api_secret", "")),
            api_passphrase=str(params.get("api_passphrase", "")),
        )
        if exchange == "binance":
            return self._binance_account_connector.close_all_positions(credentials)
        if exchange == "bitget":
            return self._bitget_account_connector.close_all_positions(credentials)
        if exchange == "bybit":
            return self._bybit_account_connector.close_all_positions(credentials)
        raise ValueError(f"Unsupported exchange: {exchange}")

    def _ensure_exchange_loaded(self, exchange: str) -> None:
        if exchange == "binance":
            self._ensure_market_type_loaded(exchange, UiInstrumentType.SPOT.value)
            self._ensure_market_type_loaded(exchange, UiInstrumentType.PERPETUAL.value)
            return
        if exchange == "bybit":
            self._ensure_market_type_loaded(exchange, UiInstrumentType.SPOT.value)
            self._ensure_market_type_loaded(exchange, UiInstrumentType.PERPETUAL.value)
            return
        if exchange == "bitget":
            self._ensure_market_type_loaded(exchange, UiInstrumentType.SPOT.value)
            self._ensure_market_type_loaded(exchange, UiInstrumentType.PERPETUAL.value)
            return
        raise ValueError(f"Unsupported exchange: {exchange}")

    def _prefetch_market_type(self, exchange: str, market_type: str) -> None:
        normalized_key = (str(exchange or "").strip().lower(), str(market_type or "").strip().lower())
        with self._load_lock:
            if self._is_market_type_loaded(*normalized_key) or normalized_key in self._loading_market_types:
                return
            self._loading_market_types.add(normalized_key)

        def _run_prefetch() -> None:
            try:
                self._ensure_market_type_loaded(exchange, market_type)
                self.instruments_loaded.emit(normalized_key[0], normalized_key[1])
            except Exception as exc:
                self.instruments_load_failed.emit(normalized_key[0], normalized_key[1], str(exc))
                self.public_quote_error.emit(f"{exchange} instruments preload failed: {exc}")
            finally:
                with self._load_lock:
                    self._loading_market_types.discard(normalized_key)

        threading.Thread(
            target=_run_prefetch,
            name=f"prefetch-{normalized_key[0]}-{normalized_key[1]}",
            daemon=True,
        ).start()

    def _ensure_market_type_loaded(self, exchange: str, market_type: str) -> None:
        normalized_exchange = str(exchange or "").strip().lower()
        normalized_market_type = str(market_type or "").strip().lower()
        if normalized_exchange not in {"binance", "bitget", "bybit"}:
            raise ValueError(f"Unsupported exchange: {exchange}")

        with self._load_lock:
            if normalized_exchange == "binance" and normalized_market_type == UiInstrumentType.SPOT.value and self._binance_spot_loaded:
                return
            if normalized_exchange == "binance" and normalized_market_type == UiInstrumentType.PERPETUAL.value and self._binance_perp_loaded:
                return
            if normalized_exchange == "bitget" and normalized_market_type == UiInstrumentType.PERPETUAL.value and self._bitget_perp_loaded:
                return
            if normalized_exchange == "bitget" and normalized_market_type == UiInstrumentType.SPOT.value and self._bitget_spot_loaded:
                return
            if normalized_exchange == "bybit" and normalized_market_type == UiInstrumentType.SPOT.value and self._bybit_spot_loaded:
                return
            if normalized_exchange == "bybit" and normalized_market_type == UiInstrumentType.PERPETUAL.value and self._bybit_perp_loaded:
                return

        if normalized_market_type == UiInstrumentType.SPOT.value:
            if normalized_exchange == "binance":
                spot_instruments = self._binance_spot_loader.load_instruments()
                with self._load_lock:
                    current = self.instrument_registry.list_by_exchange("binance")
                    if not self._binance_spot_loaded:
                        self.instrument_registry.replace_exchange_instruments("binance", [*current, *spot_instruments])
                        self._binance_spot_loaded = True
                return
            if normalized_exchange == "bitget":
                spot_instruments = self._bitget_spot_loader.load_instruments()
                with self._load_lock:
                    if not self._bitget_spot_loaded:
                        self.instrument_registry.replace_exchange_instruments("bitget", spot_instruments)
                        self._bitget_spot_loaded = True
                return
            if normalized_exchange == "bybit":
                spot_instruments = self._bybit_spot_loader.load_instruments()
                with self._load_lock:
                    current = self.instrument_registry.list_by_exchange("bybit")
                    if not self._bybit_spot_loaded:
                        self.instrument_registry.replace_exchange_instruments("bybit", [*current, *spot_instruments])
                        self._bybit_spot_loaded = True
                return

        if normalized_market_type == UiInstrumentType.PERPETUAL.value:
            if normalized_exchange == "binance":
                perp_instruments = self._binance_loader.load_instruments()
                with self._load_lock:
                    current = self.instrument_registry.list_by_exchange("binance")
                    if not self._binance_perp_loaded:
                        self.instrument_registry.replace_exchange_instruments("binance", [*current, *perp_instruments])
                        self._binance_perp_loaded = True
                return
            if normalized_exchange == "bitget":
                perp_instruments = self._bitget_linear_loader.load_instruments()
                with self._load_lock:
                    current = self.instrument_registry.list_by_exchange("bitget")
                    if not self._bitget_perp_loaded:
                        self.instrument_registry.replace_exchange_instruments("bitget", [*current, *perp_instruments])
                        self._bitget_perp_loaded = True
                return
            if normalized_exchange == "bybit":
                perp_instruments = self._bybit_linear_loader.load_instruments()
                with self._load_lock:
                    if not self._bybit_perp_loaded:
                        self.instrument_registry.replace_exchange_instruments("bybit", perp_instruments)
                        self._bybit_perp_loaded = True
                return

        raise ValueError(f"Unsupported market type: {market_type}")

    def _is_market_type_loaded(self, exchange: str, market_type: str) -> bool:
        if exchange == "binance":
            if market_type == UiInstrumentType.SPOT.value:
                return self._binance_spot_loaded
            if market_type == UiInstrumentType.PERPETUAL.value:
                return self._binance_perp_loaded
        if exchange == "bitget" and market_type == UiInstrumentType.SPOT.value:
            return self._bitget_spot_loaded
        if exchange == "bitget" and market_type == UiInstrumentType.PERPETUAL.value:
            return self._bitget_perp_loaded
        if exchange == "bybit" and market_type == UiInstrumentType.SPOT.value:
            return self._bybit_spot_loaded
        if exchange == "bybit" and market_type == UiInstrumentType.PERPETUAL.value:
            return self._bybit_perp_loaded
        return False

    def _resolve_instrument(self, exchange: str, market_type: str, symbol: str):
        normalized_exchange = str(exchange or "").strip().lower()
        normalized_market_type = str(market_type or "").strip().lower()
        normalized_symbol = str(symbol or "").strip().upper()
        if not normalized_symbol:
            raise ValueError("Instrument symbol is required")
        self._ensure_market_type_loaded(normalized_exchange, normalized_market_type)
        instrument = self.instrument_registry.find_by_ui_symbol(
            normalized_exchange,
            symbol=normalized_symbol,
            ui_market_type=normalized_market_type,
        )
        if instrument is None:
            raise ValueError(f"Instrument not found: {normalized_symbol}")
        return instrument

    def _on_worker_state(self, state: object) -> None:
        if isinstance(state, WorkerState):
            self.worker_state_updated.emit(state.worker_id, state.to_dict())

    def _on_worker_event(self, event: object) -> None:
        if isinstance(event, WorkerEvent):
            self.worker_event_received.emit(event.worker_id, event.to_dict())

    def _account_connector_for_exchange(self, exchange: str):
        if exchange == "binance":
            return self._binance_account_connector
        if exchange == "bitget":
            return self._bitget_account_connector
        if exchange == "bybit":
            return self._bybit_account_connector
        raise ValueError(f"Unsupported exchange: {exchange}")

    @staticmethod
    def _display_symbol(instrument: InstrumentId) -> str:
        symbol = str(instrument.symbol or "").strip().upper()
        base_asset = str(instrument.spec.base_asset or "").strip().upper()
        quote_asset = str(instrument.spec.quote_asset or instrument.spec.settle_asset or "").strip().upper()
        if instrument.exchange == "bybit" and instrument.market_type == "linear_perp":
            if quote_asset == "USDC" and symbol.endswith("PERP") and base_asset:
                return f"{base_asset}{quote_asset}"
        return symbol

