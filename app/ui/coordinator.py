from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
import threading

from PySide6.QtCore import QObject, Signal

from app.core.app import CoreApp
from app.core.logging.logger_factory import get_logger
from app.ui.coordinator_parts import UiCoordinatorPartsMixin


class UiCoordinator(UiCoordinatorPartsMixin, QObject):
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
    execution_stream_health_updated = Signal(str, object)
    worker_command_failed = Signal(str, str)

    def __init__(self, core_app: CoreApp) -> None:
        # Explicitly initialise both QObject and mixin to avoid
        # breaking QObject construction order.
        QObject.__init__(self)
        UiCoordinatorPartsMixin.__init__(self)

        self.core_app = core_app
        # Backwards-compatible aliases used by UiCoordinatorPartsMixin.
        self.instrument_registry = core_app.instrument_registry
        self.market_data_service = core_app.market_data_service
        self.worker_manager = core_app.worker_manager
        self.event_bus = core_app.event_bus
        self._async_executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="ui-coordinator")
        self._async_lock = threading.RLock()
        self._pending_futures: set[Future] = set()
        self._loading_market_types: set[tuple[str, str]] = set()
        self._load_lock = threading.Lock()
        self._subscription_lock = threading.RLock()
        self._subscriptions: dict[str, tuple[object, object]] = {}
        self._monitor_lock = threading.RLock()
        self._account_monitors: dict[str, object] = {}
        self._shutdown = False
        self._logger = get_logger("ui.coordinator")
        self.event_bus.subscribe("worker_state", self._on_worker_state)
        self.event_bus.subscribe("worker_events", self._on_worker_event)

    def bootstrap(self) -> None:
        """
        Perform UI-side bootstrap of core services.

        The actual registration and orchestration logic should
        gradually move into CoreApp; this method will then
        delegate to a high-level bootstrap call.
        """
        # For now, keep existing behaviour: core wiring remains
        # in the UI process but is owned by CoreApp.
        from app.core.market_data.binance_spot_connector import BinanceSpotPublicConnector
        from app.core.market_data.binance_spot_normalizer import BinanceSpotQuoteNormalizer
        from app.core.market_data.binance_usdm_connector import BinanceUsdmPublicConnector
        from app.core.market_data.binance_usdm_normalizer import BinanceUsdmQuoteNormalizer
        from app.core.market_data.bitget_linear_connector import BitgetLinearPublicConnector
        from app.core.market_data.bitget_linear_normalizer import BitgetLinearQuoteNormalizer
        from app.core.market_data.bitget_spot_connector import BitgetSpotPublicConnector
        from app.core.market_data.bitget_spot_normalizer import BitgetSpotQuoteNormalizer
        from app.core.market_data.bybit_linear_connector import BybitLinearPublicConnector
        from app.core.market_data.bybit_linear_normalizer import BybitLinearQuoteNormalizer
        from app.core.market_data.bybit_spot_connector import BybitSpotPublicConnector
        from app.core.market_data.bybit_spot_normalizer import BybitSpotQuoteNormalizer

        self.market_data_service.register_exchange_transport("binance:spot", BinanceSpotPublicConnector(), BinanceSpotQuoteNormalizer())
        self.market_data_service.register_exchange_transport("binance:linear_perp", BinanceUsdmPublicConnector(), BinanceUsdmQuoteNormalizer())
        self.market_data_service.register_exchange_transport("bitget:spot", BitgetSpotPublicConnector(), BitgetSpotQuoteNormalizer())
        self.market_data_service.register_exchange_transport("bitget:linear_perp", BitgetLinearPublicConnector(), BitgetLinearQuoteNormalizer())
        self.market_data_service.register_exchange_transport("bybit:linear_perp", BybitLinearPublicConnector(), BybitLinearQuoteNormalizer())
        self.market_data_service.register_exchange_transport("bybit:spot", BybitSpotPublicConnector(), BybitSpotQuoteNormalizer())

    def _submit_background(self, task_name: str, fn) -> None:
        if self._shutdown:
            return
        future = self._async_executor.submit(fn)
        with self._async_lock:
            self._pending_futures.add(future)

        def _cleanup(done: Future) -> None:
            with self._async_lock:
                self._pending_futures.discard(done)
            try:
                done.result()
            except Exception:
                self._logger.exception("background task failed | task=%s", task_name)

        future.add_done_callback(_cleanup)
