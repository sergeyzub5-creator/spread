from __future__ import annotations

import threading

from PySide6.QtCore import QObject, Signal

from app.core.events.bus import EventBus
from app.core.instruments.binance_spot_loader import BinanceSpotInstrumentLoader
from app.core.instruments.binance_usdm_loader import BinanceUsdmInstrumentLoader
from app.core.instruments.registry import InstrumentRegistry
from app.core.market_data.binance_spot_connector import BinanceSpotPublicConnector
from app.core.market_data.binance_spot_normalizer import BinanceSpotQuoteNormalizer
from app.core.market_data.binance_usdm_connector import BinanceUsdmPublicConnector
from app.core.market_data.binance_usdm_normalizer import BinanceUsdmQuoteNormalizer
from app.core.market_data.service import MarketDataService
from app.core.models.market_data import QuoteL1
from app.core.models.instrument_types import UI_INSTRUMENT_TYPE_LABELS, UiInstrumentType
from app.core.workers.manager import WorkerManager


class UiCoordinator(QObject):
    """Thin bridge between GUI widgets and backend services."""

    public_quote_received = Signal(str, object)
    public_quote_error = Signal(str)
    instruments_loaded = Signal(str, str)
    instruments_load_failed = Signal(str, str, str)

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
        self._binance_spot_loaded = False
        self._binance_perp_loaded = False
        self._loading_market_types: set[tuple[str, str]] = set()
        self._load_lock = threading.Lock()
        self._subscriptions: dict[str, tuple[object, object]] = {}

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

    def available_exchanges(self) -> list[tuple[str, str]]:
        return [("binance", "Binance")]

    def list_market_types(self, exchange: str) -> list[tuple[str, str]]:
        if exchange == "binance":
            return [
                (UiInstrumentType.SPOT.value, UI_INSTRUMENT_TYPE_LABELS[UiInstrumentType.SPOT]),
                (UiInstrumentType.PERPETUAL.value, UI_INSTRUMENT_TYPE_LABELS[UiInstrumentType.PERPETUAL]),
            ]
        return []

    def list_instruments(self, exchange: str, market_type: str) -> list[str]:
        if not self._is_market_type_loaded(str(exchange or "").strip().lower(), str(market_type or "").strip().lower()):
            self._prefetch_market_type(exchange, market_type)
            return []
        instruments = self.instrument_registry.list_by_ui_market_type(exchange, market_type)
        return [instrument.symbol for instrument in instruments]

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

    def prefetch_exchange_instruments(self, exchange: str) -> None:
        for market_type, _title in self.list_market_types(exchange):
            self._prefetch_market_type(exchange, market_type)

    def prefetch_market_type(self, exchange: str, market_type: str) -> None:
        self._prefetch_market_type(exchange, market_type)

    def _ensure_exchange_loaded(self, exchange: str) -> None:
        if exchange == "binance":
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
        if normalized_exchange != "binance":
            raise ValueError(f"Unsupported exchange: {exchange}")

        with self._load_lock:
            if normalized_market_type == UiInstrumentType.SPOT.value and self._binance_spot_loaded:
                return
            if normalized_market_type == UiInstrumentType.PERPETUAL.value and self._binance_perp_loaded:
                return

        if normalized_market_type == UiInstrumentType.SPOT.value:
            spot_instruments = self._binance_spot_loader.load_instruments()
            with self._load_lock:
                current = self.instrument_registry.list_by_exchange("binance")
                if not self._binance_spot_loaded:
                    self.instrument_registry.replace_exchange_instruments("binance", [*current, *spot_instruments])
                    self._binance_spot_loaded = True
            return

        if normalized_market_type == UiInstrumentType.PERPETUAL.value:
            perp_instruments = self._binance_loader.load_instruments()
            with self._load_lock:
                current = self.instrument_registry.list_by_exchange("binance")
                if not self._binance_perp_loaded:
                    self.instrument_registry.replace_exchange_instruments("binance", [*current, *perp_instruments])
                    self._binance_perp_loaded = True
            return

        raise ValueError(f"Unsupported market type: {market_type}")

    def _is_market_type_loaded(self, exchange: str, market_type: str) -> bool:
        if exchange != "binance":
            return False
        if market_type == UiInstrumentType.SPOT.value:
            return self._binance_spot_loaded
        if market_type == UiInstrumentType.PERPETUAL.value:
            return self._binance_perp_loaded
        return False
