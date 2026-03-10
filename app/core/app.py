from __future__ import annotations

from dataclasses import dataclass

from app.core.application.spread import SpreadTableService
from app.core.events.bus import EventBus
from app.core.instruments.registry import InstrumentRegistry
from app.core.market_data.service import MarketDataService
from app.core.workers.manager import WorkerManager


@dataclass(slots=True)
class CoreApp:
    """
    High-level façade for core services.

    For now this is a thin wrapper that owns the main backend
    services and exposes them as attributes. Over time more
    orchestration and domain-specific methods should move here
    from the UI layer.
    """

    instrument_registry: InstrumentRegistry
    market_data_service: MarketDataService
    event_bus: EventBus
    worker_manager: WorkerManager
    _spread_table_service: SpreadTableService | None = None

    @classmethod
    def create_default(cls) -> "CoreApp":
        """
        Construct CoreApp with default in-process services.

        This keeps all wiring in one place so that in the future
        the hosting model (separate process, remote service, etc.)
        can be changed without touching the UI layer.
        """
        instrument_registry = InstrumentRegistry()
        market_data_service = MarketDataService()
        event_bus = EventBus()
        worker_manager = WorkerManager(
            market_data_service=market_data_service,
            event_bus=event_bus,
        )
        app = cls(
            instrument_registry=instrument_registry,
            market_data_service=market_data_service,
            event_bus=event_bus,
            worker_manager=worker_manager,
        )
        app._spread_table_service = None
        return app

    def get_spread_table_service(self) -> SpreadTableService:
        """
        Lazily construct and return the spread table service.

        This keeps application-specific orchestration logic in
        the core layer while allowing UI tabs to remain thin.
        """
        if self._spread_table_service is None:
            self._spread_table_service = SpreadTableService()
        return self._spread_table_service

