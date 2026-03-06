import sys

from PySide6.QtWidgets import QApplication

from app.core.events.bus import EventBus
from app.core.instruments.registry import InstrumentRegistry
from app.core.market_data.service import MarketDataService
from app.core.workers.manager import WorkerManager
from app.ui.coordinator import UiCoordinator
from ui.theme import get_theme_manager
from ui.window import AppWindow
from ui.widgets.startup_splash import StartupSplash


def main() -> int:
    app = QApplication(sys.argv)
    app.setApplicationName("Spread Sniper UI Shell")
    get_theme_manager().set_theme("dark")

    instrument_registry = InstrumentRegistry()
    market_data_service = MarketDataService()
    event_bus = EventBus()
    worker_manager = WorkerManager(market_data_service=market_data_service, event_bus=event_bus)
    coordinator = UiCoordinator(
        instrument_registry=instrument_registry,
        market_data_service=market_data_service,
        worker_manager=worker_manager,
        event_bus=event_bus,
    )
    coordinator.bootstrap()

    window = AppWindow(coordinator=coordinator)
    splash = StartupSplash()
    splash.finished.connect(window.show)
    splash.start()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
