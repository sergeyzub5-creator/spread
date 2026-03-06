import sys

from PySide6.QtWidgets import QApplication

from app.core.events.bus import EventBus
from app.core.instruments.registry import InstrumentRegistry
from app.core.logging.logger_factory import get_logger, reset_session_trace_log, session_trace_log_path
from app.core.market_data.service import MarketDataService
from app.ui import AppWindow
from app.core.workers.manager import WorkerManager
from app.ui.coordinator import UiCoordinator
from app.ui.theme import get_theme_manager
from app.ui.widgets.startup_splash import StartupSplash


def main() -> int:
    trace_path = reset_session_trace_log()
    startup_logger = get_logger("app.startup")
    startup_logger.info("session trace log reset | path=%s", trace_path)
    startup_logger.info("session startup begin")

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
    startup_logger.info("ui startup complete | trace_log=%s", session_trace_log_path())
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
