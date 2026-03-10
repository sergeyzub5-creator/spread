from __future__ import annotations

from PySide6.QtCore import QTimer, Signal
from PySide6.QtWidgets import QWidget

from app.ui.tabs.runtime_test_tab_parts import RuntimeTestTabPartsMixin


class RuntimeTestTab(RuntimeTestTabPartsMixin, QWidget):
    action_triggered = Signal(str)

    def __init__(self, coordinator=None, parent=None) -> None:
        super().__init__(parent)
        self.coordinator = coordinator
        self.worker_id = "test_runtime"
        self._exchange_items: list[tuple[str, str]] = []
        self._display_to_symbol: dict[str, str] = {}
        self._symbols: list[str] = []
        self._running = False
        self._pending_state: dict | None = None
        self._ui_state_timer = QTimer(self)
        self._ui_state_timer.setInterval(50)
        self._ui_state_timer.timeout.connect(self._flush_pending_state)
        self._transport_mock_controls: list[dict[str, object]] = []
        self._build_ui()
        self.apply_theme()
        self.retranslate_ui()
        self._bind_coordinator()
        self._populate_exchange_options()
        self._prefetch_current_exchange()
