from __future__ import annotations

from app.core.events.bus import EventBus
from app.core.logging.logger_factory import get_logger
from app.core.market_data.service import MarketDataService
from app.core.models.workers import WorkerTask
from app.core.workers.runtime import WorkerRuntime


class WorkerManager:
    """Lifecycle manager for worker runtimes keyed by worker_id."""

    def __init__(self, market_data_service: MarketDataService, event_bus: EventBus) -> None:
        self.market_data_service = market_data_service
        self.event_bus = event_bus
        self.logger = get_logger("workers.manager")
        self._workers: dict[str, WorkerRuntime] = {}

    def create_worker(self, task: WorkerTask) -> WorkerRuntime:
        existing = self._workers.get(task.worker_id)
        if existing is not None:
            existing.stop()
        runtime = WorkerRuntime(task=task, market_data_service=self.market_data_service, event_bus=self.event_bus)
        self._workers[task.worker_id] = runtime
        return runtime

    def start_worker(self, task: WorkerTask) -> WorkerRuntime:
        runtime = self.create_worker(task)
        runtime.start()
        return runtime

    def stop_worker(self, worker_id: str) -> None:
        runtime = self._workers.get(worker_id)
        if runtime is None:
            return
        runtime.stop()
        self._workers.pop(worker_id, None)

    def shutdown(self) -> None:
        for worker_id in list(self._workers.keys()):
            self.stop_worker(worker_id)

    def submit_test_order(self, worker_id: str, side: str, submitted_at_ms: int | None = None) -> dict:
        runtime = self._workers.get(worker_id)
        if runtime is None:
            raise KeyError(f"Worker not found: {worker_id}")
        return runtime.submit_test_order(side, submitted_at_ms=submitted_at_ms)

    def submit_dual_test_orders(
        self,
        worker_id: str,
        *,
        left_side: str,
        right_side: str,
        left_qty: str,
        right_qty: str,
        left_price_mode: str = "top_of_book",
        right_price_mode: str = "top_of_book",
        submitted_at_ms: int | None = None,
    ) -> dict:
        runtime = self._workers.get(worker_id)
        if runtime is None:
            raise KeyError(f"Worker not found: {worker_id}")
        return runtime.submit_dual_test_orders(
            left_side=left_side,
            right_side=right_side,
            left_qty=left_qty,
            right_qty=right_qty,
            left_price_mode=left_price_mode,
            right_price_mode=right_price_mode,
            submitted_at_ms=submitted_at_ms,
        )

    def get_worker(self, worker_id: str) -> WorkerRuntime | None:
        return self._workers.get(worker_id)
