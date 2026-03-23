from __future__ import annotations

from PySide6.QtCore import QObject, Signal

from app.futures_spread_scanner_v2.runtime.comparison_runtime import BaseComparisonRuntime
from app.futures_spread_scanner_v2.runtime.contracts import (
    BaseComparisonSnapshot,
    BaseOutputRuntime,
    BasePerpRuntime,
    OutputSnapshot,
    PerpSnapshot,
    StarterSnapshot,
    WorkspaceHeaderSnapshot,
)
from app.futures_spread_scanner_v2.runtime.header_runtime import WorkspaceHeaderRuntime
from app.futures_spread_scanner_v2.runtime.starter_runtime import StarterPairsRuntime


class PerpColumnViewModel(QObject):
    changed = Signal()

    def __init__(self, runtime: BasePerpRuntime) -> None:
        super().__init__()
        self._runtime = runtime
        self._runtime.snapshot_changed.connect(self.changed.emit)

    def snapshot(self) -> PerpSnapshot:
        return self._runtime.snapshot()


class OutputColumnViewModel(QObject):
    changed = Signal()

    def __init__(self, runtime: BaseOutputRuntime) -> None:
        super().__init__()
        self._runtime = runtime
        self._runtime.snapshot_changed.connect(self.changed.emit)

    def snapshot(self) -> OutputSnapshot:
        return self._runtime.snapshot()


class BaseComparisonViewModel(QObject):
    changed = Signal()

    def __init__(self, runtime: BaseComparisonRuntime) -> None:
        super().__init__()
        self._runtime = runtime
        self._runtime.snapshot_changed.connect(self.changed.emit)

    def accent_for(self, exchange_id: str, canonical: str) -> str | None:
        return self._runtime.accent_for(exchange_id, canonical)

    def snapshot(self) -> BaseComparisonSnapshot:
        return self._runtime.snapshot()


class StarterPairsViewModel(QObject):
    changed = Signal()

    def __init__(self, runtime: StarterPairsRuntime) -> None:
        super().__init__()
        self._runtime = runtime
        self._runtime.snapshot_changed.connect(self.changed.emit)

    def snapshot(self) -> StarterSnapshot:
        return self._runtime.snapshot()

    def set_search_text(self, text: str) -> None:
        self._runtime.set_search_text(text)

    def toggle_bookmark(self, canonical: str) -> None:
        self._runtime.toggle_bookmark(canonical)

    def reorder_bookmark(self, canonical: str, target_index: int | None) -> None:
        self._runtime.reorder_bookmark(canonical, target_index)


class WorkspaceHeaderViewModel(QObject):
    changed = Signal()

    def __init__(self, runtime: WorkspaceHeaderRuntime) -> None:
        super().__init__()
        self._runtime = runtime
        self._runtime.snapshot_changed.connect(self.changed.emit)

    def snapshot(self) -> WorkspaceHeaderSnapshot:
        return self._runtime.snapshot()

    def set_top_volume_text(self, text: str) -> None:
        self._runtime.set_top_volume_text(text)

    def request_refresh(self) -> None:
        self._runtime.request_refresh()

    def request_notification(self) -> None:
        self._runtime.request_notification()


__all__ = [
    "BaseComparisonViewModel",
    "OutputColumnViewModel",
    "PerpColumnViewModel",
    "StarterPairsViewModel",
    "WorkspaceHeaderViewModel",
]
