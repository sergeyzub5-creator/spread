from __future__ import annotations

from PySide6.QtCore import QObject, Signal

from app.futures_spread_scanner_v2.runtime.starter_runtime import StarterPairsRuntime
from app.futures_spread_scanner_v2.runtime.contracts import WorkspaceHeaderSnapshot
from app.futures_spread_scanner_v2.common.i18n import tr
from app.futures_spread_scanner_v2.common.volume_parse import format_volume_threshold, parse_daily_volume_threshold


class WorkspaceHeaderRuntime(QObject):
    snapshot_changed = Signal()
    top_volume_changed = Signal(int)
    refresh_requested = Signal()
    notification_requested = Signal()

    def __init__(self, starter_runtime: StarterPairsRuntime, top_volume: int | str | None = None) -> None:
        super().__init__()
        self._disposed = False
        self._starter_runtimes: list[StarterPairsRuntime] = []
        parsed_top = parse_daily_volume_threshold(top_volume)
        self._top_volume = parsed_top if parsed_top is not None else 200
        self._loading = False
        self._snapshot = WorkspaceHeaderSnapshot(
            top_volume_text=format_volume_threshold(self._top_volume),
            loaded_pairs_count=0,
            pairs_status_text=tr("scanner.loaded_pairs_count", count=0),
            add_notification_enabled=True,
            refresh_enabled=True,
            loading=False,
        )
        self.set_starter_runtimes([starter_runtime])
        self._rebuild_snapshot()

    @staticmethod
    def _safe_emit(signal, *args) -> None:
        try:
            signal.emit(*args)
        except RuntimeError:
            return

    def snapshot(self) -> WorkspaceHeaderSnapshot:
        return self._snapshot

    def dispose(self) -> None:
        self._disposed = True
        for runtime in self._starter_runtimes:
            try:
                runtime.snapshot_changed.disconnect(self._rebuild_snapshot)
            except Exception:
                pass
        self._starter_runtimes = []

    def top_volume_limit(self) -> int:
        return int(self._top_volume)

    def set_top_volume_limit(self, value: int | str | None) -> None:
        if self._disposed:
            return
        parsed = parse_daily_volume_threshold(value)
        if parsed is None or parsed == self._top_volume:
            return
        self._top_volume = parsed
        self._rebuild_snapshot()

    def set_starter_runtimes(self, runtimes: list[StarterPairsRuntime]) -> None:
        if self._disposed:
            return
        for runtime in self._starter_runtimes:
            try:
                runtime.snapshot_changed.disconnect(self._rebuild_snapshot)
            except Exception:
                pass
        self._starter_runtimes = [runtime for runtime in runtimes if runtime is not None]
        for runtime in self._starter_runtimes:
            try:
                runtime.snapshot_changed.connect(self._rebuild_snapshot)
            except Exception:
                pass
        self._rebuild_snapshot()

    def set_top_volume_text(self, text: str) -> None:
        if self._disposed:
            return
        parsed = parse_daily_volume_threshold(text)
        if parsed is None or parsed == self._top_volume:
            return
        self._top_volume = parsed
        self._rebuild_snapshot()

    def request_refresh(self) -> None:
        if self._disposed:
            return
        self._safe_emit(self.top_volume_changed, self._top_volume)
        self._safe_emit(self.refresh_requested)

    def request_notification(self) -> None:
        if self._disposed:
            return
        self._safe_emit(self.notification_requested)

    def set_loading(self, loading: bool) -> None:
        if self._disposed:
            return
        loading_flag = bool(loading)
        if loading_flag == self._loading:
            return
        self._loading = loading_flag
        self._rebuild_snapshot()

    def _rebuild_snapshot(self) -> None:
        if self._disposed:
            return
        count = sum(
            int(getattr(runtime.snapshot(), "total_pairs", 0) or 0)
            for runtime in self._starter_runtimes
        )
        next_snapshot = WorkspaceHeaderSnapshot(
            top_volume_text=format_volume_threshold(self._top_volume),
            loaded_pairs_count=count,
            pairs_status_text=tr("scanner.refreshing") if self._loading else tr("scanner.loaded_pairs_count", count=count),
            add_notification_enabled=not self._loading,
            refresh_enabled=not self._loading,
            loading=self._loading,
        )
        if next_snapshot == self._snapshot:
            return
        self._snapshot = next_snapshot
        self._safe_emit(self.snapshot_changed)


__all__ = ["WorkspaceHeaderRuntime"]
