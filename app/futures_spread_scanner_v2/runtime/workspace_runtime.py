from __future__ import annotations

from PySide6.QtCore import QObject, Signal

from app.futures_spread_scanner_v2.runtime.comparison_runtime import BaseComparisonRuntime
from app.futures_spread_scanner_v2.runtime.contracts import WorkspaceSnapshot


class WorkspaceRuntime(QObject):
    snapshot_changed = Signal()

    def __init__(
        self,
        *,
        top_volume_limit: int = 200,
        base_runtimes: list[QObject] | None = None,
        initial_sort_role: str | None = None,
        initial_sort_source_id: str | None = None,
        initial_sort_key: str | None = None,
        initial_sort_descending: bool = True,
    ) -> None:
        super().__init__()
        self._disposed = False
        self._top_volume_limit = max(1, int(top_volume_limit or 200))
        self._base_runtimes: list[QObject] = []
        self._comparison_runtime = BaseComparisonRuntime()
        self._sort_role: str | None = str(initial_sort_role or "").strip().lower() or None
        self._sort_source_id: str | None = str(initial_sort_source_id or "").strip().lower() or None
        self._sort_key: str | None = str(initial_sort_key or "").strip() or None
        self._sort_descending = bool(initial_sort_descending)
        self._sort_values_by_canonical: dict[str, float] = {}
        self._snapshot = WorkspaceSnapshot(
            top_volume_limit=self._top_volume_limit,
            sort_role=None,
            sort_source_id=None,
            sort_key=None,
            sort_descending=True,
            column_stretch_by_role={
                "starter": 14,
                "base": 20,
                "output": 8,
            },
            runtime_roles={},
        )
        self.set_base_runtimes(base_runtimes or [])

    @staticmethod
    def _safe_emit(signal) -> None:
        try:
            signal.emit()
        except RuntimeError:
            return

    def snapshot(self) -> WorkspaceSnapshot:
        return self._snapshot

    def dispose(self) -> None:
        self._disposed = True
        self._comparison_runtime.dispose()
        self._base_runtimes = []

    def comparison_runtime(self) -> BaseComparisonRuntime:
        return self._comparison_runtime

    def external_sort_key(self) -> str | None:
        return self._sort_key

    def external_sort_role(self) -> str | None:
        return self._sort_role

    def external_sort_source_id(self) -> str | None:
        return self._sort_source_id

    def external_sort_descending(self) -> bool:
        return bool(self._sort_descending)

    def external_sort_values(self) -> dict[str, float]:
        return dict(self._sort_values_by_canonical)

    def base_runtimes(self) -> list[QObject]:
        return list(self._base_runtimes)

    def column_stretch_by_role(self) -> dict[str, int]:
        return dict(self._snapshot.column_stretch_by_role)

    def stretch_for_role(self, role: str, default: int = 1) -> int:
        normalized_role = str(role or "").strip().lower()
        value = int(self._snapshot.column_stretch_by_role.get(normalized_role, default) or default)
        return max(1, value)

    def set_top_volume_limit(self, top_volume_limit: int | None) -> None:
        if self._disposed:
            return
        next_limit = max(1, int(top_volume_limit or 200))
        if next_limit == self._top_volume_limit:
            return
        self._top_volume_limit = next_limit
        next_snapshot = WorkspaceSnapshot(
            top_volume_limit=self._top_volume_limit,
            sort_role=self._sort_role,
            sort_source_id=self._sort_source_id,
            sort_key=self._sort_key,
            sort_descending=self._sort_descending,
            column_stretch_by_role=dict(self._snapshot.column_stretch_by_role),
            runtime_roles=dict(self._snapshot.runtime_roles),
        )
        if next_snapshot != self._snapshot:
            self._snapshot = next_snapshot
            self._safe_emit(self.snapshot_changed)

    def set_base_runtimes(self, base_runtimes: list[QObject]) -> None:
        if self._disposed:
            return
        self._base_runtimes = list(base_runtimes)
        self._comparison_runtime.set_base_runtimes(self._base_runtimes)
        runtime_roles: dict[str, str] = {}
        for runtime in self._base_runtimes:
            snapshot = runtime.snapshot() if hasattr(runtime, "snapshot") else None
            exchange_id = str(getattr(snapshot, "exchange_id", "") or "").strip().lower() if snapshot is not None else ""
            if exchange_id:
                runtime_roles[exchange_id] = "base"
        next_snapshot = WorkspaceSnapshot(
            top_volume_limit=self._top_volume_limit,
            sort_role=self._sort_role,
            sort_source_id=self._sort_source_id,
            sort_key=self._sort_key,
            sort_descending=self._sort_descending,
            column_stretch_by_role=dict(self._snapshot.column_stretch_by_role),
            runtime_roles=runtime_roles,
        )
        if next_snapshot != self._snapshot:
            self._snapshot = next_snapshot
            self._safe_emit(self.snapshot_changed)

    def set_external_sort(
        self,
        sort_role: str,
        sort_source_id: str,
        sort_key: str,
        values_by_canonical: dict[str, float] | None,
        *,
        descending: bool = True,
    ) -> None:
        if self._disposed:
            return
        normalized_role = str(sort_role or "").strip().lower() or None
        normalized_source_id = str(sort_source_id or "").strip().lower() or None
        normalized_key = str(sort_key or "").strip() or None
        normalized_values = {
            str(canonical or "").strip().upper(): float(value)
            for canonical, value in (values_by_canonical or {}).items()
            if str(canonical or "").strip()
        }
        next_descending = bool(descending)
        if (
            normalized_role == self._sort_role
            and normalized_source_id == self._sort_source_id
            and normalized_key == self._sort_key
            and normalized_values == self._sort_values_by_canonical
            and next_descending == self._sort_descending
        ):
            return
        self._sort_role = normalized_role
        self._sort_source_id = normalized_source_id
        self._sort_key = normalized_key
        self._sort_values_by_canonical = normalized_values
        self._sort_descending = next_descending
        next_snapshot = WorkspaceSnapshot(
            top_volume_limit=self._top_volume_limit,
            sort_role=self._sort_role,
            sort_source_id=self._sort_source_id,
            sort_key=self._sort_key,
            sort_descending=self._sort_descending,
            column_stretch_by_role=dict(self._snapshot.column_stretch_by_role),
            runtime_roles=dict(self._snapshot.runtime_roles),
        )
        self._snapshot = next_snapshot
        self._safe_emit(self.snapshot_changed)


__all__ = ["WorkspaceRuntime"]
