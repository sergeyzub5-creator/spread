from __future__ import annotations

from typing import TYPE_CHECKING

from PySide6.QtCore import QObject, Signal

from app.futures_spread_scanner_v2.runtime.contracts import StarterRowState, StarterSnapshot

if TYPE_CHECKING:
    from app.futures_spread_scanner_v2.runtime.workspace_runtime import WorkspaceRuntime


class StarterPairsRuntime(QObject):
    snapshot_changed = Signal()

    def __init__(self, base_runtimes: list[QObject] | None = None, workspace_runtime: WorkspaceRuntime | None = None) -> None:
        super().__init__()
        self._disposed = False
        self._base_runtimes: list[QObject] = []
        self._search_text = ""
        self._bookmarked_pairs: set[str] = set()
        self._bookmarked_order: list[str] = []
        self._workspace_runtime = workspace_runtime
        self._snapshot = StarterSnapshot(total_pairs=0, rows=[])
        if self._workspace_runtime is not None:
            self._workspace_runtime.snapshot_changed.connect(self._rebuild_snapshot)
        self.set_base_runtimes(base_runtimes or [])

    @staticmethod
    def _safe_emit(signal) -> None:
        try:
            signal.emit()
        except RuntimeError:
            return

    def snapshot(self) -> StarterSnapshot:
        return self._snapshot

    def bookmark_order(self) -> list[str]:
        return [item for item in self._bookmarked_order if item in self._bookmarked_pairs]

    def dispose(self) -> None:
        self._disposed = True
        if self._workspace_runtime is not None:
            try:
                self._workspace_runtime.snapshot_changed.disconnect(self._rebuild_snapshot)
            except Exception:
                pass
        for runtime in self._base_runtimes:
            try:
                runtime.snapshot_changed.disconnect(self._rebuild_snapshot)
            except Exception:
                pass
        self._base_runtimes = []

    def set_base_runtimes(self, base_runtimes: list[QObject]) -> None:
        if self._disposed:
            return
        for runtime in self._base_runtimes:
            try:
                runtime.snapshot_changed.disconnect(self._rebuild_snapshot)
            except Exception:
                pass
        self._base_runtimes = list(base_runtimes)
        for runtime in self._base_runtimes:
            try:
                runtime.snapshot_changed.connect(self._rebuild_snapshot)
            except Exception:
                pass
        self._rebuild_snapshot()

    def set_search_text(self, text: str) -> None:
        if self._disposed:
            return
        normalized = self._normalize_search(text)
        if normalized == self._search_text:
            return
        self._search_text = normalized
        self._rebuild_snapshot()

    def toggle_bookmark(self, canonical: str) -> None:
        if self._disposed:
            return
        normalized = str(canonical or "").strip().upper()
        if not normalized:
            return
        if normalized in self._bookmarked_pairs:
            self._bookmarked_pairs.remove(normalized)
            self._bookmarked_order = [item for item in self._bookmarked_order if item != normalized]
        else:
            self._bookmarked_pairs.add(normalized)
            if normalized not in self._bookmarked_order:
                self._bookmarked_order.append(normalized)
        self._rebuild_snapshot()

    def reorder_bookmark(self, canonical: str, target_index: int | None) -> None:
        if self._disposed:
            return
        normalized = str(canonical or "").strip().upper()
        if not normalized or normalized not in self._bookmarked_pairs:
            return
        order = [item for item in self._bookmarked_order if item in self._bookmarked_pairs]
        if normalized not in order:
            order.append(normalized)
        current_index = order.index(normalized)
        if target_index is None:
            target_index = current_index
        target_index = max(0, min(int(target_index), len(order) - 1))
        if current_index == target_index:
            return
        order.pop(current_index)
        order.insert(target_index, normalized)
        self._bookmarked_order = order
        self._rebuild_snapshot()

    def set_bookmark_order(self, bookmark_order: list[str] | tuple[str, ...]) -> None:
        if self._disposed:
            return
        normalized_order: list[str] = []
        for item in bookmark_order or []:
            canonical = str(item or "").strip().upper()
            if canonical and canonical not in normalized_order:
                normalized_order.append(canonical)
        next_pairs = set(normalized_order)
        if next_pairs == self._bookmarked_pairs and normalized_order == self._bookmarked_order:
            return
        self._bookmarked_pairs = next_pairs
        self._bookmarked_order = normalized_order
        self._rebuild_snapshot()

    @staticmethod
    def _normalize_search(value: str | None) -> str:
        return str(value or "").strip().upper()

    @classmethod
    def _search_rank(cls, canonical: str, query: str) -> tuple[int, int, str] | None:
        canon = str(canonical or "").strip().upper()
        q = cls._normalize_search(query)
        if not q:
            return (2, 0, canon)
        idx = canon.find(q)
        if idx < 0:
            return None
        return (0 if idx == 0 else 1, idx, canon)

    def _intersection_pairs(self) -> set[str]:
        pair_sets: list[set[str]] = []
        for runtime in self._base_runtimes:
            snapshot = runtime.snapshot() if hasattr(runtime, "snapshot") else None
            if snapshot is None:
                continue
            canonicals = {
                str(row.canonical or "").strip().upper()
                for row in getattr(snapshot, "rows", []) or []
                if getattr(row, "kind", "") == "row" and str(getattr(row, "canonical", "") or "").strip()
            }
            if canonicals:
                pair_sets.append(canonicals)
        if not pair_sets:
            return set()
        out = set(pair_sets[0])
        for pair_set in pair_sets[1:]:
            out &= pair_set
        return out

    def _rebuild_snapshot(self) -> None:
        if self._disposed:
            return
        pairs = self._intersection_pairs()
        query = self._search_text
        sort_key = self._workspace_runtime.external_sort_key() if self._workspace_runtime is not None else None
        sort_values_by_canonical = (
            self._workspace_runtime.external_sort_values() if self._workspace_runtime is not None else {}
        )
        sort_descending = (
            self._workspace_runtime.external_sort_descending() if self._workspace_runtime is not None else True
        )
        has_external_sort = bool(sort_key and sort_values_by_canonical)

        ranked_pairs: list[tuple[tuple[int, int, float, str], str]] = []
        for canonical in pairs:
            rank = self._search_rank(canonical, query)
            if rank is None:
                continue
            sort_value = sort_values_by_canonical.get(canonical)
            if sort_value is None:
                sort_component = float("inf") if sort_descending else float("-inf")
            else:
                sort_component = -float(sort_value) if sort_descending else float(sort_value)
            ranked_pairs.append(((rank[0], rank[1], sort_component, rank[2]), canonical))
        ranked_pairs.sort(key=lambda item: item[0])
        ordered = [canonical for _rank, canonical in ranked_pairs]

        bookmark_order = [canonical for canonical in self._bookmarked_order if canonical in pairs]
        bookmark_set = set(bookmark_order)
        if query:
            bookmark_rows = [canonical for canonical in ordered if canonical in bookmark_set]
            other_rows = [canonical for canonical in ordered if canonical not in bookmark_set]
        else:
            bookmark_rows = bookmark_order
            other_candidates = [canonical for canonical in ordered if canonical not in bookmark_set]
            other_rows = other_candidates if has_external_sort else sorted(other_candidates)

        rows: list[StarterRowState] = [
            StarterRowState(kind="pair", canonical=canonical, bookmarked=True)
            for canonical in bookmark_rows
        ]
        if bookmark_rows and other_rows:
            rows.append(StarterRowState(kind="separator"))
        rows.extend(
            StarterRowState(kind="pair", canonical=canonical, bookmarked=False)
            for canonical in other_rows
        )
        next_snapshot = StarterSnapshot(total_pairs=len(pairs), rows=rows)
        if next_snapshot == self._snapshot:
            return
        self._snapshot = next_snapshot
        self._safe_emit(self.snapshot_changed)


__all__ = ["StarterPairsRuntime"]
