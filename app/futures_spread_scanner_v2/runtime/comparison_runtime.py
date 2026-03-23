from __future__ import annotations

from PySide6.QtCore import QObject, Signal

from app.futures_spread_scanner_v2.runtime.contracts import BaseComparisonSnapshot


class BaseComparisonRuntime(QObject):
    snapshot_changed = Signal()

    def __init__(self, base_runtimes: list[QObject] | None = None) -> None:
        super().__init__()
        self._disposed = False
        self._base_runtimes: list[QObject] = []
        self._snapshot = BaseComparisonSnapshot(accents_by_exchange={})
        self.set_base_runtimes(base_runtimes or [])

    @staticmethod
    def _safe_emit(signal) -> None:
        try:
            signal.emit()
        except RuntimeError:
            return

    def snapshot(self) -> BaseComparisonSnapshot:
        return self._snapshot

    def dispose(self) -> None:
        self._disposed = True
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

    def accent_for(self, exchange_id: str, canonical: str) -> str | None:
        exchange_key = str(exchange_id or "").strip().lower()
        canonical_key = str(canonical or "").strip().upper()
        return (getattr(self._snapshot, "accents_by_exchange", {}).get(exchange_key, {}) or {}).get(canonical_key)

    def _rebuild_snapshot(self) -> None:
        if self._disposed:
            return
        prices_by_canonical: dict[str, dict[str, float]] = {}
        exchanges: set[str] = set()
        for runtime in self._base_runtimes:
            snapshot = runtime.snapshot() if hasattr(runtime, "snapshot") else None
            if snapshot is None:
                continue
            exchange_id = str(getattr(snapshot, "exchange_id", "") or "").strip().lower()
            if not exchange_id:
                continue
            exchanges.add(exchange_id)
            for row in getattr(snapshot, "rows", []) or []:
                if getattr(row, "kind", "") != "row":
                    continue
                canonical = str(getattr(row, "canonical", "") or "").strip().upper()
                if not canonical:
                    continue
                price_value = getattr(row, "price_value", None)
                if price_value is None:
                    continue
                prices_by_canonical.setdefault(canonical, {})[exchange_id] = float(price_value)

        accents_by_exchange: dict[str, dict[str, str]] = {exchange_id: {} for exchange_id in exchanges}
        for canonical, exchange_prices in prices_by_canonical.items():
            if not exchange_prices:
                continue
            if len(exchange_prices) <= 1:
                for exchange_id in exchange_prices:
                    accents_by_exchange.setdefault(exchange_id, {})[canonical] = "same"
                continue
            values = list(exchange_prices.values())
            min_price = min(values)
            max_price = max(values)
            if abs(max_price - min_price) <= 1e-12:
                for exchange_id in exchange_prices:
                    accents_by_exchange.setdefault(exchange_id, {})[canonical] = "same"
                continue
            for exchange_id, value in exchange_prices.items():
                if abs(value - min_price) <= 1e-12:
                    accents_by_exchange.setdefault(exchange_id, {})[canonical] = "low"
                elif abs(value - max_price) <= 1e-12:
                    accents_by_exchange.setdefault(exchange_id, {})[canonical] = "high"
                else:
                    accents_by_exchange.setdefault(exchange_id, {})[canonical] = None  # type: ignore[assignment]

        next_snapshot = BaseComparisonSnapshot(accents_by_exchange=accents_by_exchange)
        if next_snapshot == self._snapshot:
            return
        self._snapshot = next_snapshot
        self._safe_emit(self.snapshot_changed)


__all__ = ["BaseComparisonRuntime"]
