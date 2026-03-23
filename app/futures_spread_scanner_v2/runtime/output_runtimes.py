from __future__ import annotations

from app.futures_spread_scanner_v2.runtime.contracts import (
    BaseOutputRuntime,
    BasePerpRuntime,
    OutputRowState,
    OutputSnapshot,
    PerpRowState,
)
from app.futures_spread_scanner_v2.runtime.market_helpers import format_spread_pct, select_low_high_exchange_ids
from app.futures_spread_scanner_v2.runtime.starter_runtime import StarterPairsRuntime
from app.futures_spread_scanner_v2.common.i18n import tr


class RateDeltaRuntime(BaseOutputRuntime):
    def __init__(
        self,
        starter_runtime: StarterPairsRuntime,
        base_runtimes: list[BasePerpRuntime] | None = None,
        runtime_id: str = "rate_delta",
        title: str | None = None,
    ) -> None:
        super().__init__()
        self._disposed = False
        self._starter_runtime = starter_runtime
        self._starter_runtime.snapshot_changed.connect(self._rebuild_snapshot)
        self._base_runtimes: list[BasePerpRuntime] = []
        self._runtime_id = str(runtime_id or "rate_delta")
        self._title = str(title or tr("experimental.col_rate_delta_short"))
        self._snapshot = OutputSnapshot(runtime_id=self._runtime_id, title=self._title, rows=[])
        self.set_base_runtimes(base_runtimes or [])

    @staticmethod
    def _safe_emit(signal) -> None:
        try:
            signal.emit()
        except RuntimeError:
            return

    def snapshot(self) -> OutputSnapshot:
        return self._snapshot

    def dispose(self) -> None:
        self._disposed = True
        try:
            self._starter_runtime.snapshot_changed.disconnect(self._rebuild_snapshot)
        except Exception:
            pass
        for runtime in self._base_runtimes:
            try:
                runtime.snapshot_changed.disconnect(self._rebuild_snapshot)
            except Exception:
                pass
        self._base_runtimes = []

    def set_base_runtimes(self, base_runtimes: list[BasePerpRuntime]) -> None:
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

    @staticmethod
    def _normalize_rate_to_interval(rate: object, interval_hours: int | None, base_interval_hours: int | None):
        if rate is None or interval_hours is None or base_interval_hours is None:
            return None
        try:
            from decimal import Decimal

            rate_value = Decimal(str(rate))
            interval_value = int(interval_hours)
            base_interval_value = int(base_interval_hours)
        except Exception:
            return None
        if interval_value <= 0 or base_interval_value <= 0:
            return None
        return rate_value * (Decimal(str(base_interval_value)) / Decimal(str(interval_value)))

    @classmethod
    def _calc_rate_delta_pct(cls, rows_by_exchange: dict[str, PerpRowState]) -> float | None:
        if len(rows_by_exchange) < 2:
            return None
        from decimal import Decimal

        price_map: dict[str, Decimal] = {}
        for exchange_id, row in rows_by_exchange.items():
            try:
                price_value = getattr(row, "price_value", None)
                if price_value is None:
                    continue
                price_map[str(exchange_id)] = Decimal(str(price_value))
            except Exception:
                continue
        exchange_pair = select_low_high_exchange_ids(price_map, list(rows_by_exchange))
        if exchange_pair is None:
            return None
        low_exchange_id, high_exchange_id = exchange_pair
        low_row = rows_by_exchange.get(low_exchange_id)
        high_row = rows_by_exchange.get(high_exchange_id)
        if low_row is None or high_row is None:
            return None
        low_interval = getattr(low_row, "interval_hours", None)
        high_interval = getattr(high_row, "interval_hours", None)
        try:
            base_interval = min(int(low_interval), int(high_interval))
        except Exception:
            return None
        low_normalized = cls._normalize_rate_to_interval(getattr(low_row, "funding_rate_raw", None), low_interval, base_interval)
        high_normalized = cls._normalize_rate_to_interval(getattr(high_row, "funding_rate_raw", None), high_interval, base_interval)
        if low_normalized is None or high_normalized is None:
            return None
        try:
            return float((high_normalized - low_normalized) * Decimal("100"))
        except Exception:
            return None

    @staticmethod
    def _format_value(value: float | None) -> str:
        if value is None:
            return "-"
        try:
            return f"{value:+.4f}%"
        except Exception:
            return "-"

    @staticmethod
    def _accent_for_value(value: float | None) -> str | None:
        if value is None:
            return None
        if value > 0:
            return "positive"
        if value < 0:
            return "negative"
        return "neutral"

    def _rebuild_snapshot(self) -> None:
        if self._disposed:
            return
        starter_snapshot = self._starter_runtime.snapshot()
        runtime_rows: dict[str, dict[str, PerpRowState]] = {}
        for runtime in self._base_runtimes:
            snapshot = runtime.snapshot()
            exchange_id = str(getattr(snapshot, "exchange_id", "") or "").strip().lower()
            if not exchange_id:
                continue
            for row in getattr(snapshot, "rows", []) or []:
                if getattr(row, "kind", "") != "row":
                    continue
                canonical = str(getattr(row, "canonical", "") or "").strip().upper()
                if not canonical:
                    continue
                runtime_rows.setdefault(canonical, {})[exchange_id] = row

        rows: list[OutputRowState] = []
        for starter_row in starter_snapshot.rows:
            if starter_row.kind == "separator":
                rows.append(OutputRowState(kind="separator"))
                continue
            canonical = str(starter_row.canonical or "").strip().upper()
            delta_value = self._calc_rate_delta_pct(runtime_rows.get(canonical, {}))
            rows.append(
                OutputRowState(
                    kind="row",
                    canonical=canonical,
                    value_text=self._format_value(delta_value),
                    sort_value=delta_value,
                    accent=self._accent_for_value(delta_value),
                )
            )
        next_snapshot = OutputSnapshot(runtime_id=self._runtime_id, title=self._title, rows=rows)
        if next_snapshot == self._snapshot:
            return
        self._snapshot = next_snapshot
        self._safe_emit(self.snapshot_changed)


class SpreadRuntime(BaseOutputRuntime):
    def __init__(
        self,
        starter_runtime: StarterPairsRuntime,
        base_runtimes: list[BasePerpRuntime] | None = None,
        runtime_id: str = "spread",
        title: str | None = None,
    ) -> None:
        super().__init__()
        self._disposed = False
        self._starter_runtime = starter_runtime
        self._starter_runtime.snapshot_changed.connect(self._rebuild_snapshot)
        self._base_runtimes: list[BasePerpRuntime] = []
        self._runtime_id = str(runtime_id or "spread")
        self._title = str(title or tr("scanner.col_spread_pct"))
        self._snapshot = OutputSnapshot(runtime_id=self._runtime_id, title=self._title, rows=[])
        self.set_base_runtimes(base_runtimes or [])

    @staticmethod
    def _safe_emit(signal) -> None:
        try:
            signal.emit()
        except RuntimeError:
            return

    def snapshot(self) -> OutputSnapshot:
        return self._snapshot

    def dispose(self) -> None:
        self._disposed = True
        try:
            self._starter_runtime.snapshot_changed.disconnect(self._rebuild_snapshot)
        except Exception:
            pass
        for runtime in self._base_runtimes:
            try:
                runtime.snapshot_changed.disconnect(self._rebuild_snapshot)
            except Exception:
                pass
        self._base_runtimes = []

    def set_base_runtimes(self, base_runtimes: list[BasePerpRuntime]) -> None:
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

    @staticmethod
    def _calc_spread_pct(rows_by_exchange: dict[str, PerpRowState]) -> float | None:
        if len(rows_by_exchange) < 2:
            return None
        from decimal import Decimal

        price_map: dict[str, Decimal] = {}
        bid_map: dict[str, Decimal] = {}
        ask_map: dict[str, Decimal] = {}
        for exchange_id, row in rows_by_exchange.items():
            try:
                price_value = getattr(row, "price_value", None)
                if price_value is not None:
                    price_map[str(exchange_id)] = Decimal(str(price_value))
            except Exception:
                pass
            try:
                bid_value = getattr(row, "bid_price_value", None)
                if bid_value is not None:
                    bid_map[str(exchange_id)] = Decimal(str(bid_value))
            except Exception:
                pass
            try:
                ask_value = getattr(row, "ask_price_value", None)
                if ask_value is not None:
                    ask_map[str(exchange_id)] = Decimal(str(ask_value))
            except Exception:
                pass
        exchange_pair = select_low_high_exchange_ids(price_map, list(rows_by_exchange))
        if exchange_pair is None:
            return None
        low_exchange_id, high_exchange_id = exchange_pair
        low_ask = ask_map.get(low_exchange_id)
        high_bid = bid_map.get(high_exchange_id)
        if low_ask is not None and high_bid is not None and low_ask > 0:
            try:
                return float((high_bid - low_ask) / low_ask * 100)
            except Exception:
                return None
        low_price = price_map.get(low_exchange_id)
        high_price = price_map.get(high_exchange_id)
        if low_price is not None and high_price is not None and low_price > 0:
            try:
                return float((high_price - low_price) / low_price * 100)
            except Exception:
                return None
        return None

    def _rebuild_snapshot(self) -> None:
        if self._disposed:
            return
        starter_snapshot = self._starter_runtime.snapshot()
        runtime_rows: dict[str, dict[str, PerpRowState]] = {}
        for runtime in self._base_runtimes:
            snapshot = runtime.snapshot()
            exchange_id = str(getattr(snapshot, "exchange_id", "") or "").strip().lower()
            if not exchange_id:
                continue
            for row in getattr(snapshot, "rows", []) or []:
                if getattr(row, "kind", "") != "row":
                    continue
                canonical = str(getattr(row, "canonical", "") or "").strip().upper()
                if not canonical:
                    continue
                runtime_rows.setdefault(canonical, {})[exchange_id] = row

        rows: list[OutputRowState] = []
        for starter_row in starter_snapshot.rows:
            if starter_row.kind == "separator":
                rows.append(OutputRowState(kind="separator"))
                continue
            canonical = str(starter_row.canonical or "").strip().upper()
            spread_value = self._calc_spread_pct(runtime_rows.get(canonical, {}))
            rows.append(
                OutputRowState(
                    kind="row",
                    canonical=canonical,
                    value_text="-" if spread_value is None else format_spread_pct(spread_value),
                    sort_value=spread_value,
                    accent=None,
                )
            )
        next_snapshot = OutputSnapshot(runtime_id=self._runtime_id, title=self._title, rows=rows)
        if next_snapshot == self._snapshot:
            return
        self._snapshot = next_snapshot
        self._safe_emit(self.snapshot_changed)


__all__ = ["RateDeltaRuntime", "SpreadRuntime"]
