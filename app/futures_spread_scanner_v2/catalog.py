from __future__ import annotations

from dataclasses import dataclass

from app.futures_spread_scanner_v2.common.i18n import tr


@dataclass(slots=True, frozen=True)
class RuntimeSelectionDraft:
    runtime_class: str | None = None
    exchange_id: str | None = None
    asset_type: str | None = None
    selected_type: str | None = None


@dataclass(slots=True, frozen=True)
class RuntimeCatalogEntry:
    runtime_id: str
    runtime_class: str
    title_key: str
    exchange_id: str | None = None
    asset_type: str | None = None
    selected_type: str | None = None


class RuntimeCatalogService:
    def __init__(self) -> None:
        self._entries = (
            RuntimeCatalogEntry(
                runtime_id="pair",
                runtime_class="starter",
                title_key="runtime.pair",
                selected_type="pair",
            ),
            RuntimeCatalogEntry(
                runtime_id="binance_futures_perp",
                runtime_class="base",
                title_key="runtime.binance_futures_perp",
                exchange_id="binance",
                asset_type="perpetual_futures",
            ),
            RuntimeCatalogEntry(
                runtime_id="bybit_futures_perp",
                runtime_class="base",
                title_key="runtime.bybit_futures_perp",
                exchange_id="bybit",
                asset_type="perpetual_futures",
            ),
            RuntimeCatalogEntry(
                runtime_id="spread::binance_futures_perp::bybit_futures_perp",
                runtime_class="output",
                title_key="runtime.spread",
                selected_type="spread",
            ),
            RuntimeCatalogEntry(
                runtime_id="rate_delta::binance_futures_perp::bybit_futures_perp",
                runtime_class="output",
                title_key="scanner.col_annual",
                selected_type="rate_delta",
            ),
        )

    def class_options(self) -> list[tuple[str, str]]:
        return [
            ("starter", tr("workspace.runtime_class_starter")),
            ("base", tr("workspace.runtime_class_base")),
            ("output", tr("workspace.runtime_class_output")),
        ]

    def exchange_options(self, *, runtime_class: str | None) -> list[tuple[str, str]]:
        if runtime_class != "base":
            return []
        seen: set[str] = set()
        out: list[tuple[str, str]] = []
        for entry in self._entries:
            if entry.runtime_class != "base" or not entry.exchange_id or entry.exchange_id in seen:
                continue
            seen.add(entry.exchange_id)
            out.append((entry.exchange_id, tr(f"runtime.exchange.{entry.exchange_id}")))
        return out

    def asset_type_options(self, *, runtime_class: str | None, exchange_id: str | None) -> list[tuple[str, str]]:
        if runtime_class != "base" or not exchange_id:
            return []
        seen: set[str] = set()
        out: list[tuple[str, str]] = []
        for entry in self._entries:
            if entry.runtime_class != "base" or entry.exchange_id != exchange_id or not entry.asset_type:
                continue
            if entry.asset_type in seen:
                continue
            seen.add(entry.asset_type)
            out.append((entry.asset_type, tr(f"runtime.asset.{entry.asset_type}")))
        return out

    def type_options(self, *, runtime_class: str | None) -> list[tuple[str, str]]:
        if runtime_class == "starter":
            return [("pair", tr("runtime.pair"))]
        if runtime_class == "output":
            return [("spread", tr("runtime.spread")), ("rate_delta", tr("scanner.col_annual"))]
        return []

    def resolve_runtime_id(self, draft: RuntimeSelectionDraft) -> str | None:
        runtime_class = str(draft.runtime_class or "").strip()
        if runtime_class == "starter":
            return "pair" if str(draft.selected_type or "").strip() == "pair" else None
        if runtime_class == "output":
            selected_type = str(draft.selected_type or "").strip()
            if selected_type == "spread":
                return "spread::binance_futures_perp::bybit_futures_perp"
            if selected_type == "rate_delta":
                return "rate_delta::binance_futures_perp::bybit_futures_perp"
            return None
        if runtime_class == "base":
            exchange_id = str(draft.exchange_id or "").strip()
            asset_type = str(draft.asset_type or "").strip()
            for entry in self._entries:
                if entry.runtime_class == "base" and entry.exchange_id == exchange_id and entry.asset_type == asset_type:
                    return entry.runtime_id
        return None

    def draft_for_runtime_id(self, runtime_id: str) -> RuntimeSelectionDraft:
        normalized = str(runtime_id or "").strip()
        for entry in self._entries:
            if entry.runtime_id != normalized:
                continue
            return RuntimeSelectionDraft(
                runtime_class=entry.runtime_class,
                exchange_id=entry.exchange_id,
                asset_type=entry.asset_type,
                selected_type=entry.selected_type,
            )
        return RuntimeSelectionDraft()


__all__ = ["RuntimeCatalogEntry", "RuntimeCatalogService", "RuntimeSelectionDraft"]
