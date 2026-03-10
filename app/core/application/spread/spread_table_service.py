from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict


@dataclass(frozen=True, slots=True)
class SpreadView:
    """
    UI-ready projection of spread worker state.

    The UI layer should be able to render the spread value and
    entry status table using only this structure, without
    re-implementing business rules.
    """

    spread_value_text: str
    edge_tone: str | None
    entry_values: Dict[str, str]


class SpreadTableService:
    """
    Application service that prepares spread tab view models
    from raw worker state dictionaries.
    """

    def build_view_from_worker_state(self, state: dict[str, Any] | None) -> SpreadView:
        """
        Build a UI-ready view from a worker state dictionary
        (as published on the EventBus).
        """
        if not isinstance(state, dict):
            metrics: dict[str, Any] = {}
        else:
            raw_metrics = state.get("metrics")
            metrics = raw_metrics if isinstance(raw_metrics, dict) else {}

        spread_state = str(metrics.get("spread_state") or "WAITING_QUOTES")
        edge_1 = self._decimal_or_none(metrics.get("edge_1"))
        edge_2 = self._decimal_or_none(metrics.get("edge_2"))

        spread_value_text = "--"
        edge_tone: str | None = None

        if spread_state == "LIVE":
            active_edge = self._active_edge(edge_1, edge_2)
            if active_edge is not None:
                spread_value_text = self._format_spread_percent(active_edge)
            if edge_1 is not None and (edge_2 is None or edge_1 >= edge_2):
                edge_tone = "right_cheap"
            elif edge_2 is not None:
                edge_tone = "left_cheap"

        entry_values = self._build_entry_values(metrics)
        return SpreadView(
            spread_value_text=spread_value_text,
            edge_tone=edge_tone,
            entry_values=entry_values,
        )

    def _build_entry_values(self, metrics: dict[str, Any]) -> Dict[str, str]:
        best_edge_value = self._decimal_or_none(metrics.get("best_edge"))
        return {
            "spread.status.edge_value": self._format_spread_percent(best_edge_value),
            "spread.status.direction_value": str(metrics.get("entry_direction") or "--"),
            "spread.status.entry_state_value": str(metrics.get("entry_state") or "--"),
            "spread.status.last_result_value": str(metrics.get("last_result") or "--"),
            "spread.status.block_reason_value": str(metrics.get("entry_block_reason") or "--"),
        }

    @staticmethod
    def _active_edge(edge_1: Decimal | None, edge_2: Decimal | None) -> Decimal | None:
        if edge_1 is None and edge_2 is None:
            return None
        if edge_1 is not None and (edge_2 is None or edge_1 >= edge_2):
            return edge_1
        return edge_2

    @staticmethod
    def _decimal_or_none(value: Any) -> Decimal | None:
        if value in (None, "", "-"):
            return None
        try:
            return Decimal(str(value).strip())
        except Exception:
            return None

    @staticmethod
    def _format_spread_percent(value: Decimal | None) -> str:
        if value is None:
            return "--"
        return f"{(value * Decimal('100')):.2f}%"

