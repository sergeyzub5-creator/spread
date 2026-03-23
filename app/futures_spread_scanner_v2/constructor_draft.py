from __future__ import annotations

from dataclasses import dataclass, replace

from PySide6.QtCore import QObject, Signal

from app.futures_spread_scanner_v2.catalog import RuntimeCatalogService, RuntimeSelectionDraft


@dataclass(slots=True, frozen=True)
class ConstructorRuntimeNode:
    node_id: int
    selection: RuntimeSelectionDraft


class ConstructorDraft(QObject):
    changed = Signal()

    def __init__(self) -> None:
        super().__init__()
        self._next_node_id = 1
        self._nodes: list[ConstructorRuntimeNode] = []
        self._catalog = RuntimeCatalogService()
        self._title = ""
        self._auto_title = False

    def nodes(self) -> list[ConstructorRuntimeNode]:
        return list(self._nodes)

    def catalog(self) -> RuntimeCatalogService:
        return self._catalog

    def starter_count(self) -> int:
        return sum(
            1
            for node in self._nodes
            if self._resolved_class(node) == "starter" and self.is_node_complete(node)
        )

    def base_count(self) -> int:
        return sum(
            1
            for node in self._nodes
            if self._resolved_class(node) == "base" and self.is_node_complete(node)
        )

    def output_count(self) -> int:
        return sum(
            1
            for node in self._nodes
            if self._resolved_class(node) == "output" and self.is_node_complete(node)
        )

    def has_any_content(self) -> bool:
        return bool(self._nodes)

    def title(self) -> str:
        return self._title

    def set_title(self, value: str) -> None:
        next_title = str(value or "").strip()
        if next_title == self._title:
            return
        self._title = next_title
        self.changed.emit()

    def auto_title_enabled(self) -> bool:
        return self._auto_title

    def set_auto_title_enabled(self, value: bool) -> None:
        next_value = bool(value)
        if next_value == self._auto_title:
            return
        self._auto_title = next_value
        self.changed.emit()

    def generated_title(self) -> str:
        family_order: list[str] = []
        for node in self._nodes:
            if self._resolved_class(node) != "base" or not self.is_node_complete(node):
                continue
            family = self._base_market_family(node)
            if family and family not in family_order:
                family_order.append(family)
        if not family_order:
            return ""
        if len(family_order) == 1:
            return self._family_title(family_order[0], short=False)
        if len(family_order) == 2:
            left = self._family_title(family_order[0], short=False)
            right = self._family_title(family_order[1], short=False).lower()
            title = f"{left} vs {right}"
            if len(title) <= 26:
                return title
            left = self._family_title(family_order[0], short=True)
            right = self._family_title(family_order[1], short=True).lower()
            return f"{left} vs {right}"
        return "Смешанные рынки"

    def effective_title(self) -> str:
        if self._auto_title:
            return self.generated_title()
        return self._title

    def title_ready(self) -> bool:
        return bool(self.effective_title().strip())

    def is_valid(self) -> bool:
        if not self._nodes:
            return False
        if any(not self.is_node_complete(node) for node in self._nodes):
            return False
        if self.starter_count() < 1 or self.base_count() < 1:
            return False
        return self.title_ready()

    def insert_node(self, index: int, runtime_class: str) -> ConstructorRuntimeNode:
        normalized_class = str(runtime_class or "").strip().lower()
        if normalized_class not in self.available_classes_for_position(index):
            raise ValueError(f"Runtime class '{normalized_class}' is not allowed at position {index}")
        node = ConstructorRuntimeNode(
            node_id=self._next_node_id,
            selection=RuntimeSelectionDraft(runtime_class=normalized_class or None),
        )
        self._next_node_id += 1
        insert_at = max(0, min(int(index), len(self._nodes)))
        self._nodes.insert(insert_at, node)
        self.changed.emit()
        return node

    def clear(self) -> None:
        if not self._nodes and not self._title and not self._auto_title:
            return
        self._nodes = []
        self._title = ""
        self._auto_title = False
        self.changed.emit()

    def replace_nodes(
        self,
        selections: list[RuntimeSelectionDraft],
        *,
        title: str = "",
        auto_title: bool = False,
    ) -> None:
        normalized_title = str(title or "").strip()
        next_nodes: list[ConstructorRuntimeNode] = []
        next_node_id = 1
        for selection in selections:
            next_nodes.append(ConstructorRuntimeNode(node_id=next_node_id, selection=selection))
            next_node_id += 1
        self._nodes = next_nodes
        self._next_node_id = next_node_id
        self._title = normalized_title
        self._auto_title = bool(auto_title)
        self.changed.emit()

    def update_node_class(self, node_id: int, runtime_class: str) -> None:
        normalized_class = str(runtime_class or "").strip().lower()
        current_index = next((idx for idx, node in enumerate(self._nodes) if node.node_id == node_id), -1)
        if current_index < 0:
            return
        if normalized_class not in self.available_classes_for_position(current_index):
            return
        next_selection = RuntimeSelectionDraft(runtime_class=normalized_class or None)
        next_nodes: list[ConstructorRuntimeNode] = []
        changed = False
        for node in self._nodes:
            if node.node_id != node_id:
                next_nodes.append(node)
                continue
            next_node = replace(node, selection=next_selection)
            next_nodes.append(next_node)
            changed = changed or next_node != node
        if not changed:
            return
        self._nodes = next_nodes
        self.changed.emit()

    def update_node_selection(self, node_id: int, selection: RuntimeSelectionDraft) -> None:
        next_nodes: list[ConstructorRuntimeNode] = []
        changed = False
        for node in self._nodes:
            if node.node_id != node_id:
                next_nodes.append(node)
                continue
            next_node = replace(node, selection=selection)
            next_nodes.append(next_node)
            changed = changed or next_node != node
        if not changed:
            return
        self._nodes = next_nodes
        self.changed.emit()

    def resolved_runtime_id(self, node: ConstructorRuntimeNode) -> str | None:
        return self._catalog.resolve_runtime_id(node.selection)

    def resolved_runtime_title_key(self, node: ConstructorRuntimeNode) -> str:
        runtime_class = str(node.selection.runtime_class or "").strip().lower()
        if runtime_class == "starter":
            return "v2.constructor_role_starter"
        if runtime_class == "base":
            return "v2.constructor_role_base"
        if runtime_class == "output":
            return "v2.constructor_role_output"
        return "v2.constructor_role_base"

    def is_node_complete(self, node: ConstructorRuntimeNode) -> bool:
        return self.resolved_runtime_id(node) is not None

    def node_runtime_title(self, node: ConstructorRuntimeNode) -> str | None:
        runtime_id = self.resolved_runtime_id(node)
        if not runtime_id:
            return None
        normalized = str(runtime_id).strip()
        if normalized == "pair":
            return "runtime.pair"
        if normalized == "binance_futures_perp":
            return "runtime.binance_futures_perp"
        if normalized == "bybit_futures_perp":
            return "runtime.bybit_futures_perp"
        if normalized.startswith("spread::"):
            return "runtime.spread"
        if normalized.startswith("rate_delta::"):
            return "scanner.col_annual"
        return None

    def nearest_left_starter_index(self, index: int) -> int | None:
        for idx in range(max(-1, int(index) - 1), -1, -1):
            if self._resolved_class(self._nodes[idx]) == "starter":
                return idx
        return None

    def nearest_left_starter_node(self, index: int) -> ConstructorRuntimeNode | None:
        starter_index = self.nearest_left_starter_index(index)
        if starter_index is None:
            return None
        return self._nodes[starter_index]

    def available_classes_for_position(self, index: int) -> list[str]:
        out = ["starter"]
        starter_index = self.nearest_left_starter_index(index)
        if starter_index is not None:
            out.append("base")
            if self._group_has_base_before(starter_index, index):
                out.append("output")
        return out

    def _group_has_base_before(self, starter_index: int, index: int) -> bool:
        start = max(0, int(starter_index) + 1)
        end = max(start, min(int(index), len(self._nodes)))
        for node in self._nodes[start:end]:
            if self._resolved_class(node) == "base":
                return True
        return False

    @staticmethod
    def _resolved_class(node: ConstructorRuntimeNode) -> str:
        return str(node.selection.runtime_class or "").strip().lower()

    @staticmethod
    def _base_market_family(node: ConstructorRuntimeNode) -> str | None:
        asset_type = str(node.selection.asset_type or "").strip().lower()
        if asset_type == "perpetual_futures":
            return "futures"
        if asset_type == "delivery_futures":
            return "delivery_futures"
        if asset_type == "spot":
            return "spot"
        return None

    @staticmethod
    def _family_title(family: str, *, short: bool) -> str:
        normalized = str(family or "").strip().lower()
        if normalized == "spot":
            return "Споты"
        if normalized == "delivery_futures":
            return "Сроч. фьючерсы" if short else "Срочные фьючерсы"
        if normalized == "futures":
            return "Фьючерзы"
        return "Рынки"


__all__ = ["ConstructorDraft", "ConstructorRuntimeNode"]
