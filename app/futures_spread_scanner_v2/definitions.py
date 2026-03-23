from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING
from uuid import uuid4

from app.futures_spread_scanner_v2.common.i18n import tr

if TYPE_CHECKING:
    from app.futures_spread_scanner_v2.constructor_draft import ConstructorDraft, ConstructorRuntimeNode


@dataclass(slots=True, frozen=True)
class WorkspaceNodeDefinition:
    node_id: str
    runtime_class: str
    runtime_id: str
    exchange_id: str | None = None
    asset_type: str | None = None
    selected_type: str | None = None
    bookmark_order: tuple[str, ...] = ()
    depends_on: tuple[str, ...] = ()


@dataclass(slots=True, frozen=True)
class WorkspaceDefinition:
    workspace_id: str
    title: str
    top_volume_limit: int
    nodes: tuple[WorkspaceNodeDefinition, ...]
    sort_role: str | None = None
    sort_source_id: str | None = None
    sort_key: str | None = None
    sort_descending: bool = True
    column_stretch_by_role: dict[str, int] = field(default_factory=dict)
    version: int = 1


@dataclass(slots=True, frozen=True)
class WorkspaceStorageState:
    schema_version: int
    active_workspace_id: str | None
    workspaces: tuple[WorkspaceDefinition, ...]


def build_workspace_definition_from_draft(
    draft: ConstructorDraft,
    *,
    top_volume_limit: int,
    workspace_id: str | None = None,
) -> WorkspaceDefinition:
    nodes: list[WorkspaceNodeDefinition] = []
    current_starter_node_id: str | None = None
    current_group_base_ids: list[str] = []
    base_index = 0
    starter_index = 0
    output_index = 0

    for draft_node in draft.nodes():
        runtime_id = str(draft.resolved_runtime_id(draft_node) or "").strip()
        runtime_class = str(draft_node.selection.runtime_class or "").strip().lower()
        if not runtime_id or not runtime_class:
            continue
        if runtime_class == "starter":
            starter_index += 1
            node_id = f"starter_{starter_index}"
            current_starter_node_id = node_id
            current_group_base_ids = []
            depends_on: tuple[str, ...] = ()
        elif runtime_class == "base":
            base_index += 1
            node_id = f"base_{base_index}"
            depends_on = (current_starter_node_id,) if current_starter_node_id else ()
            current_group_base_ids.append(node_id)
        elif runtime_class == "output":
            output_index += 1
            node_id = f"output_{output_index}"
            dependency_chain: list[str] = []
            if current_starter_node_id:
                dependency_chain.append(current_starter_node_id)
            dependency_chain.extend(current_group_base_ids)
            depends_on = tuple(dependency_chain)
        else:
            continue
        nodes.append(
            WorkspaceNodeDefinition(
                node_id=node_id,
                runtime_class=runtime_class,
                runtime_id=runtime_id,
                exchange_id=str(draft_node.selection.exchange_id or "").strip().lower() or None,
                asset_type=str(draft_node.selection.asset_type or "").strip().lower() or None,
                selected_type=str(draft_node.selection.selected_type or "").strip().lower() or None,
                depends_on=depends_on,
            )
        )

    normalized_title = str(draft.effective_title() or "").strip()
    column_stretch_by_role = {
        "starter": 14,
        "base": 20,
        "output": 8,
    }
    return WorkspaceDefinition(
        workspace_id=str(workspace_id or f"workspace_{uuid4().hex[:8]}"),
        title=normalized_title,
        top_volume_limit=max(1, int(top_volume_limit or 200)),
        nodes=tuple(nodes),
        sort_role=None,
        sort_source_id=None,
        sort_key=None,
        sort_descending=True,
        column_stretch_by_role=column_stretch_by_role,
        version=1,
    )


def build_default_workspace_definition() -> WorkspaceDefinition:
    class _DefaultDraft:
        def nodes(self) -> list[_DefaultNode]:
            return [
                _DefaultNode("starter", selected_type="pair"),
                _DefaultNode("base", exchange_id="binance", asset_type="perpetual_futures"),
                _DefaultNode("base", exchange_id="bybit", asset_type="perpetual_futures"),
                _DefaultNode("output", selected_type="rate_delta"),
                _DefaultNode("output", selected_type="spread"),
            ]

        def resolved_runtime_id(self, node: _DefaultNode) -> str | None:
            if node.selection.runtime_class == "starter":
                return "pair"
            if node.selection.runtime_class == "base":
                if node.selection.exchange_id == "binance" and node.selection.asset_type == "perpetual_futures":
                    return "binance_futures_perp"
                if node.selection.exchange_id == "bybit" and node.selection.asset_type == "perpetual_futures":
                    return "bybit_futures_perp"
            if node.selection.runtime_class == "output":
                if node.selection.selected_type == "rate_delta":
                    return "rate_delta::binance_futures_perp::bybit_futures_perp"
                if node.selection.selected_type == "spread":
                    return "spread::binance_futures_perp::bybit_futures_perp"
            return None

        def effective_title(self) -> str:
            return tr("tab.scanner")

    @dataclass(slots=True, frozen=True)
    class _Selection:
        runtime_class: str | None = None
        exchange_id: str | None = None
        asset_type: str | None = None
        selected_type: str | None = None

    @dataclass(slots=True, frozen=True)
    class _DefaultNode:
        runtime_class: str
        exchange_id: str | None = None
        asset_type: str | None = None
        selected_type: str | None = None

        @property
        def selection(self) -> _Selection:
            return _Selection(
                runtime_class=self.runtime_class,
                exchange_id=self.exchange_id,
                asset_type=self.asset_type,
                selected_type=self.selected_type,
            )

    return build_workspace_definition_from_draft(_DefaultDraft(), top_volume_limit=200, workspace_id="workspace_default")


__all__ = [
    "WorkspaceDefinition",
    "WorkspaceNodeDefinition",
    "WorkspaceStorageState",
    "build_default_workspace_definition",
    "build_workspace_definition_from_draft",
]
