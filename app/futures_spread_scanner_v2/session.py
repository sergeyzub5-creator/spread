from __future__ import annotations

from dataclasses import dataclass

from PySide6.QtCore import QObject

from app.futures_spread_scanner_v2.common.logger import get_logger
from app.futures_spread_scanner_v2.definitions import WorkspaceDefinition, WorkspaceNodeDefinition
from app.futures_spread_scanner_v2.runtime import (
    RateDeltaRuntime,
    SpreadRuntime,
    StarterPairsRuntime,
    WorkspaceHeaderRuntime,
    WorkspaceRuntime,
    get_shared_binance_perp_runtime,
    get_shared_bybit_perp_runtime,
)


@dataclass(slots=True, frozen=True)
class WorkspaceColumnBinding:
    node_id: str
    role: str
    runtime_id: str
    exchange_id: str | None = None
    anchor_starter_id: str | None = None
    depends_on: tuple[str, ...] = ()


class WorkspaceSession(QObject):
    def __init__(self, definition: WorkspaceDefinition) -> None:
        super().__init__()
        self._logger = get_logger("scanner.v2.session", worker_id=definition.workspace_id)
        self._definition = definition
        self._shared_base_runtimes_by_node_id: dict[str, QObject] = {}
        self._starter_runtimes_by_node_id: dict[str, StarterPairsRuntime] = {}
        self._output_runtimes_by_node_id: dict[str, QObject] = {}
        self._column_bindings: list[WorkspaceColumnBinding] = []
        self._owned_qobjects: list[QObject] = []
        self._logger.info("session init | title=%s | nodes=%s", definition.title, len(definition.nodes))

        base_runtimes_in_order: list[QObject] = []
        for node in self._definition.nodes:
            if node.runtime_class != "base":
                continue
            runtime = self._shared_base_runtime_for(node, self._definition.top_volume_limit)
            self._shared_base_runtimes_by_node_id[node.node_id] = runtime
            base_runtimes_in_order.append(runtime)
            self._logger.info(
                "base runtime bound | node_id=%s | runtime_id=%s | exchange_id=%s",
                node.node_id,
                node.runtime_id,
                node.exchange_id,
            )

        self._workspace_runtime = WorkspaceRuntime(
            top_volume_limit=self._definition.top_volume_limit,
            base_runtimes=base_runtimes_in_order,
            initial_sort_role=self._definition.sort_role,
            initial_sort_source_id=self._definition.sort_source_id,
            initial_sort_key=self._definition.sort_key,
            initial_sort_descending=self._definition.sort_descending,
        )
        self._workspace_runtime.setParent(self)
        self._owned_qobjects.append(self._workspace_runtime)

        starter_base_runtimes: dict[str, list[QObject]] = {}
        current_starter_id: str | None = None
        for node in self._definition.nodes:
            if node.runtime_class == "starter":
                current_starter_id = node.node_id
                starter_base_runtimes.setdefault(current_starter_id, [])
                continue
            if node.runtime_class == "base" and current_starter_id is not None:
                runtime = self._shared_base_runtimes_by_node_id.get(node.node_id)
                if runtime is not None:
                    starter_base_runtimes.setdefault(current_starter_id, []).append(runtime)

        for node in self._definition.nodes:
            if node.runtime_class != "starter":
                continue
            starter_runtime = StarterPairsRuntime(
                starter_base_runtimes.get(node.node_id, []),
                workspace_runtime=self._workspace_runtime,
            )
            starter_runtime.set_bookmark_order(list(node.bookmark_order))
            starter_runtime.setParent(self)
            self._starter_runtimes_by_node_id[node.node_id] = starter_runtime
            self._owned_qobjects.append(starter_runtime)
            self._logger.info(
                "starter runtime created | node_id=%s | base_count=%s",
                node.node_id,
                len(starter_base_runtimes.get(node.node_id, [])),
            )

        for node in self._definition.nodes:
            if node.runtime_class != "output":
                continue
            starter_id = node.depends_on[0] if node.depends_on else None
            starter_runtime = self._starter_runtimes_by_node_id.get(starter_id or "")
            if starter_runtime is None:
                continue
            dependent_base_runtimes = [
                self._shared_base_runtimes_by_node_id[base_node_id]
                for base_node_id in node.depends_on[1:]
                if base_node_id in self._shared_base_runtimes_by_node_id
            ]
            output_runtime = self._build_output_runtime(node, starter_runtime, dependent_base_runtimes)
            if output_runtime is None:
                continue
            output_runtime.setParent(self)
            self._output_runtimes_by_node_id[node.node_id] = output_runtime
            self._owned_qobjects.append(output_runtime)
            self._logger.info(
                "output runtime created | node_id=%s | runtime_id=%s | depends_on=%s",
                node.node_id,
                node.runtime_id,
                list(node.depends_on),
            )

        header_starters = list(self._starter_runtimes_by_node_id.values())
        if header_starters:
            header_source = header_starters[0]
        else:
            header_source = StarterPairsRuntime([])
            header_source.setParent(self)
            self._owned_qobjects.append(header_source)
        self._header_runtime = WorkspaceHeaderRuntime(header_source, self._definition.top_volume_limit)
        self._header_runtime.setParent(self)
        self._owned_qobjects.append(self._header_runtime)
        if len(header_starters) > 1:
            self._header_runtime.set_starter_runtimes(header_starters)
        for runtime in self._shared_base_runtimes_by_node_id.values():
            self._header_runtime.top_volume_changed.connect(runtime.set_top_volume_limit)
            self._header_runtime.refresh_requested.connect(runtime.force_refresh)
            runtime.loading_changed.connect(self._sync_header_loading_state)
        self._header_runtime.top_volume_changed.connect(self._workspace_runtime.set_top_volume_limit)
        self._sync_header_loading_state()
        self._logger.info(
            "header runtime created | starter_groups=%s | shared_bases=%s",
            len(header_starters),
            len(self._shared_base_runtimes_by_node_id),
        )

        for node in self._definition.nodes:
            role = str(node.runtime_class or "").strip().lower()
            anchor_starter_id = None
            if role == "starter":
                anchor_starter_id = node.node_id
            elif role in {"base", "output"} and node.depends_on:
                anchor_starter_id = node.depends_on[0]
            self._column_bindings.append(
                WorkspaceColumnBinding(
                    node_id=node.node_id,
                    role=role,
                    runtime_id=node.runtime_id,
                    exchange_id=node.exchange_id,
                    anchor_starter_id=anchor_starter_id,
                    depends_on=node.depends_on,
                )
            )
        self._logger.info("session ready | column_bindings=%s", len(self._column_bindings))

    def definition(self) -> WorkspaceDefinition:
        return self._definition

    def workspace_runtime(self) -> WorkspaceRuntime:
        return self._workspace_runtime

    def header_runtime(self) -> WorkspaceHeaderRuntime:
        return self._header_runtime

    def column_bindings(self) -> list[WorkspaceColumnBinding]:
        return list(self._column_bindings)

    def runtime_for_node(self, node_id: str) -> QObject | None:
        normalized = str(node_id or "").strip()
        if normalized in self._shared_base_runtimes_by_node_id:
            return self._shared_base_runtimes_by_node_id[normalized]
        if normalized in self._starter_runtimes_by_node_id:
            return self._starter_runtimes_by_node_id[normalized]
        if normalized in self._output_runtimes_by_node_id:
            return self._output_runtimes_by_node_id[normalized]
        return None

    def starter_runtimes(self) -> dict[str, StarterPairsRuntime]:
        return dict(self._starter_runtimes_by_node_id)

    def dispose(self) -> None:
        self._logger.info(
            "session dispose | owned=%s | shared_bases=%s",
            len(self._owned_qobjects),
            len(self._shared_base_runtimes_by_node_id),
        )
        for runtime in list(self._shared_base_runtimes_by_node_id.values()):
            try:
                self._header_runtime.top_volume_changed.disconnect(runtime.set_top_volume_limit)
            except Exception:
                pass
            try:
                self._header_runtime.refresh_requested.disconnect(runtime.force_refresh)
            except Exception:
                pass
            try:
                runtime.loading_changed.disconnect(self._sync_header_loading_state)
            except Exception:
                pass
        for owned in self._owned_qobjects:
            dispose_method = getattr(owned, "dispose", None)
            if callable(dispose_method):
                try:
                    dispose_method()
                except Exception:
                    pass
        for owned in reversed(self._owned_qobjects):
            try:
                owned.deleteLater()
            except Exception:
                pass
        self._owned_qobjects.clear()
        self._logger.info("session disposed")

    @staticmethod
    def _shared_base_runtime_for(node: WorkspaceNodeDefinition, top_volume_limit: int) -> QObject:
        runtime_id = str(node.runtime_id or "").strip().lower()
        if runtime_id == "binance_futures_perp":
            return get_shared_binance_perp_runtime(top_volume_limit)
        if runtime_id == "bybit_futures_perp":
            return get_shared_bybit_perp_runtime(top_volume_limit)
        raise ValueError(f"Unsupported base runtime_id: {runtime_id}")

    @staticmethod
    def _build_output_runtime(
        node: WorkspaceNodeDefinition,
        starter_runtime: StarterPairsRuntime,
        base_runtimes: list[QObject],
    ) -> QObject | None:
        runtime_id = str(node.runtime_id or "").strip().lower()
        selected_type = str(node.selected_type or "").strip().lower()
        if runtime_id.startswith("rate_delta::") or selected_type == "rate_delta":
            return RateDeltaRuntime(starter_runtime, list(base_runtimes), runtime_id=node.runtime_id)
        if runtime_id.startswith("spread::") or selected_type == "spread":
            return SpreadRuntime(starter_runtime, list(base_runtimes), runtime_id=node.runtime_id)
        return None

    def _sync_header_loading_state(self, *_args) -> None:
        is_loading = any(
            bool(getattr(runtime.snapshot(), "loading", False))
            for runtime in self._shared_base_runtimes_by_node_id.values()
            if hasattr(runtime, "snapshot")
        )
        self._header_runtime.set_loading(is_loading)


class WorkspaceSessionFactory:
    def build(self, definition: WorkspaceDefinition) -> WorkspaceSession:
        return WorkspaceSession(definition)


__all__ = [
    "WorkspaceColumnBinding",
    "WorkspaceSession",
    "WorkspaceSessionFactory",
]
