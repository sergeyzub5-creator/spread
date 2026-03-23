from __future__ import annotations

import json
import os
from pathlib import Path

from app.futures_spread_scanner_v2.definitions import WorkspaceDefinition, WorkspaceNodeDefinition, WorkspaceStorageState

_STATE_PATH = Path("app/futures_spread_scanner_v2/data/futures_spread_scanner_v2_workspaces.json")


def _empty_state() -> WorkspaceStorageState:
    return WorkspaceStorageState(schema_version=1, active_workspace_id=None, workspaces=())


def _serialize_node(node: WorkspaceNodeDefinition) -> dict:
    return {
        "node_id": node.node_id,
        "runtime_class": node.runtime_class,
        "runtime_id": node.runtime_id,
        "exchange_id": node.exchange_id,
        "asset_type": node.asset_type,
        "selected_type": node.selected_type,
        "bookmark_order": list(node.bookmark_order),
        "depends_on": list(node.depends_on),
    }


def _deserialize_node(payload: object) -> WorkspaceNodeDefinition | None:
    if not isinstance(payload, dict):
        return None
    node_id = str(payload.get("node_id") or "").strip()
    runtime_class = str(payload.get("runtime_class") or "").strip().lower()
    runtime_id = str(payload.get("runtime_id") or "").strip()
    if not node_id or not runtime_class or not runtime_id:
        return None
    depends_on_raw = payload.get("depends_on")
    bookmark_order_raw = payload.get("bookmark_order")
    depends_on = tuple(
        str(item or "").strip()
        for item in (depends_on_raw if isinstance(depends_on_raw, list) else [])
        if str(item or "").strip()
    )
    bookmark_order = tuple(
        str(item or "").strip().upper()
        for item in (bookmark_order_raw if isinstance(bookmark_order_raw, list) else [])
        if str(item or "").strip()
    )
    return WorkspaceNodeDefinition(
        node_id=node_id,
        runtime_class=runtime_class,
        runtime_id=runtime_id,
        exchange_id=str(payload.get("exchange_id") or "").strip().lower() or None,
        asset_type=str(payload.get("asset_type") or "").strip().lower() or None,
        selected_type=str(payload.get("selected_type") or "").strip().lower() or None,
        bookmark_order=bookmark_order,
        depends_on=depends_on,
    )


def _serialize_workspace(workspace: WorkspaceDefinition) -> dict:
    return {
        "workspace_id": workspace.workspace_id,
        "title": workspace.title,
        "top_volume_limit": int(workspace.top_volume_limit),
        "sort_role": workspace.sort_role,
        "sort_source_id": workspace.sort_source_id,
        "sort_key": workspace.sort_key,
        "sort_descending": bool(workspace.sort_descending),
        "version": int(workspace.version),
        "column_stretch_by_role": dict(workspace.column_stretch_by_role),
        "nodes": [_serialize_node(node) for node in workspace.nodes],
    }


def _deserialize_workspace(payload: object) -> WorkspaceDefinition | None:
    if not isinstance(payload, dict):
        return None
    workspace_id = str(payload.get("workspace_id") or "").strip()
    title = str(payload.get("title") or "").strip()
    if not workspace_id or not title:
        return None
    nodes_payload = payload.get("nodes")
    nodes: list[WorkspaceNodeDefinition] = []
    if isinstance(nodes_payload, list):
        for item in nodes_payload:
            node = _deserialize_node(item)
            if node is not None:
                nodes.append(node)
    if not nodes:
        return None
    top_volume_limit = max(1, int(payload.get("top_volume_limit") or 200))
    stretch_raw = payload.get("column_stretch_by_role")
    column_stretch_by_role = {
        str(key or "").strip().lower(): max(1, int(value or 1))
        for key, value in (stretch_raw.items() if isinstance(stretch_raw, dict) else {})
        if str(key or "").strip()
    }
    if not column_stretch_by_role:
        column_stretch_by_role = {"starter": 14, "base": 20, "output": 8}
    return WorkspaceDefinition(
        workspace_id=workspace_id,
        title=title,
        top_volume_limit=top_volume_limit,
        nodes=tuple(nodes),
        sort_role=str(payload.get("sort_role") or "").strip().lower() or None,
        sort_source_id=str(payload.get("sort_source_id") or "").strip().lower() or None,
        sort_key=str(payload.get("sort_key") or "").strip() or None,
        sort_descending=bool(payload.get("sort_descending", True)),
        column_stretch_by_role=column_stretch_by_role,
        version=max(1, int(payload.get("version") or 1)),
    )


class WorkspaceStorage:
    def __init__(self, path: Path | None = None) -> None:
        self._path = path or _STATE_PATH

    def load(self) -> WorkspaceStorageState:
        if not self._path.exists():
            return _empty_state()
        try:
            with self._path.open("r", encoding="utf-8") as file:
                payload = json.load(file)
        except Exception:
            return _empty_state()
        if not isinstance(payload, dict):
            return _empty_state()
        workspaces_payload = payload.get("workspaces")
        workspaces: list[WorkspaceDefinition] = []
        if isinstance(workspaces_payload, list):
            for item in workspaces_payload:
                workspace = _deserialize_workspace(item)
                if workspace is not None:
                    workspaces.append(workspace)
        active_workspace_id = str(payload.get("active_workspace_id") or "").strip() or None
        if active_workspace_id and active_workspace_id not in {item.workspace_id for item in workspaces}:
            active_workspace_id = None
        return WorkspaceStorageState(
            schema_version=max(1, int(payload.get("schema_version") or 1)),
            active_workspace_id=active_workspace_id,
            workspaces=tuple(workspaces),
        )

    def save(self, state: WorkspaceStorageState) -> None:
        payload = {
            "schema_version": int(state.schema_version or 1),
            "active_workspace_id": state.active_workspace_id,
            "workspaces": [_serialize_workspace(item) for item in state.workspaces],
        }
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self._path.with_suffix(self._path.suffix + ".tmp")
        with tmp_path.open("w", encoding="utf-8") as file:
            json.dump(payload, file, ensure_ascii=False, indent=2)
        os.replace(tmp_path, self._path)


__all__ = ["WorkspaceStorage"]
