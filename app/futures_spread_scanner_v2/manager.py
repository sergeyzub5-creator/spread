from __future__ import annotations

from PySide6.QtCore import QObject, Signal

from app.futures_spread_scanner_v2.common.logger import get_logger
from app.futures_spread_scanner_v2.constructor_draft import ConstructorDraft
from app.futures_spread_scanner_v2.definitions import (
    WorkspaceDefinition,
    WorkspaceStorageState,
    build_default_workspace_definition,
    build_workspace_definition_from_draft,
)
from app.futures_spread_scanner_v2.session import WorkspaceSession, WorkspaceSessionFactory
from app.futures_spread_scanner_v2.storage import WorkspaceStorage


class WorkspaceManager(QObject):
    workspaces_changed = Signal()
    active_workspace_changed = Signal(str)

    def __init__(
        self,
        *,
        storage: WorkspaceStorage | None = None,
        session_factory: WorkspaceSessionFactory | None = None,
    ) -> None:
        super().__init__()
        self._logger = get_logger("scanner.v2.manager")
        self._storage = storage or WorkspaceStorage()
        self._session_factory = session_factory or WorkspaceSessionFactory()
        self._state = self._storage.load()
        self._sessions: dict[str, WorkspaceSession] = {}
        self._logger.info(
            "manager init | stored_workspaces=%s | active=%s",
            len(self._state.workspaces),
            self._state.active_workspace_id,
        )
        if not self._state.workspaces:
            default_workspace = build_default_workspace_definition()
            self._state = WorkspaceStorageState(
                schema_version=1,
                active_workspace_id=default_workspace.workspace_id,
                workspaces=(default_workspace,),
            )
            self._storage.save(self._state)
            self._logger.info("manager created default workspace | workspace_id=%s", default_workspace.workspace_id)

    def workspaces(self) -> list[WorkspaceDefinition]:
        return list(self._state.workspaces)

    def active_workspace_id(self) -> str | None:
        return self._state.active_workspace_id

    def session_for(self, workspace_id: str) -> WorkspaceSession | None:
        normalized = str(workspace_id or "").strip()
        if not normalized:
            return None
        workspace = self.workspace_by_id(normalized)
        if workspace is None:
            return None
        session = self._sessions.get(normalized)
        if session is None:
            self._logger.info("session build requested | workspace_id=%s", normalized)
            session = self._session_factory.build(workspace)
            self._sessions[normalized] = session
            self._logger.info("session built | workspace_id=%s | nodes=%s", normalized, len(workspace.nodes))
        return session

    def release_session(self, workspace_id: str) -> None:
        normalized = str(workspace_id or "").strip()
        if not normalized:
            return
        session = self._sessions.pop(normalized, None)
        if session is None:
            return
        self._logger.info("session released | workspace_id=%s", normalized)
        session.dispose()

    def workspace_by_id(self, workspace_id: str) -> WorkspaceDefinition | None:
        normalized = str(workspace_id or "").strip()
        for workspace in self._state.workspaces:
            if workspace.workspace_id == normalized:
                return workspace
        return None

    def set_active_workspace(self, workspace_id: str | None) -> None:
        normalized = str(workspace_id or "").strip() or None
        if normalized == self._state.active_workspace_id:
            return
        if normalized is not None and self.workspace_by_id(normalized) is None:
            return
        self._state = WorkspaceStorageState(
            schema_version=self._state.schema_version,
            active_workspace_id=normalized,
            workspaces=self._state.workspaces,
        )
        self._storage.save(self._state)
        self._logger.info("active workspace changed | workspace_id=%s", normalized)
        self.active_workspace_changed.emit(normalized or "")

    def create_or_update_from_draft(
        self,
        draft: ConstructorDraft,
        *,
        top_volume_limit: int,
        workspace_id: str | None = None,
        make_active: bool = True,
    ) -> WorkspaceDefinition:
        definition = build_workspace_definition_from_draft(
            draft,
            top_volume_limit=top_volume_limit,
            workspace_id=workspace_id,
        )
        existing = self.workspace_by_id(definition.workspace_id)
        if existing is not None:
            bookmark_order_by_node_id = {
                node.node_id: node.bookmark_order
                for node in existing.nodes
                if str(node.runtime_class or "").strip().lower() == "starter"
            }
            next_nodes = []
            for node in definition.nodes:
                if str(node.runtime_class or "").strip().lower() == "starter":
                    updated_node = type(node)(
                        node_id=node.node_id,
                        runtime_class=node.runtime_class,
                        runtime_id=node.runtime_id,
                        exchange_id=node.exchange_id,
                        asset_type=node.asset_type,
                        selected_type=node.selected_type,
                        bookmark_order=bookmark_order_by_node_id.get(node.node_id, ()),
                        depends_on=node.depends_on,
                    )
                    next_nodes.append(updated_node)
                else:
                    next_nodes.append(node)
            definition = WorkspaceDefinition(
                workspace_id=definition.workspace_id,
                title=definition.title,
                top_volume_limit=definition.top_volume_limit,
                nodes=tuple(next_nodes),
                sort_role=existing.sort_role,
                sort_source_id=existing.sort_source_id,
                sort_key=existing.sort_key,
                sort_descending=existing.sort_descending,
                column_stretch_by_role=dict(definition.column_stretch_by_role),
                version=definition.version,
            )
        self._logger.info(
            "workspace definition built from draft | workspace_id=%s | title=%s | nodes=%s | top=%s",
            definition.workspace_id,
            definition.title,
            len(definition.nodes),
            definition.top_volume_limit,
        )
        return self.save_workspace(definition, make_active=make_active)

    def save_workspace(self, definition: WorkspaceDefinition, *, make_active: bool = True) -> WorkspaceDefinition:
        workspaces = list(self._state.workspaces)
        default_only_bootstrap = (
            definition.workspace_id != "workspace_default"
            and len(workspaces) == 1
            and workspaces[0].workspace_id == "workspace_default"
        )
        if default_only_bootstrap:
            self._logger.info(
                "replacing bootstrap default workspace | old=%s | new=%s",
                workspaces[0].workspace_id,
                definition.workspace_id,
            )
            stale_default_session = self._sessions.pop("workspace_default", None)
            if stale_default_session is not None:
                stale_default_session.dispose()
            workspaces = []
        replaced = False
        for index, item in enumerate(workspaces):
            if item.workspace_id != definition.workspace_id:
                continue
            workspaces[index] = definition
            replaced = True
            break
        if not replaced:
            workspaces.append(definition)
        stale_session = self._sessions.pop(definition.workspace_id, None)
        if stale_session is not None:
            self._logger.info("disposing stale session before save | workspace_id=%s", definition.workspace_id)
            stale_session.dispose()
        next_active_workspace_id = definition.workspace_id if make_active else self._state.active_workspace_id
        self._state = WorkspaceStorageState(
            schema_version=max(1, int(self._state.schema_version or 1)),
            active_workspace_id=next_active_workspace_id,
            workspaces=tuple(workspaces),
        )
        self._storage.save(self._state)
        self._logger.info(
            "workspace saved | workspace_id=%s | title=%s | total=%s | active=%s",
            definition.workspace_id,
            definition.title,
            len(self._state.workspaces),
            next_active_workspace_id,
        )
        self.workspaces_changed.emit()
        if make_active:
            self.active_workspace_changed.emit(definition.workspace_id)
        return definition

    def update_workspace_sort_state(
        self,
        workspace_id: str,
        *,
        sort_role: str | None,
        sort_source_id: str | None,
        sort_key: str | None,
        sort_descending: bool,
    ) -> None:
        normalized = str(workspace_id or "").strip()
        if not normalized:
            return
        workspaces = list(self._state.workspaces)
        changed = False
        for index, item in enumerate(workspaces):
            if item.workspace_id != normalized:
                continue
            next_definition = WorkspaceDefinition(
                workspace_id=item.workspace_id,
                title=item.title,
                top_volume_limit=item.top_volume_limit,
                nodes=item.nodes,
                sort_role=str(sort_role or "").strip().lower() or None,
                sort_source_id=str(sort_source_id or "").strip().lower() or None,
                sort_key=str(sort_key or "").strip() or None,
                sort_descending=bool(sort_descending),
                column_stretch_by_role=dict(item.column_stretch_by_role),
                version=item.version,
            )
            if next_definition == item:
                return
            workspaces[index] = next_definition
            changed = True
            break
        if not changed:
            return
        self._state = WorkspaceStorageState(
            schema_version=max(1, int(self._state.schema_version or 1)),
            active_workspace_id=self._state.active_workspace_id,
            workspaces=tuple(workspaces),
        )
        self._storage.save(self._state)
        self._logger.info(
            "workspace sort state saved | workspace_id=%s | role=%s | source=%s | key=%s | desc=%s",
            normalized,
            sort_role,
            sort_source_id,
            sort_key,
            sort_descending,
        )

    def update_workspace_bookmarks(
        self,
        workspace_id: str,
        *,
        starter_node_id: str,
        bookmark_order: list[str] | tuple[str, ...],
    ) -> None:
        normalized_workspace_id = str(workspace_id or "").strip()
        normalized_starter_node_id = str(starter_node_id or "").strip()
        if not normalized_workspace_id or not normalized_starter_node_id:
            return
        normalized_bookmarks = tuple(
            str(item or "").strip().upper()
            for item in bookmark_order or []
            if str(item or "").strip()
        )
        workspaces = list(self._state.workspaces)
        changed = False
        for workspace_index, workspace in enumerate(workspaces):
            if workspace.workspace_id != normalized_workspace_id:
                continue
            next_nodes = list(workspace.nodes)
            for node_index, node in enumerate(next_nodes):
                if node.node_id != normalized_starter_node_id:
                    continue
                updated_node = type(node)(
                    node_id=node.node_id,
                    runtime_class=node.runtime_class,
                    runtime_id=node.runtime_id,
                    exchange_id=node.exchange_id,
                    asset_type=node.asset_type,
                    selected_type=node.selected_type,
                    bookmark_order=normalized_bookmarks,
                    depends_on=node.depends_on,
                )
                if updated_node == node:
                    return
                next_nodes[node_index] = updated_node
                workspaces[workspace_index] = WorkspaceDefinition(
                    workspace_id=workspace.workspace_id,
                    title=workspace.title,
                    top_volume_limit=workspace.top_volume_limit,
                    nodes=tuple(next_nodes),
                    sort_role=workspace.sort_role,
                    sort_source_id=workspace.sort_source_id,
                    sort_key=workspace.sort_key,
                    sort_descending=workspace.sort_descending,
                    column_stretch_by_role=dict(workspace.column_stretch_by_role),
                    version=workspace.version,
                )
                changed = True
                break
            break
        if not changed:
            return
        self._state = WorkspaceStorageState(
            schema_version=max(1, int(self._state.schema_version or 1)),
            active_workspace_id=self._state.active_workspace_id,
            workspaces=tuple(workspaces),
        )
        self._storage.save(self._state)
        self._logger.info(
            "workspace bookmarks saved | workspace_id=%s | starter=%s | count=%s",
            normalized_workspace_id,
            normalized_starter_node_id,
            len(normalized_bookmarks),
        )

    def update_workspace_top_volume_limit(self, workspace_id: str, *, top_volume_limit: int) -> None:
        normalized_workspace_id = str(workspace_id or "").strip()
        if not normalized_workspace_id:
            return
        next_top_volume_limit = max(1, int(top_volume_limit or 200))
        workspaces = list(self._state.workspaces)
        changed = False
        for index, workspace in enumerate(workspaces):
            if workspace.workspace_id != normalized_workspace_id:
                continue
            if int(workspace.top_volume_limit) == next_top_volume_limit:
                return
            workspaces[index] = WorkspaceDefinition(
                workspace_id=workspace.workspace_id,
                title=workspace.title,
                top_volume_limit=next_top_volume_limit,
                nodes=workspace.nodes,
                sort_role=workspace.sort_role,
                sort_source_id=workspace.sort_source_id,
                sort_key=workspace.sort_key,
                sort_descending=workspace.sort_descending,
                column_stretch_by_role=dict(workspace.column_stretch_by_role),
                version=workspace.version,
            )
            changed = True
            break
        if not changed:
            return
        self._state = WorkspaceStorageState(
            schema_version=max(1, int(self._state.schema_version or 1)),
            active_workspace_id=self._state.active_workspace_id,
            workspaces=tuple(workspaces),
        )
        self._storage.save(self._state)
        self._logger.info(
            "workspace top saved | workspace_id=%s | top=%s",
            normalized_workspace_id,
            next_top_volume_limit,
        )

    def delete_workspace(self, workspace_id: str) -> None:
        normalized = str(workspace_id or "").strip()
        if not normalized:
            return
        workspaces = [item for item in self._state.workspaces if item.workspace_id != normalized]
        if len(workspaces) == len(self._state.workspaces):
            return
        session = self._sessions.pop(normalized, None)
        if session is not None:
            self._logger.info("deleting workspace session | workspace_id=%s", normalized)
            session.dispose()
        next_active_workspace_id = self._state.active_workspace_id
        if next_active_workspace_id == normalized:
            next_active_workspace_id = workspaces[0].workspace_id if workspaces else None
        self._state = WorkspaceStorageState(
            schema_version=max(1, int(self._state.schema_version or 1)),
            active_workspace_id=next_active_workspace_id,
            workspaces=tuple(workspaces),
        )
        self._storage.save(self._state)
        self._logger.info(
            "workspace deleted | workspace_id=%s | remaining=%s | active=%s",
            normalized,
            len(self._state.workspaces),
            next_active_workspace_id,
        )
        self.workspaces_changed.emit()
        self.active_workspace_changed.emit(next_active_workspace_id or "")

    def dispose(self) -> None:
        self._logger.info("manager dispose | sessions=%s", len(self._sessions))
        for session in list(self._sessions.values()):
            session.dispose()
        self._sessions.clear()


__all__ = ["WorkspaceManager"]
