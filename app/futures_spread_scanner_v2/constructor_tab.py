from __future__ import annotations

from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QFont
from PySide6.QtWidgets import (
    QCheckBox,
    QFrame,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QMenu,
    QPushButton,
    QScrollArea,
    QToolButton,
    QVBoxLayout,
    QWidget,
)

from app.futures_spread_scanner_v2.constructor_draft import ConstructorDraft, ConstructorRuntimeNode
from app.futures_spread_scanner_v2.catalog import RuntimeSelectionDraft
from app.futures_spread_scanner_v2.common.logger import get_logger
from app.futures_spread_scanner_v2.runtime.header_runtime import WorkspaceHeaderRuntime
from app.futures_spread_scanner_v2.runtime.starter_runtime import StarterPairsRuntime
from app.futures_spread_scanner_v2.views.header_view import WorkspaceHeaderRuntimeWidget
from app.futures_spread_scanner_v2.definitions import WorkspaceDefinition
from app.futures_spread_scanner_v2.common.i18n import tr
from app.futures_spread_scanner_v2.common.theme import THEMES


def _constructor_font(*, point_size: int = 9, bold: bool = False) -> QFont:
    font = QFont("Segoe UI")
    font.setPointSize(max(1, int(point_size)))
    font.setBold(bool(bold))
    return font


class _ConstructorInsertButton(QToolButton):
    def __init__(self, index: int, parent=None) -> None:
        super().__init__(parent)
        self.index = int(index)
        self.setFont(_constructor_font())
        self.setObjectName("v2ConstructorInsertBtn")
        self.setText("+")
        self.setToolButtonStyle(Qt.ToolButtonStyle.ToolButtonTextOnly)
        self.setFixedSize(42, 42)


class _ConstructorRuntimeCard(QFrame):
    def __init__(self, node: ConstructorRuntimeNode, draft: ConstructorDraft, index: int, parent=None) -> None:
        super().__init__(parent)
        self._node = node
        self._draft = draft
        self._index = int(index)
        self.setFont(_constructor_font())
        self.setObjectName("v2ConstructorRoleSlot")
        self.setMinimumWidth(156)
        self.setFixedHeight(340)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(8)

        self._title = QLabel(tr(self._draft.resolved_runtime_title_key(node)), self)
        self._title.setFont(_constructor_font(point_size=10, bold=True))
        self._title.setObjectName("v2ConstructorRoleTitle")
        layout.addWidget(self._title, 0)

        self._fields_wrap = QVBoxLayout()
        self._fields_wrap.setContentsMargins(0, 0, 0, 0)
        self._fields_wrap.setSpacing(8)
        layout.addLayout(self._fields_wrap, 0)

        self._detail = QLabel(self)
        self._detail.setFont(_constructor_font())
        self._detail.setObjectName("v2ConstructorRoleHint")
        self._detail.setWordWrap(True)
        layout.addWidget(self._detail, 1, Qt.AlignmentFlag.AlignTop)
        self._rebuild_fields()
        self._sync_texts()

    def retranslate_ui(self) -> None:
        self._title.setText(tr(self._draft.resolved_runtime_title_key(self._node)))
        self._rebuild_fields()
        self._sync_texts()

    def _sync_texts(self) -> None:
        self._detail.clear()

    def _open_class_menu(self, anchor: QWidget) -> None:
        menu = self._build_menu()
        allowed_classes = set(self._draft.available_classes_for_position(self._index))
        for runtime_class, title_key in (
            ("starter", "v2.constructor_role_starter"),
            ("base", "v2.constructor_role_base"),
            ("output", "v2.constructor_role_output"),
        ):
            if runtime_class not in allowed_classes:
                continue
            action = menu.addAction(tr(title_key))
            action.setFont(menu.font())
            action.triggered.connect(
                lambda _checked=False, node_id=self._node.node_id, runtime_class=runtime_class: self._draft.update_node_class(
                    node_id, runtime_class
                )
            )
        menu.exec(anchor.mapToGlobal(anchor.rect().bottomLeft()))

    def _build_menu(self) -> QMenu:
        menu = QMenu(self)
        menu.setFont(_constructor_font())
        return menu

    def _rebuild_fields(self) -> None:
        while self._fields_wrap.count():
            item = self._fields_wrap.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()

        selection = self._node.selection
        self._fields_wrap.addWidget(
            self._build_selector_button(
                label=tr("workspace.runtime_class"),
                value=self._class_value_text(selection),
                filled=bool(str(selection.runtime_class or "").strip()),
                on_click=self._open_class_menu,
            ),
            0,
        )

        runtime_class = str(selection.runtime_class or "").strip().lower()
        if runtime_class == "base":
            self._fields_wrap.addWidget(
                self._build_selector_button(
                    label=tr("workspace.runtime_exchange"),
                    value=self._exchange_value_text(selection),
                    filled=bool(str(selection.exchange_id or "").strip()),
                    on_click=self._open_exchange_menu,
                ),
                0,
            )
            if selection.exchange_id:
                self._fields_wrap.addWidget(
                    self._build_selector_button(
                        label=tr("workspace.runtime_asset_type"),
                        value=self._asset_type_value_text(selection),
                        filled=bool(str(selection.asset_type or "").strip()),
                        on_click=self._open_asset_type_menu,
                    ),
                    0,
                )
        elif runtime_class in {"starter", "output"}:
            self._fields_wrap.addWidget(
                self._build_selector_button(
                    label=tr("workspace.runtime_type"),
                    value=self._type_value_text(selection),
                    filled=bool(str(selection.selected_type or "").strip()),
                    on_click=self._open_type_menu,
                ),
                0,
            )

    def _build_selector_button(self, *, label: str, value: str, filled: bool, on_click) -> QWidget:
        wrap = QWidget(self)
        wrap.setObjectName("v2ConstructorFieldWrap")
        layout = QVBoxLayout(wrap)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(4)

        field_label = QLabel(label, wrap)
        field_label.setFont(_constructor_font(bold=True))
        field_label.setObjectName("v2ConstructorFieldLabel")
        layout.addWidget(field_label, 0)

        button = QPushButton(value, wrap)
        button.setFont(_constructor_font(bold=True))
        button.setObjectName("v2ConstructorSelectBtn")
        button.setProperty("pendingField", not filled)
        button.clicked.connect(lambda _checked=False, anchor=button, callback=on_click: callback(anchor))
        layout.addWidget(button, 0)
        return wrap

    def _class_value_text(self, selection: RuntimeSelectionDraft) -> str:
        options = dict(self._draft.catalog().class_options())
        return options.get(str(selection.runtime_class or "").strip(), tr("workspace.runtime_class"))

    def _exchange_value_text(self, selection: RuntimeSelectionDraft) -> str:
        options = dict(self._draft.catalog().exchange_options(runtime_class=selection.runtime_class))
        return options.get(str(selection.exchange_id or "").strip(), tr("workspace.runtime_exchange"))

    def _asset_type_value_text(self, selection: RuntimeSelectionDraft) -> str:
        options = dict(
            self._draft.catalog().asset_type_options(
                runtime_class=selection.runtime_class,
                exchange_id=selection.exchange_id,
            )
        )
        return options.get(str(selection.asset_type or "").strip(), tr("workspace.runtime_asset_type"))

    def _type_value_text(self, selection: RuntimeSelectionDraft) -> str:
        options = dict(self._draft.catalog().type_options(runtime_class=selection.runtime_class))
        return options.get(str(selection.selected_type or "").strip(), tr("workspace.runtime_type"))

    def _open_exchange_menu(self, anchor: QWidget) -> None:
        menu = self._build_menu()
        selection = self._node.selection
        for value, label in self._draft.catalog().exchange_options(runtime_class=selection.runtime_class):
            action = menu.addAction(label)
            action.setFont(menu.font())
            action.triggered.connect(
                lambda _checked=False, value=value: self._draft.update_node_selection(
                    self._node.node_id,
                    RuntimeSelectionDraft(
                        runtime_class=self._node.selection.runtime_class,
                        exchange_id=value,
                    ),
                )
            )
        menu.exec(anchor.mapToGlobal(anchor.rect().bottomLeft()))

    def _open_asset_type_menu(self, anchor: QWidget) -> None:
        menu = self._build_menu()
        selection = self._node.selection
        for value, label in self._draft.catalog().asset_type_options(
            runtime_class=selection.runtime_class,
            exchange_id=selection.exchange_id,
        ):
            action = menu.addAction(label)
            action.setFont(menu.font())
            action.triggered.connect(
                lambda _checked=False, value=value: self._draft.update_node_selection(
                    self._node.node_id,
                    RuntimeSelectionDraft(
                        runtime_class=self._node.selection.runtime_class,
                        exchange_id=self._node.selection.exchange_id,
                        asset_type=value,
                    ),
                )
            )
        menu.exec(anchor.mapToGlobal(anchor.rect().bottomLeft()))

    def _open_type_menu(self, anchor: QWidget) -> None:
        menu = self._build_menu()
        selection = self._node.selection
        for value, label in self._draft.catalog().type_options(runtime_class=selection.runtime_class):
            action = menu.addAction(label)
            action.setFont(menu.font())
            action.triggered.connect(
                lambda _checked=False, value=value: self._draft.update_node_selection(
                    self._node.node_id,
                    RuntimeSelectionDraft(
                        runtime_class=self._node.selection.runtime_class,
                        selected_type=value,
                    ),
                )
            )
        menu.exec(anchor.mapToGlobal(anchor.rect().bottomLeft()))


class FuturesSpreadConstructorTab(QWidget):
    apply_requested = Signal()
    delete_requested = Signal(str)
    cancel_requested = Signal(str)

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self._logger = get_logger("scanner.v2.constructor")
        self.setFont(_constructor_font())
        self._has_existing_layout = False
        self._editing_workspace_id: str | None = None
        self._draft = ConstructorDraft()
        self._starter_runtime = StarterPairsRuntime([])
        self._header_runtime = WorkspaceHeaderRuntime(self._starter_runtime, 200)
        self._draft.changed.connect(self._on_draft_changed)

        root = QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(12)

        self._header_widget = WorkspaceHeaderRuntimeWidget(self._header_runtime, self)
        root.addWidget(self._header_widget, 0)

        self._title_row = QWidget(self)
        self._title_row.setObjectName("v2ConstructorTitleRow")
        title_row_layout = QVBoxLayout(self._title_row)
        title_row_layout.setContentsMargins(0, 0, 0, 0)
        title_row_layout.setSpacing(4)

        self._title_field_label = QLabel(self._title_row)
        self._title_field_label.setFont(_constructor_font(bold=True))
        self._title_field_label.setObjectName("v2ConstructorFieldLabel")
        title_row_layout.addWidget(self._title_field_label, 0)

        self._title_input_row = QWidget(self._title_row)
        self._title_input_row.setObjectName("v2ConstructorTitleInputRow")
        title_input_row_layout = QHBoxLayout(self._title_input_row)
        title_input_row_layout.setContentsMargins(0, 0, 0, 0)
        title_input_row_layout.setSpacing(10)

        self._title_edit = QLineEdit(self._title_input_row)
        self._title_edit.setFont(_constructor_font())
        self._title_edit.setObjectName("v2ConstructorTitleEdit")
        self._title_edit.setFixedWidth(220)
        self._title_edit.textChanged.connect(self._on_title_changed)
        title_input_row_layout.addWidget(self._title_edit, 0)

        self._auto_title_check = QCheckBox(self._title_input_row)
        self._auto_title_check.setFont(_constructor_font())
        self._auto_title_check.setObjectName("v2ConstructorAutoTitleCheck")
        self._auto_title_check.toggled.connect(self._on_auto_title_toggled)
        title_input_row_layout.addWidget(self._auto_title_check, 0, Qt.AlignmentFlag.AlignVCenter)
        title_input_row_layout.addStretch(1)

        title_row_layout.addWidget(self._title_input_row, 0)
        root.addWidget(self._title_row, 0)

        self._surface = QFrame(self)
        self._surface.setObjectName("v2ConstructorSurface")
        surface_layout = QVBoxLayout(self._surface)
        surface_layout.setContentsMargins(16, 16, 16, 16)
        surface_layout.setSpacing(12)

        self._scroll_area = QScrollArea(self._surface)
        self._scroll_area.setObjectName("v2ConstructorScrollArea")
        self._scroll_area.setWidgetResizable(True)
        self._scroll_area.setFrameShape(QFrame.Shape.NoFrame)
        self._scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        self._scroll_area.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)

        self._canvas = QWidget(self._scroll_area)
        self._canvas.setObjectName("v2ConstructorCanvas")
        self._slots_row = QHBoxLayout(self._canvas)
        self._slots_row.setContentsMargins(0, 6, 0, 0)
        self._slots_row.setSpacing(12)
        self._scroll_area.setWidget(self._canvas)
        surface_layout.addWidget(self._scroll_area, 1)

        root.addWidget(self._surface, 1)

        actions = QHBoxLayout()
        actions.setContentsMargins(0, 0, 0, 0)
        actions.setSpacing(10)
        actions.addStretch(1)

        self._apply_btn = QPushButton(self)
        self._apply_btn.setFont(_constructor_font(bold=True))
        self._apply_btn.setObjectName("v2ConstructorPrimaryBtn")
        self._apply_btn.clicked.connect(self._on_apply_clicked)
        actions.addWidget(self._apply_btn, 0)

        self._cancel_btn = QPushButton(self)
        self._cancel_btn.setFont(_constructor_font(bold=True))
        self._cancel_btn.setObjectName("v2ConstructorDeleteBtn")
        self._cancel_btn.clicked.connect(self._on_cancel_clicked)
        actions.addWidget(self._cancel_btn, 0)

        self._delete_btn = QPushButton(self)
        self._delete_btn.setFont(_constructor_font(bold=True))
        self._delete_btn.setObjectName("v2ConstructorDeleteBtn")
        self._delete_btn.clicked.connect(self._confirm_delete)
        actions.addWidget(self._delete_btn, 0)

        root.addLayout(actions, 0)

        self.apply_theme()
        self.retranslate_ui()
        self._sync_actions()

    def retranslate_ui(self) -> None:
        self._header_widget.retranslate_ui()
        self._title_field_label.setText(tr("workspace.name"))
        self._title_edit.setPlaceholderText("")
        self._auto_title_check.setText(tr("v2.constructor_auto_name"))
        self._cancel_btn.setText(tr("common.cancel"))
        self._delete_btn.setText(tr("v2.constructor_delete"))
        self._rebuild_slots()
        self._sync_title_state()
        self._sync_actions()

    def apply_theme(self) -> None:
        d = THEMES["dark"]
        self.setObjectName("v2ConstructorTab")
        self.setStyleSheet(
            f"""
            QWidget#v2ConstructorTab {{
                background-color: {d["window_bg"]};
            }}
            QWidget#v2ConstructorTitleRow {{
                background: transparent;
            }}
            QFrame#v2ConstructorSurface {{
                background-color: {d["surface"]};
                border: 2px solid {d["border"]};
                border-radius: 16px;
            }}
            QScrollArea#v2ConstructorScrollArea, QWidget#v2ConstructorCanvas {{
                background: transparent;
                border: none;
            }}
            QWidget#v2ConstructorFieldWrap {{
                background: transparent;
                border: none;
            }}
            QCheckBox#v2ConstructorAutoTitleCheck {{
                color: {d["text_primary"]};
                background: transparent;
                spacing: 6px;
            }}
            QCheckBox#v2ConstructorAutoTitleCheck::indicator {{
                width: 16px;
                height: 16px;
                border-radius: 4px;
                border: 1px solid {d["border"]};
                background: transparent;
            }}
            QCheckBox#v2ConstructorAutoTitleCheck::indicator:hover {{
                border: 1px solid {d["accent"]};
            }}
            QCheckBox#v2ConstructorAutoTitleCheck::indicator:checked {{
                border: 1px solid {d["accent"]};
                background: {d["accent_bg"]};
            }}
            QLabel#v2ConstructorFieldLabel {{
                color: {d["text_muted"]};
                background: transparent;
            }}
            QLineEdit#v2ConstructorTitleEdit {{
                background-color: transparent;
                color: {d["text_primary"]};
                border: 1px solid {d["border"]};
                border-radius: 10px;
                padding: 6px 10px;
                min-height: 30px;
            }}
            QLineEdit#v2ConstructorTitleEdit:focus {{
                border: 1px solid {d["accent"]};
            }}
            QLineEdit#v2ConstructorTitleEdit[pendingField="true"] {{
                color: {d["accent"]};
                border: 1px solid {d["accent"]};
            }}
            QLineEdit#v2ConstructorTitleEdit::placeholder {{
                color: {d["text_muted"]};
            }}
            QFrame#v2ConstructorRoleSlot {{
                background-color: {d["surface_alt"]};
                border: 2px dashed {d["border"]};
                border-radius: 14px;
            }}
            QPushButton#v2ConstructorSelectBtn {{
                background-color: transparent;
                color: {d["text_primary"]};
                border: 1px solid {d["border"]};
                border-radius: 10px;
                padding: 6px 10px;
                text-align: left;
                min-height: 30px;
            }}
            QPushButton#v2ConstructorSelectBtn:hover {{
                border: 1px solid {d["accent"]};
                background-color: transparent;
            }}
            QPushButton#v2ConstructorSelectBtn:pressed {{
                background-color: transparent;
            }}
            QPushButton#v2ConstructorSelectBtn[pendingField="true"] {{
                color: {d["accent"]};
                border: 1px solid {d["accent"]};
            }}
            QPushButton#v2ConstructorSelectBtn[pendingField="true"]:hover {{
                border: 1px solid {d["accent"]};
                background-color: transparent;
            }}
            QLabel#v2ConstructorRoleTitle {{
                color: {d["text_primary"]};
                background: transparent;
            }}
            QToolButton#v2ConstructorInsertBtn {{
                background-color: {d["surface_alt"]};
                color: {d["text_primary"]};
                border: 2px dashed {d["border"]};
                border-radius: 12px;
            }}
            QToolButton#v2ConstructorInsertBtn:hover {{
                border: 2px dashed {d["accent"]};
                color: {d["accent"]};
            }}
            QLabel#v2ConstructorRoleHint {{
                color: {d["text_muted"]};
                background: transparent;
            }}
            QPushButton#v2ConstructorDeleteBtn {{
                background-color: {d["surface"]};
                color: {d["text_primary"]};
                border: 1px solid {d["border"]};
                border-radius: 10px;
                padding: 6px 14px;
                min-height: 34px;
            }}
            QPushButton#v2ConstructorDeleteBtn:hover {{
                background-color: {d["surface_alt"]};
            }}
            QPushButton#v2ConstructorPrimaryBtn {{
                background-color: {d["accent_bg"]};
                color: {d["text_primary"]};
                border: 1px solid {d["accent"]};
                border-radius: 10px;
                padding: 6px 16px;
                min-height: 34px;
            }}
            QPushButton#v2ConstructorPrimaryBtn:hover:!disabled {{
                background-color: {d["accent_bg_hover"]};
            }}
            QPushButton#v2ConstructorPrimaryBtn:pressed:!disabled {{
                padding-top: 7px;
                padding-bottom: 5px;
            }}
            QPushButton#v2ConstructorPrimaryBtn:disabled {{
                background-color: {d["surface_alt"]};
                color: {d["text_muted"]};
                border: 1px solid {d["border"]};
            }}
            """
        )

    def set_constructor_counts(self, *, starter_count: int, base_count: int, output_count: int = 0) -> None:
        self._sync_actions()

    def set_has_existing_layout(self, value: bool) -> None:
        self._has_existing_layout = bool(value)
        self._sync_actions()

    def _is_valid(self) -> bool:
        return self._draft.is_valid()

    def _has_any_content(self) -> bool:
        return self._draft.has_any_content()

    def _primary_action_text(self) -> str:
        if self._has_existing_layout or self._has_any_content():
            return tr("workspace.apply")
        return tr("workspace.create")

    def _sync_actions(self) -> None:
        self._apply_btn.setText(self._primary_action_text())
        self._apply_btn.setEnabled(self._is_valid())
        self._cancel_btn.setVisible(bool(self._editing_workspace_id))

    def _on_title_changed(self, value: str) -> None:
        if self._draft.auto_title_enabled():
            return
        self._draft.set_title(value)

    def _on_auto_title_toggled(self, checked: bool) -> None:
        self._draft.set_auto_title_enabled(checked)

    def _sync_title_state(self) -> None:
        auto_title = self._draft.auto_title_enabled()
        self._auto_title_check.blockSignals(True)
        self._auto_title_check.setChecked(auto_title)
        self._auto_title_check.blockSignals(False)

        if auto_title:
            display_title = self._draft.generated_title()
        else:
            display_title = self._draft.title()

        self._title_edit.blockSignals(True)
        self._title_edit.setText(display_title)
        self._title_edit.setReadOnly(auto_title)
        self._title_edit.setProperty("pendingField", not self._draft.title_ready())
        self._title_edit.style().unpolish(self._title_edit)
        self._title_edit.style().polish(self._title_edit)
        self._title_edit.blockSignals(False)

    def _confirm_delete(self) -> None:
        result = QMessageBox.question(
            self,
            tr("v2.constructor_delete_confirm_title"),
            tr("v2.constructor_delete_confirm_message"),
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if result != QMessageBox.StandardButton.Yes:
            return
        if self._editing_workspace_id:
            self._logger.info("constructor delete requested | workspace_id=%s", self._editing_workspace_id)
            self.delete_requested.emit(self._editing_workspace_id)
            return
        self._has_existing_layout = False
        self._draft.clear()
        self._header_runtime.set_top_volume_limit(200)
        self._sync_actions()
        self._logger.info("constructor draft cleared by delete")

    def _on_apply_clicked(self) -> None:
        if not self._is_valid():
            self._logger.info("constructor apply ignored | valid=false")
            return
        self._has_existing_layout = True
        self._sync_actions()
        self._logger.info(
            "constructor apply emit | title=%s | auto=%s | top=%s | nodes=%s",
            self._draft.effective_title(),
            self._draft.auto_title_enabled(),
            self._header_runtime.top_volume_limit(),
            len(self._draft.nodes()),
        )
        self.apply_requested.emit()

    def _on_cancel_clicked(self) -> None:
        if not self._editing_workspace_id:
            return
        self._logger.info("constructor cancel requested | workspace_id=%s", self._editing_workspace_id)
        self.cancel_requested.emit(self._editing_workspace_id)

    def _on_insert_clicked(self, index: int) -> None:
        menu = QMenu(self)
        font = QFont("Segoe UI")
        font.setPointSize(9)
        menu.setFont(font)
        allowed_classes = set(self._draft.available_classes_for_position(index))
        for runtime_class, title_key in (
            ("starter", "v2.constructor_role_starter"),
            ("base", "v2.constructor_role_base"),
            ("output", "v2.constructor_role_output"),
        ):
            if runtime_class not in allowed_classes:
                continue
            action = menu.addAction(tr(title_key))
            action.setFont(menu.font())
            action.triggered.connect(
                lambda _checked=False, insert_at=index, runtime_class=runtime_class: self._draft.insert_node(
                    insert_at, runtime_class
                )
            )
        sender = self.sender()
        if isinstance(sender, QWidget):
            menu.exec(sender.mapToGlobal(sender.rect().bottomLeft()))
        else:
            menu.exec(self.mapToGlobal(self.rect().center()))

    def _rebuild_slots(self) -> None:
        while self._slots_row.count():
            item = self._slots_row.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()
        nodes = self._draft.nodes()
        for index in range(len(nodes) + 1):
            insert_btn = _ConstructorInsertButton(index, self._canvas)
            insert_btn.clicked.connect(
                lambda _checked=False, insert_at=index, anchor=insert_btn: self._open_insert_menu(anchor, insert_at)
            )
            self._slots_row.addWidget(insert_btn, 0, Qt.AlignmentFlag.AlignTop)
            if index >= len(nodes):
                continue
            card = _ConstructorRuntimeCard(nodes[index], self._draft, index, self._canvas)
            self._slots_row.addWidget(card, 0, Qt.AlignmentFlag.AlignTop)
        self._slots_row.addStretch(1)

    def _open_insert_menu(self, anchor: QWidget, index: int) -> None:
        menu = QMenu(self)
        font = QFont("Segoe UI")
        font.setPointSize(9)
        menu.setFont(font)
        allowed_classes = set(self._draft.available_classes_for_position(index))
        for runtime_class, title_key in (
            ("starter", "v2.constructor_role_starter"),
            ("base", "v2.constructor_role_base"),
            ("output", "v2.constructor_role_output"),
        ):
            if runtime_class not in allowed_classes:
                continue
            action = menu.addAction(tr(title_key))
            action.setFont(menu.font())
            action.triggered.connect(
                lambda _checked=False, insert_at=index, runtime_class=runtime_class: self._draft.insert_node(
                    insert_at, runtime_class
                )
            )
        menu.exec(anchor.mapToGlobal(anchor.rect().bottomLeft()))

    def _on_draft_changed(self) -> None:
        self._rebuild_slots()
        self._sync_title_state()
        self._sync_actions()

    def draft(self) -> ConstructorDraft:
        return self._draft

    def editing_workspace_id(self) -> str | None:
        return self._editing_workspace_id

    def is_edit_mode(self) -> bool:
        return bool(self._editing_workspace_id)

    def top_volume_limit(self) -> int:
        return self._header_runtime.top_volume_limit()

    def reset_draft(self) -> None:
        self._has_existing_layout = False
        self._editing_workspace_id = None
        self._draft.clear()
        self._header_runtime.set_top_volume_limit(200)
        self._sync_title_state()
        self._sync_actions()
        self._logger.info("constructor draft reset")

    def load_definition(self, definition: WorkspaceDefinition) -> None:
        selections: list[RuntimeSelectionDraft] = [
            self._draft.catalog().draft_for_runtime_id(node.runtime_id) for node in definition.nodes
        ]
        self._draft.replace_nodes(selections)
        generated_title = self._draft.generated_title().strip()
        normalized_title = str(definition.title or "").strip()
        auto_title = bool(generated_title) and normalized_title == generated_title
        self._draft.replace_nodes(
            selections,
            title="" if auto_title else normalized_title,
            auto_title=auto_title,
        )
        self._editing_workspace_id = definition.workspace_id
        self._has_existing_layout = True
        self._header_runtime.set_top_volume_limit(definition.top_volume_limit)
        self._sync_title_state()
        self._sync_actions()
        self._logger.info(
            "constructor definition loaded | workspace_id=%s | title=%s | nodes=%s | auto_title=%s",
            definition.workspace_id,
            definition.title,
            len(definition.nodes),
            auto_title,
        )

    def showEvent(self, event) -> None:  # type: ignore[override]
        super().showEvent(event)
        if self._slots_row.count() == 0:
            self._rebuild_slots()


__all__ = ["FuturesSpreadConstructorTab"]
