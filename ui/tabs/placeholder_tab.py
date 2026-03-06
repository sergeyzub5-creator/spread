from __future__ import annotations

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import QFrame, QHBoxLayout, QLabel, QPushButton, QVBoxLayout, QWidget

from ui.i18n import tr
from ui.theme import button_style, theme_color


class PlaceholderTab(QWidget):
    action_triggered = Signal(str)

    def __init__(self, title_key: str, subtitle_key: str, action_keys: list[str], parent=None) -> None:
        super().__init__(parent)
        self._title_key = title_key
        self._subtitle_key = subtitle_key
        self._action_keys = list(action_keys)
        self._buttons: list[QPushButton] = []
        self._hero_title_label: QLabel | None = None
        self._hero_subtitle_label: QLabel | None = None
        self._mini_chip_labels: list[QLabel] = []
        self._mini_title_labels: list[QLabel] = []
        self._mini_text_labels: list[QLabel] = []
        self._build_ui()
        self.apply_theme()
        self.retranslate_ui()

    def _build_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(14, 14, 14, 14)
        root.setSpacing(12)

        hero = QFrame()
        hero.setObjectName("heroCard")
        hero_layout = QVBoxLayout(hero)
        hero_layout.setContentsMargins(18, 18, 18, 18)
        hero_layout.setSpacing(8)

        title = QLabel()
        title.setObjectName("heroTitle")
        self._hero_title_label = title
        hero_layout.addWidget(title)

        subtitle = QLabel()
        subtitle.setObjectName("heroSubtitle")
        subtitle.setWordWrap(True)
        self._hero_subtitle_label = subtitle
        hero_layout.addWidget(subtitle)

        actions_row = QHBoxLayout()
        actions_row.setSpacing(10)
        for index, action_key in enumerate(self._action_keys):
            button = QPushButton()
            button.clicked.connect(lambda _checked=False, key=action_key: self._emit_action(tr(key)))
            button.setCursor(Qt.CursorShape.PointingHandCursor)
            button.setStyleSheet(button_style("primary" if index == 0 else "secondary"))
            self._buttons.append(button)
            actions_row.addWidget(button)
        actions_row.addStretch(1)
        hero_layout.addLayout(actions_row)
        root.addWidget(hero)

        grid = QHBoxLayout()
        grid.setSpacing(12)
        for idx in range(3):
            panel = QFrame()
            panel.setObjectName("miniCard")
            panel_layout = QVBoxLayout(panel)
            panel_layout.setContentsMargins(14, 14, 14, 14)
            panel_layout.setSpacing(6)

            chip = QLabel()
            chip.setObjectName("miniChip")
            self._mini_chip_labels.append(chip)
            panel_layout.addWidget(chip, 0, Qt.AlignmentFlag.AlignLeft)

            panel_title = QLabel()
            panel_title.setObjectName("miniTitle")
            self._mini_title_labels.append(panel_title)
            panel_layout.addWidget(panel_title)

            panel_text = QLabel()
            panel_text.setWordWrap(True)
            panel_text.setObjectName("miniText")
            self._mini_text_labels.append(panel_text)
            panel_layout.addWidget(panel_text)

            panel_layout.addStretch(1)
            grid.addWidget(panel, 1)
        root.addLayout(grid)

        self.state_label = QLabel()
        self.state_label.setObjectName("stateLabel")
        root.addWidget(self.state_label)

        root.addStretch(1)

    def _emit_action(self, label: str) -> None:
        self.state_label.setText(tr("placeholder.clicked", label=label))
        self.action_triggered.emit(label)

    def retranslate_ui(self) -> None:
        if self._hero_title_label is not None:
            self._hero_title_label.setText(tr(self._title_key))
        if self._hero_subtitle_label is not None:
            self._hero_subtitle_label.setText(tr(self._subtitle_key))
        for button, action_key in zip(self._buttons, self._action_keys):
            button.setText(tr(action_key))
        for index, label in enumerate(self._mini_chip_labels, start=1):
            label.setText(tr("placeholder.block", index=index))
        for index, label in enumerate(self._mini_title_labels, start=1):
            label.setText(tr("placeholder.block_title", index=index))
        for label in self._mini_text_labels:
            label.setText(tr("placeholder.block_text"))
        self.state_label.setText(tr("placeholder.ready"))

    def apply_theme(self) -> None:
        self.setStyleSheet(
            f"""
            QFrame#heroCard {{
                background: qlineargradient(
                    x1: 0, y1: 0, x2: 1, y2: 1,
                    stop: 0 {theme_color('surface_alt')},
                    stop: 1 {theme_color('surface')}
                );
                border: 1px solid {theme_color('border')};
                border-radius: 18px;
            }}
            QFrame#miniCard {{
                background-color: {theme_color('surface')};
                border: 1px solid {theme_color('border')};
                border-radius: 16px;
            }}
            QLabel#heroTitle {{
                color: {theme_color('text_primary')};
                font-size: 26px;
                font-weight: 800;
            }}
            QLabel#heroSubtitle {{
                color: {theme_color('text_muted')};
                font-size: 13px;
            }}
            QLabel#miniChip {{
                color: {theme_color('accent')};
                font-size: 11px;
                font-weight: 800;
                padding: 4px 8px;
                background-color: {theme_color('selection_bg_soft')};
                border-radius: 10px;
            }}
            QLabel#miniTitle {{
                color: {theme_color('text_primary')};
                font-size: 15px;
                font-weight: 700;
            }}
            QLabel#miniText {{
                color: {theme_color('text_muted')};
                font-size: 12px;
            }}
            QLabel#stateLabel {{
                color: {theme_color('accent')};
                font-size: 12px;
                font-weight: 700;
                padding: 6px 2px;
            }}
            """
        )
