from __future__ import annotations

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import QFrame, QHBoxLayout, QLabel, QPushButton, QVBoxLayout, QWidget

from ui.theme import button_style, theme_color


class PlaceholderTab(QWidget):
    action_triggered = Signal(str)

    def __init__(self, title: str, subtitle: str, actions: list[str], parent=None) -> None:
        super().__init__(parent)
        self._title = title
        self._subtitle = subtitle
        self._actions = list(actions)
        self._buttons: list[QPushButton] = []
        self._build_ui()
        self.apply_theme()

    def _build_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(14, 14, 14, 14)
        root.setSpacing(12)

        hero = QFrame()
        hero.setObjectName("heroCard")
        hero_layout = QVBoxLayout(hero)
        hero_layout.setContentsMargins(18, 18, 18, 18)
        hero_layout.setSpacing(8)

        title = QLabel(self._title)
        title.setObjectName("heroTitle")
        hero_layout.addWidget(title)

        subtitle = QLabel(self._subtitle)
        subtitle.setObjectName("heroSubtitle")
        subtitle.setWordWrap(True)
        hero_layout.addWidget(subtitle)

        actions_row = QHBoxLayout()
        actions_row.setSpacing(10)
        for index, label in enumerate(self._actions):
            button = QPushButton(label)
            button.clicked.connect(lambda _checked=False, text=label: self._emit_action(text))
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

            chip = QLabel(f"BLOCK {idx + 1}")
            chip.setObjectName("miniChip")
            panel_layout.addWidget(chip, 0, Qt.AlignmentFlag.AlignLeft)

            panel_title = QLabel(f"Interactive placeholder {idx + 1}")
            panel_title.setObjectName("miniTitle")
            panel_layout.addWidget(panel_title)

            panel_text = QLabel("This area is intentionally lightweight and ready for a full rewrite.")
            panel_text.setWordWrap(True)
            panel_text.setObjectName("miniText")
            panel_layout.addWidget(panel_text)

            panel_layout.addStretch(1)
            grid.addWidget(panel, 1)
        root.addLayout(grid)

        self.state_label = QLabel("Ready")
        self.state_label.setObjectName("stateLabel")
        root.addWidget(self.state_label)

        root.addStretch(1)

    def _emit_action(self, label: str) -> None:
        self.state_label.setText(f"Clicked: {label}")
        self.action_triggered.emit(label)

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
