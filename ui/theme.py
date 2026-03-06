from __future__ import annotations

from PySide6.QtCore import QObject, Signal


THEMES = {
    "dark": {
        "window_bg": "#07090d",
        "surface": "#10141a",
        "surface_alt": "#181e26",
        "border": "#34404a",
        "text_primary": "#e8eef2",
        "text_muted": "#a0b0c0",
        "accent": "#7aa2f7",
        "accent_bg": "#2a3a5a",
        "accent_bg_hover": "#3a4a7a",
        "success": "#7ec8a6",
        "warning": "#e5c07b",
        "danger": "#e06c75",
        "selection_bg_soft": "rgba(42, 58, 90, 72)",
        "tab_selected_bg": "rgba(20, 24, 28, 128)",
        "glow_a": "#19B8FF",
        "glow_b": "#00E0B8",
    },
    "steel": {
        "window_bg": "#1d2025",
        "surface": "#272c33",
        "surface_alt": "#323840",
        "border": "#5a626e",
        "text_primary": "#eef2f8",
        "text_muted": "#a9b4c4",
        "accent": "#8fa6c8",
        "accent_bg": "#3a4657",
        "accent_bg_hover": "#46556a",
        "success": "#69a997",
        "warning": "#b49a69",
        "danger": "#c2868e",
        "selection_bg_soft": "rgba(146, 160, 180, 38)",
        "tab_selected_bg": "rgba(78, 86, 98, 156)",
        "glow_a": "#6cb7ff",
        "glow_b": "#7de2cb",
    },
    "graphite_pro": {
        "window_bg": "#d6dbe3",
        "surface": "#cbd2dd",
        "surface_alt": "#b8c1cf",
        "border": "#8e9cad",
        "text_primary": "#1f2735",
        "text_muted": "#4d596c",
        "accent": "#4c79bd",
        "accent_bg": "#c6d5ec",
        "accent_bg_hover": "#b7c9e6",
        "success": "#418f7e",
        "warning": "#9f7633",
        "danger": "#b56a74",
        "selection_bg_soft": "rgba(76, 121, 189, 48)",
        "tab_selected_bg": "rgba(76, 121, 189, 102)",
        "glow_a": "#2f7de1",
        "glow_b": "#0aa985",
    },
}


class ThemeManager(QObject):
    theme_changed = Signal(str)

    def __init__(self) -> None:
        super().__init__()
        self._theme_name = "dark"

    @property
    def theme_name(self) -> str:
        return self._theme_name

    def set_theme(self, name: str) -> bool:
        normalized = str(name or "").strip().lower()
        if normalized not in THEMES or normalized == self._theme_name:
            return False
        self._theme_name = normalized
        self.theme_changed.emit(normalized)
        return True

    def colors(self) -> dict:
        return THEMES[self._theme_name]

    def color(self, key: str, default: str = "#000000") -> str:
        return self.colors().get(key, default)

    def available_themes(self) -> list[str]:
        return ["dark", "steel", "graphite_pro"]


_THEME_MANAGER = ThemeManager()


def get_theme_manager() -> ThemeManager:
    return _THEME_MANAGER


def theme_color(key: str, default: str = "#000000") -> str:
    return _THEME_MANAGER.color(key, default)


def build_app_stylesheet() -> str:
    c = get_theme_manager().colors()
    return f"""
        QMainWindow {{
            background-color: {c['window_bg']};
        }}
        QWidget {{
            background-color: {c['window_bg']};
            color: {c['text_primary']};
            font-family: 'Segoe UI', sans-serif;
        }}
        QTabWidget::pane {{
            background: qlineargradient(
                x1: 0, y1: 0, x2: 0, y2: 1,
                stop: 0 {c['surface_alt']},
                stop: 1 {c['surface']}
            );
            border-top: 1px solid {c['border']};
            border-left: none;
            border-right: none;
            border-bottom: none;
        }}
        QTabBar::tab {{
            background-color: {c['window_bg']};
            color: {c['text_muted']};
            border: 1px solid {c['border']};
            border-top-left-radius: 10px;
            border-top-right-radius: 10px;
            border-bottom: none;
            padding: 8px 18px;
            margin-right: 4px;
            font-weight: 700;
        }}
        QTabBar::tab:!selected:hover {{
            background-color: {c['surface']};
            color: {c['text_primary']};
            border-color: {c['accent']};
        }}
        QTabBar::tab:selected {{
            background-color: {c['surface_alt']};
            color: {c['accent']};
            border-color: {c['accent']};
        }}
        QMenu {{
            background-color: {c['surface']};
            border: 1px solid {c['border']};
            border-radius: 8px;
            padding: 6px;
        }}
        QMenu::item {{
            padding: 6px 10px;
            border-radius: 6px;
        }}
        QMenu::item:selected {{
            background-color: {c['selection_bg_soft']};
            color: {c['accent']};
        }}
    """


def button_style(kind: str = "primary") -> str:
    c = get_theme_manager().colors()
    palette = {
        "primary": (c["accent_bg"], c["accent"], c["text_primary"], c["accent_bg_hover"]),
        "success": ("rgba(40, 90, 58, 0.80)", c["success"], c["text_primary"], "rgba(52, 112, 72, 0.90)"),
        "warning": ("rgba(90, 74, 38, 0.80)", c["warning"], c["text_primary"], "rgba(112, 90, 48, 0.90)"),
        "secondary": (c["surface_alt"], c["border"], c["text_muted"], c["surface"]),
    }
    bg, border, text, hover = palette.get(kind, palette["primary"])
    return (
        f"QPushButton {{ background-color: {bg}; color: {text}; border: 1px solid {border}; "
        "border-radius: 10px; padding: 8px 14px; font-weight: 700; }}"
        f" QPushButton:hover {{ background-color: {hover}; }}"
    )
