"""UI integration layer for the new backend architecture."""

__all__ = ["AppWindow"]


def __getattr__(name: str):
    if name == "AppWindow":
        from app.ui.window import AppWindow

        return AppWindow
    raise AttributeError(name)

