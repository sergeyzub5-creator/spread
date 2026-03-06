import sys

from PySide6.QtWidgets import QApplication

from ui.theme import get_theme_manager
from ui.window import AppWindow


def main() -> int:
    app = QApplication(sys.argv)
    app.setApplicationName("Spread Sniper UI Shell")
    get_theme_manager().set_theme("dark")

    window = AppWindow()
    window.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
