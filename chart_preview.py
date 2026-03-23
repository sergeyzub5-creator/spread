from __future__ import annotations

import sys

from PySide6.QtWidgets import QApplication

from app.charts.ui import PriceChartWindow


def main() -> int:
    app = QApplication.instance() or QApplication(sys.argv)
    window = PriceChartWindow()
    window.show()
    window.raise_()
    window.activateWindow()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
