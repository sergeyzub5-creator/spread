from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from pathlib import Path

from PySide6.QtCore import Qt
from PySide6.QtGui import QColor, QIcon, QImage, QPainter, QPixmap


_EXCHANGE_STYLE = {
    "binance": {"label": "B", "color": "#f0b90b"},
    "bybit": {"label": "Y", "color": "#f7a600"},
    "bitget": {"label": "G", "color": "#00c2ff"},
    "okx": {"label": "O", "color": "#111111"},
    "mexc": {"label": "M", "color": "#4f7cff"},
}
CHART_EXCHANGE_MENU_ITEMS = [
    ("binance", "Binance"),
    ("bybit", "Bybit"),
    ("bitget", "Bitget"),
    ("okx", "OKX"),
    ("mexc", "MEXC"),
]
_ASSETS_DIR = Path(__file__).resolve().parents[1] / "assets" / "logos" / "exchanges"
_SUPPORTED_EXTENSIONS = (".png", ".svg", ".ico", ".webp", ".jpg", ".jpeg")


def _resolve_logo_path(exchange_code: str) -> Path | None:
    code = str(exchange_code or "").strip().lower()
    for ext in _SUPPORTED_EXTENSIONS:
        path = _ASSETS_DIR / f"{code}{ext}"
        if path.exists():
            return path
    return None


def _trim_transparent(image: QImage) -> QImage:
    if image.isNull() or not image.hasAlphaChannel():
        return image
    img = image.convertToFormat(QImage.Format.Format_ARGB32)
    width = img.width()
    height = img.height()
    min_x, min_y, max_x, max_y = width, height, -1, -1
    for y in range(height):
        for x in range(width):
            alpha = (img.pixel(x, y) >> 24) & 0xFF
            if alpha > 0:
                min_x = min(min_x, x)
                min_y = min(min_y, y)
                max_x = max(max_x, x)
                max_y = max(max_y, y)
    if max_x < min_x or max_y < min_y:
        return img
    return img.copy(min_x, min_y, max_x - min_x + 1, max_y - min_y + 1)


def _load_logo_pixmap(exchange_code: str, size: int) -> QPixmap | None:
    logo_path = _resolve_logo_path(exchange_code)
    if logo_path is None:
        return None
    pixmap = QPixmap(str(logo_path))
    if pixmap.isNull():
        return None
    trimmed = _trim_transparent(pixmap.toImage())
    if not trimmed.isNull():
        pixmap = QPixmap.fromImage(trimmed)
    scaled = pixmap.scaled(size, size, Qt.AspectRatioMode.IgnoreAspectRatio, Qt.TransformationMode.SmoothTransformation)
    if scaled.isNull():
        return None
    return scaled


def build_local_exchange_icon(exchange_code: str, size: int = 18) -> QIcon:
    code = str(exchange_code or "").strip().lower()
    logo = _load_logo_pixmap(code, size)
    if logo is not None:
        return QIcon(logo)
    meta = _EXCHANGE_STYLE.get(code, {"label": code[:1].upper() or "?", "color": "#58657a"})

    pixmap = QPixmap(size, size)
    pixmap.fill(Qt.GlobalColor.transparent)

    painter = QPainter(pixmap)
    painter.setRenderHint(QPainter.RenderHint.Antialiasing)
    painter.setPen(Qt.PenStyle.NoPen)
    painter.setBrush(QColor(meta["color"]))
    painter.drawRoundedRect(0, 0, size, size, 5, 5)
    painter.setPen(QColor("#0b0b0c" if code == "binance" else "#ffffff"))
    font = painter.font()
    font.setFamily("Segoe UI")
    font.setBold(True)
    font.setPointSize(max(7, int(size * 0.46)))
    painter.setFont(font)
    painter.drawText(pixmap.rect(), Qt.AlignmentFlag.AlignCenter, meta["label"])
    painter.end()
    return QIcon(pixmap)


def parse_daily_volume_threshold(text: object) -> int | None:
    raw = str(text or "").strip()
    if not raw:
        return None
    normalized = raw.replace("\u00a0", "").replace(" ", "").replace(",", ".")
    if re.fullmatch(r"[0-9]+\.?[0-9]*", normalized) or re.fullmatch(r"[0-9]*\.[0-9]+", normalized):
        try:
            return int(Decimal(normalized))
        except (InvalidOperation, ValueError):
            return None
    match = re.match(r"^([0-9]*\.?[0-9]+)\s*([a-zA-Zа-яА-ЯёЁ]*)$", normalized)
    if not match:
        return None

    number_part, suffix = match.group(1), match.group(2).lower()
    try:
        value = Decimal(number_part)
    except InvalidOperation:
        return None

    multiplier = Decimal("1")
    if suffix in ("k", "к", "тыс", "тысяч", "thousand"):
        multiplier = Decimal("1000")
    elif suffix in ("m", "м", "млн", "million", "mln"):
        multiplier = Decimal("1000000")
    elif suffix in ("b", "в", "млрд", "billion", "bln"):
        multiplier = Decimal("1000000000")
    elif suffix:
        return None

    try:
        return int(value * multiplier)
    except (ValueError, OverflowError):
        return None


def _trim_float(value: str) -> str:
    trimmed = value.rstrip("0").rstrip(".")
    return trimmed if trimmed else "0"


def format_volume_threshold(value: int | None) -> str:
    if value is None:
        return ""
    if value < 0:
        return str(value)
    if value >= 1_000_000_000:
        scaled = value / 1_000_000_000
        return (f"{int(round(scaled))}" if scaled >= 100 else _trim_float(f"{scaled:.2f}")) + "B"
    if value >= 1_000_000:
        scaled = value / 1_000_000
        return (f"{int(round(scaled))}" if scaled >= 100 else _trim_float(f"{scaled:.2f}")) + "M"
    if value >= 1_000:
        scaled = value / 1_000
        return (f"{int(round(scaled))}" if scaled >= 100 else _trim_float(f"{scaled:.2f}")) + "K"
    return str(value)
