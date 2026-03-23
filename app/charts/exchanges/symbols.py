from __future__ import annotations

import re


BINANCE_QUOTE = "USDT"
BINANCE_DELIVERY_SUFFIX_PATTERN = re.compile(r".+_[0-9]{6}$")


def binance_to_native(canonical: str) -> str:
    symbol = str(canonical or "").strip().upper().replace("-", "")
    if symbol and not symbol.endswith(BINANCE_QUOTE):
        symbol = f"{symbol}{BINANCE_QUOTE}"
    return symbol


def binance_from_native(raw: str) -> str | None:
    symbol = str(raw or "").strip().upper()
    if not symbol.endswith(BINANCE_QUOTE) or "_" in symbol:
        return None
    if not re.fullmatch(r"[A-Z0-9]+USDT", symbol):
        return None
    return symbol


def binance_is_delivery_symbol(symbol: str) -> bool:
    return bool(BINANCE_DELIVERY_SUFFIX_PATTERN.fullmatch(str(symbol or "").strip().upper()) or "_" in str(symbol or ""))


BYBIT_QUOTE = "USDT"


def bybit_to_native(canonical: str) -> str:
    symbol = str(canonical or "").strip().upper().replace("-", "")
    if symbol and not symbol.endswith(BYBIT_QUOTE):
        symbol = f"{symbol}{BYBIT_QUOTE}"
    return symbol


def bybit_from_native(raw: str, *, quote_suffix: str = BYBIT_QUOTE) -> str | None:
    symbol = str(raw or "").strip().upper()
    if quote_suffix and not symbol.endswith(quote_suffix):
        return None
    if not re.fullmatch(r"[A-Z0-9]+USDT", symbol):
        return None
    return symbol


BITGET_QUOTE = "USDT"


def bitget_to_native(canonical: str) -> str:
    symbol = str(canonical or "").strip().upper().replace("-", "")
    if symbol and not symbol.endswith(BITGET_QUOTE):
        symbol = f"{symbol}{BITGET_QUOTE}"
    return symbol


def bitget_from_native(raw: str) -> str | None:
    symbol = str(raw or "").strip().upper()
    if not symbol.endswith(BITGET_QUOTE):
        return None
    if not re.fullmatch(r"[A-Z0-9]+USDT", symbol):
        return None
    return symbol


OKX_SWAP_SUFFIX = "-USDT-SWAP"


def okx_to_native(canonical: str) -> str:
    symbol = str(canonical or "").strip().upper().replace("-", "")
    if symbol.endswith("USDT") and len(symbol) > 4:
        return f"{symbol[:-4]}-USDT-SWAP"
    return symbol


def okx_from_native(raw: str) -> str | None:
    inst_id = str(raw or "").strip().upper()
    if not inst_id.endswith(OKX_SWAP_SUFFIX):
        return None
    base = inst_id[: -len(OKX_SWAP_SUFFIX)]
    if not base or "-" in base:
        return None
    if not re.fullmatch(r"[A-Z0-9]+", base):
        return None
    return f"{base}USDT"


def okx_is_usdt_swap_inst_id(inst_id: str) -> bool:
    return str(inst_id or "").strip().upper().endswith(OKX_SWAP_SUFFIX)


def mexc_canonical_symbol(symbol: object) -> str:
    text = str(symbol or "").strip().upper()
    if not text:
        return ""
    return text.replace("_", "").replace("-", "")
