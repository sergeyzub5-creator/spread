from __future__ import annotations


CHART_EXCHANGE_ALIASES = {
    "kukoin": "kucoin",
    "gateio": "gate",
    "gate.io": "gate",
    "okex": "okx",
}


CHART_EXCHANGE_CATALOG = {
    "binance": {
        "code": "binance",
        "title": "Binance Futures",
        "base_name": "Binance",
        "short": "BN",
        "color": "#F3BA2F",
    },
    "bitget": {
        "code": "bitget",
        "title": "Bitget Futures",
        "base_name": "Bitget",
        "short": "BG",
        "color": "#00C1D4",
    },
    "bybit": {
        "code": "bybit",
        "title": "Bybit Futures",
        "base_name": "Bybit",
        "short": "BY",
        "color": "#F7A600",
    },
    "okx": {
        "code": "okx",
        "title": "OKX Futures",
        "base_name": "OKX",
        "short": "OK",
        "color": "#111111",
    },
    "mexc": {
        "code": "mexc",
        "title": "MEXC Futures",
        "base_name": "MEXC",
        "short": "MX",
        "color": "#2EC5B6",
    },
    "kucoin": {
        "code": "kucoin",
        "title": "KuCoin Futures",
        "base_name": "KuCoin",
        "short": "KC",
        "color": "#1FC7A3",
    },
    "gate": {
        "code": "gate",
        "title": "Gate Futures",
        "base_name": "Gate",
        "short": "GT",
        "color": "#2F54EB",
    },
    "bingx": {
        "code": "bingx",
        "title": "BingX Futures",
        "base_name": "BingX",
        "short": "BX",
        "color": "#005BFF",
    },
}


CHART_EXCHANGE_ORDER = ["binance", "bitget", "bybit", "okx", "mexc", "kucoin", "gate", "bingx"]


def normalize_chart_exchange_code(exchange_code: str | None) -> str:
    if not exchange_code:
        return "unknown"
    code = str(exchange_code).strip().lower()
    return CHART_EXCHANGE_ALIASES.get(code, code)


def get_chart_exchange_meta(exchange_code: str | None) -> dict:
    code = normalize_chart_exchange_code(exchange_code)
    if code in CHART_EXCHANGE_CATALOG:
        return dict(CHART_EXCHANGE_CATALOG[code])
    return {
        "code": code,
        "title": "Unknown Exchange",
        "base_name": "Unknown",
        "short": "EX",
        "color": "#6C7A89",
    }
