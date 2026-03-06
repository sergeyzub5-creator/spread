from __future__ import annotations

import json
import time
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from app.core.models.instrument import InstrumentId


class MarketCapRanker:
    API_URL = "https://api.coingecko.com/api/v3/coins/markets"
    FALLBACK_BASE_ASSETS = (
        "BTC",
        "ETH",
        "XRP",
        "BNB",
        "SOL",
        "USDC",
        "DOGE",
        "ADA",
        "TRX",
        "LINK",
        "AVAX",
    )

    def __init__(self, ttl_seconds: float = 1800.0, timeout_seconds: float = 8.0) -> None:
        self._ttl_seconds = float(ttl_seconds)
        self._timeout_seconds = float(timeout_seconds)
        self._cached_ranks: dict[str, int] = {}
        self._cached_at = 0.0

    def rank(self, instruments: list[InstrumentId]) -> list[InstrumentId]:
        if not instruments:
            return []
        ranks = self._coin_ranks()
        return sorted(instruments, key=lambda instrument: self._sort_key(instrument, ranks))

    def _coin_ranks(self) -> dict[str, int]:
        now = time.time()
        if self._cached_ranks and (now - self._cached_at) < self._ttl_seconds:
            return self._cached_ranks
        try:
            params = urlencode(
                {
                    "vs_currency": "usd",
                    "order": "market_cap_desc",
                    "per_page": 50,
                    "page": 1,
                    "sparkline": "false",
                }
            )
            request = Request(
                f"{self.API_URL}?{params}",
                headers={"User-Agent": "spread-sniper-ui-shell/1.0"},
            )
            with urlopen(request, timeout=self._timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8"))
            ranks: dict[str, int] = {}
            if isinstance(payload, list):
                for index, item in enumerate(payload, start=1):
                    symbol = str(item.get("symbol", "")).strip().upper()
                    if symbol and symbol not in ranks:
                        ranks[symbol] = index
            if ranks:
                self._cached_ranks = ranks
                self._cached_at = now
                return ranks
        except Exception:
            pass

        fallback = {symbol: index for index, symbol in enumerate(self.FALLBACK_BASE_ASSETS, start=1)}
        self._cached_ranks = fallback
        self._cached_at = now
        return fallback

    @staticmethod
    def _sort_key(instrument: InstrumentId, ranks: dict[str, int]) -> tuple[int, int, str]:
        base_asset = str(instrument.spec.base_asset or "").strip().upper()
        quote_asset = str(instrument.spec.quote_asset or instrument.spec.settle_asset or "").strip().upper()
        rank = ranks.get(base_asset, 10_000)
        quote_priority = {
            "USDT": 0,
            "USDC": 1,
            "USD": 2,
            "BTC": 3,
            "ETH": 4,
        }.get(quote_asset, 10)
        return (rank, quote_priority, instrument.symbol)
