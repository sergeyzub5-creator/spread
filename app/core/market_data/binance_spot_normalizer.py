from __future__ import annotations

from decimal import Decimal
from typing import Any

from app.core.market_data.normalizer import QuoteNormalizer
from app.core.models.instrument import InstrumentId
from app.core.models.market_data import QuoteL1


class BinanceSpotQuoteNormalizer(QuoteNormalizer):
    """Normalizes Binance spot bookTicker payloads into QuoteL1."""

    def normalize_l1(self, instrument: InstrumentId, payload: dict[str, Any], ts_local: int) -> QuoteL1:
        return QuoteL1(
            instrument_id=instrument,
            bid=Decimal(str(payload["b"])),
            ask=Decimal(str(payload["a"])),
            bid_qty=Decimal(str(payload["B"])),
            ask_qty=Decimal(str(payload["A"])),
            ts_exchange=int(payload.get("E") or payload.get("T") or 0),
            ts_local=int(ts_local),
            source="public_ws",
        )
