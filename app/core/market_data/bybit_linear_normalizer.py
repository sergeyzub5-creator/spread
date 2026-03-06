from __future__ import annotations

from decimal import Decimal
from typing import Any

from app.core.market_data.normalizer import QuoteNormalizer
from app.core.models.instrument import InstrumentId
from app.core.models.market_data import QuoteL1


class BybitLinearQuoteNormalizer(QuoteNormalizer):
    def normalize_l1(self, instrument: InstrumentId, payload: dict[str, Any], ts_local: int) -> QuoteL1:
        bids = payload.get("b", [])
        asks = payload.get("a", [])
        best_bid = bids[0] if isinstance(bids, list) and bids else ["0", "0"]
        best_ask = asks[0] if isinstance(asks, list) and asks else ["0", "0"]
        ts_exchange = int(payload.get("cts") or payload.get("ts") or 0)
        return QuoteL1(
            instrument_id=instrument,
            bid=Decimal(str(best_bid[0])),
            ask=Decimal(str(best_ask[0])),
            bid_qty=Decimal(str(best_bid[1])),
            ask_qty=Decimal(str(best_ask[1])),
            ts_exchange=ts_exchange,
            ts_local=int(ts_local),
            source="public_ws",
        )
