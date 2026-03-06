from __future__ import annotations

from decimal import Decimal
from typing import Any

from app.core.market_data.normalizer import QuoteNormalizer
from app.core.models.instrument import InstrumentId
from app.core.models.market_data import QuoteL1


class BitgetSpotQuoteNormalizer(QuoteNormalizer):
    def normalize_l1(self, instrument: InstrumentId, payload: dict[str, Any], ts_local: int) -> QuoteL1:
        bids = payload.get("bids") if isinstance(payload.get("bids"), list) else payload.get("b")
        asks = payload.get("asks") if isinstance(payload.get("asks"), list) else payload.get("a")
        bid_level = bids[0] if isinstance(bids, list) and bids else None
        ask_level = asks[0] if isinstance(asks, list) and asks else None
        if not isinstance(bid_level, list) or len(bid_level) < 2:
            raise ValueError("Bitget spot bid level is missing")
        if not isinstance(ask_level, list) or len(ask_level) < 2:
            raise ValueError("Bitget spot ask level is missing")
        return QuoteL1(
            instrument_id=instrument,
            bid=Decimal(str(bid_level[0])),
            ask=Decimal(str(ask_level[0])),
            bid_qty=Decimal(str(bid_level[1])),
            ask_qty=Decimal(str(ask_level[1])),
            ts_exchange=int(payload.get("ts") or 0),
            ts_local=int(ts_local),
            source="public_ws",
        )
