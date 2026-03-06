from __future__ import annotations

import json
from decimal import Decimal
from typing import Any
from urllib.request import Request, urlopen

from app.core.models.instrument import InstrumentId, InstrumentKey, InstrumentRouting, InstrumentSpec


class BinanceSpotInstrumentLoader:
    """Loads Binance spot instruments from exchangeInfo."""

    EXCHANGE = "binance"
    EXCHANGE_INFO_URL = "https://api.binance.com/api/v3/exchangeInfo?permissions=SPOT"

    def __init__(self, timeout_seconds: float = 10.0) -> None:
        self.timeout_seconds = float(timeout_seconds)

    def load_instruments(self) -> list[InstrumentId]:
        request = Request(self.EXCHANGE_INFO_URL, headers={"User-Agent": "spread-sniper-ui-shell/1.0"})
        with urlopen(request, timeout=self.timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8"))

        instruments: list[InstrumentId] = []
        for symbol_info in payload.get("symbols", []):
            instrument = self._build_instrument(symbol_info)
            if instrument is not None:
                instruments.append(instrument)
        return instruments

    def _build_instrument(self, symbol_info: dict[str, Any]) -> InstrumentId | None:
        status = str(symbol_info.get("status", "")).upper()
        symbol = str(symbol_info.get("symbol", "")).upper()
        if not symbol or status != "TRADING":
            return None

        filters = {item.get("filterType"): item for item in symbol_info.get("filters", [])}
        price_filter = filters.get("PRICE_FILTER", {})
        lot_size_filter = filters.get("LOT_SIZE", {})
        notional_filter = filters.get("MIN_NOTIONAL", {}) or filters.get("NOTIONAL", {})

        key = InstrumentKey(exchange=self.EXCHANGE, market_type="spot", symbol=symbol)
        spec = InstrumentSpec(
            base_asset=str(symbol_info.get("baseAsset", "")),
            quote_asset=str(symbol_info.get("quoteAsset", "")),
            contract_type="spot",
            settle_asset=str(symbol_info.get("quoteAsset", "")),
            price_precision=Decimal(str(price_filter.get("tickSize", "0"))),
            qty_precision=Decimal(str(lot_size_filter.get("stepSize", "0"))),
            min_qty=Decimal(str(lot_size_filter.get("minQty", "0"))),
            min_notional=Decimal(str(notional_filter.get("minNotional") or notional_filter.get("notional") or "0")),
        )
        routing = InstrumentRouting(
            ws_channel="bookTicker",
            ws_symbol=symbol.lower(),
            order_route="order.place",
        )
        return InstrumentId(key=key, spec=spec, routing=routing)
