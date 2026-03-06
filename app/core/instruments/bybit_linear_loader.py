from __future__ import annotations

from decimal import Decimal
from typing import Any

from app.core.bybit.http_client import BybitV5HttpClient
from app.core.models.instrument import InstrumentId, InstrumentKey, InstrumentRouting, InstrumentSpec


class BybitLinearInstrumentLoader:
    EXCHANGE = "bybit"
    INSTRUMENTS_PATH = "/v5/market/instruments-info"

    def __init__(self, timeout_seconds: float = 10.0) -> None:
        self._client = BybitV5HttpClient(timeout_seconds=timeout_seconds)

    def load_instruments(self) -> list[InstrumentId]:
        instruments: list[InstrumentId] = []
        cursor: str | None = None

        while True:
            params: dict[str, Any] = {"category": "linear", "limit": 1000}
            if cursor:
                params["cursor"] = cursor
            payload = self._client.get(self.INSTRUMENTS_PATH, params=params, auth=False)
            result = payload.get("result", {}) if isinstance(payload, dict) else {}
            items = result.get("list", []) if isinstance(result, dict) else []
            for item in items:
                instrument = self._build_instrument(item)
                if instrument is not None:
                    instruments.append(instrument)
            cursor = str(result.get("nextPageCursor", "")).strip()
            if not cursor:
                break
        return instruments

    def _build_instrument(self, item: dict[str, Any]) -> InstrumentId | None:
        status = str(item.get("status", "")).strip().upper()
        symbol = str(item.get("symbol", "")).strip().upper()
        contract_type = str(item.get("contractType", "")).strip()
        if not symbol or status != "TRADING":
            return None
        if "PERPETUAL" not in contract_type.upper():
            return None

        price_filter = item.get("priceFilter", {}) if isinstance(item.get("priceFilter"), dict) else {}
        lot_filter = item.get("lotSizeFilter", {}) if isinstance(item.get("lotSizeFilter"), dict) else {}
        key = InstrumentKey(exchange=self.EXCHANGE, market_type="linear_perp", symbol=symbol)
        spec = InstrumentSpec(
            base_asset=str(item.get("baseCoin", "")),
            quote_asset=str(item.get("quoteCoin", "")),
            contract_type=contract_type.lower(),
            settle_asset=str(item.get("settleCoin", "")),
            price_precision=Decimal(str(price_filter.get("tickSize", "0"))),
            qty_precision=Decimal(str(lot_filter.get("qtyStep", "0"))),
            min_qty=Decimal(str(lot_filter.get("minOrderQty", "0"))),
            min_notional=Decimal(str(lot_filter.get("minNotionalValue", "0"))),
        )
        routing = InstrumentRouting(
            ws_channel="orderbook.1",
            ws_symbol=symbol,
            order_route="bybit_linear_trade_ws",
        )
        return InstrumentId(key=key, spec=spec, routing=routing)
