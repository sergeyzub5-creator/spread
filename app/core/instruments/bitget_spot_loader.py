from __future__ import annotations

from decimal import Decimal
from typing import Any

from app.core.bitget.http_client import BitgetPublicHttpClient
from app.core.models.instrument import InstrumentId, InstrumentKey, InstrumentRouting, InstrumentSpec


class BitgetSpotInstrumentLoader:
    EXCHANGE = "bitget"
    INSTRUMENTS_PATH = "/api/v2/spot/public/symbols"

    def __init__(self, timeout_seconds: float = 10.0) -> None:
        self._client = BitgetPublicHttpClient(timeout_seconds=timeout_seconds)

    def load_instruments(self) -> list[InstrumentId]:
        payload = self._client.get(self.INSTRUMENTS_PATH)
        items = payload.get("data", []) if isinstance(payload, dict) else []
        instruments: list[InstrumentId] = []
        for item in items:
            instrument = self._build_instrument(item)
            if instrument is not None:
                instruments.append(instrument)
        return instruments

    def _build_instrument(self, item: dict[str, Any]) -> InstrumentId | None:
        status = str(item.get("status", "")).strip().lower()
        symbol = str(item.get("symbol", "")).strip().upper()
        if not symbol or status != "online":
            return None

        base_asset = str(item.get("baseCoin", "")).strip().upper()
        quote_asset = str(item.get("quoteCoin", "")).strip().upper()
        if not base_asset or not quote_asset:
            return None

        key = InstrumentKey(exchange=self.EXCHANGE, market_type="spot", symbol=symbol)
        spec = InstrumentSpec(
            base_asset=base_asset,
            quote_asset=quote_asset,
            contract_type="spot",
            settle_asset=quote_asset,
            price_precision=self._precision_to_step(item.get("pricePrecision")),
            qty_precision=self._precision_to_step(item.get("quantityPrecision")),
            min_qty=Decimal(str(item.get("minTradeAmount", "0"))),
            min_notional=Decimal(str(item.get("minTradeUSDT", "0"))),
        )
        routing = InstrumentRouting(
            ws_channel="books1",
            ws_symbol=symbol,
            order_route="bitget_spot_ws_api",
        )
        return InstrumentId(key=key, spec=spec, routing=routing)

    @staticmethod
    def _precision_to_step(value: object) -> Decimal:
        try:
            digits = int(str(value or "0").strip())
        except (TypeError, ValueError):
            return Decimal("0")
        if digits <= 0:
            return Decimal("1")
        return Decimal("1").scaleb(-digits)
