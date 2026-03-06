from __future__ import annotations

from decimal import Decimal
from typing import Any

from app.core.bitget.http_client import BitgetPublicHttpClient
from app.core.models.instrument import InstrumentId, InstrumentKey, InstrumentRouting, InstrumentSpec


class BitgetLinearInstrumentLoader:
    EXCHANGE = "bitget"
    INSTRUMENTS_PATH = "/api/v2/mix/market/contracts"

    def __init__(self, timeout_seconds: float = 10.0) -> None:
        self._client = BitgetPublicHttpClient(timeout_seconds=timeout_seconds)

    def load_instruments(self) -> list[InstrumentId]:
        payload = self._client.get(self.INSTRUMENTS_PATH, params={"productType": "usdt-futures"})
        items = payload.get("data", []) if isinstance(payload, dict) else []
        instruments: list[InstrumentId] = []
        for item in items:
            instrument = self._build_instrument(item)
            if instrument is not None:
                instruments.append(instrument)
        return instruments

    def _build_instrument(self, item: dict[str, Any]) -> InstrumentId | None:
        symbol = str(item.get("symbol", "")).strip().upper()
        symbol_type = str(item.get("symbolType", "")).strip().lower()
        symbol_status = str(item.get("symbolStatus", "")).strip().lower()
        if not symbol or symbol_type != "perpetual" or symbol_status != "normal":
            return None

        base_asset = str(item.get("baseCoin", "")).strip().upper()
        quote_asset = str(item.get("quoteCoin", "")).strip().upper()
        settle_asset = quote_asset
        if not base_asset or not quote_asset:
            return None

        key = InstrumentKey(exchange=self.EXCHANGE, market_type="linear_perp", symbol=symbol)
        spec = InstrumentSpec(
            base_asset=base_asset,
            quote_asset=quote_asset,
            contract_type="perpetual",
            settle_asset=settle_asset,
            price_precision=self._precision_to_step(item.get("pricePlace"), item.get("priceEndStep")),
            qty_precision=Decimal(str(item.get("sizeMultiplier", "0"))),
            min_qty=Decimal(str(item.get("minTradeNum", "0"))),
            min_notional=Decimal(str(item.get("minTradeUSDT", "0"))),
        )
        routing = InstrumentRouting(
            ws_channel="books1",
            ws_symbol=symbol,
            order_route="bitget_linear_trade_ws",
        )
        return InstrumentId(key=key, spec=spec, routing=routing)

    @staticmethod
    def _precision_to_step(price_place: object, price_end_step: object) -> Decimal:
        try:
            digits = int(str(price_place or "0").strip())
        except (TypeError, ValueError):
            digits = 0
        try:
            end_step = int(str(price_end_step or "1").strip())
        except (TypeError, ValueError):
            end_step = 1
        return Decimal(str(end_step)).scaleb(-digits)
