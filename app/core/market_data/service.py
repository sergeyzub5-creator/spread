from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable

from app.core.market_data.connector import PublicMarketDataConnector
from app.core.market_data.normalizer import QuoteNormalizer
from app.core.models.instrument import InstrumentId
from app.core.models.market_data import QuoteL1


class MarketDataService:
    """Owns public WS subscriptions and normalized quote fan-out.

    This service is transport-only. It must not hold strategy state, spread state,
    or a shared business cache used by worker logic.
    """

    def __init__(self) -> None:
        self._subscribers: dict[InstrumentId, list[Callable[[QuoteL1], None]]] = defaultdict(list)
        self._connectors: dict[str, PublicMarketDataConnector] = {}
        self._normalizers: dict[str, QuoteNormalizer] = {}

    def register_exchange_transport(
        self,
        transport_key: str,
        connector: PublicMarketDataConnector,
        normalizer: QuoteNormalizer,
    ) -> None:
        self._connectors[transport_key] = connector
        self._normalizers[transport_key] = normalizer
        connector.on_quote(lambda event, key=transport_key: self._handle_raw_quote(key, event))

    def subscribe_l1(self, instrument: InstrumentId, callback: Callable[[QuoteL1], None]) -> None:
        callbacks = self._subscribers[instrument]
        first_subscription = not callbacks
        callbacks.append(callback)

        connector = self._connectors.get(self._transport_key(instrument))
        if connector is not None and first_subscription:
            connector.connect()
            connector.subscribe_l1(instrument)

    def unsubscribe_l1(self, instrument: InstrumentId, callback: Callable[[QuoteL1], None]) -> None:
        callbacks = self._subscribers.get(instrument, [])
        if callback in callbacks:
            callbacks.remove(callback)
        if not callbacks and instrument in self._subscribers:
            self._subscribers.pop(instrument, None)
            connector = self._connectors.get(self._transport_key(instrument))
            if connector is not None:
                connector.unsubscribe_l1(instrument)

    def publish_quote(self, quote: QuoteL1) -> None:
        for callback in list(self._subscribers.get(quote.instrument_id, [])):
            callback(quote)

    def _handle_raw_quote(self, exchange: str, event: object) -> None:
        if not isinstance(event, dict):
            return
        instrument = event.get("instrument")
        payload = event.get("payload")
        ts_local = event.get("ts_local")
        if not isinstance(instrument, InstrumentId) or not isinstance(payload, dict):
            return
        normalizer = self._normalizers.get(exchange)
        if normalizer is None:
            return
        quote = normalizer.normalize_l1(instrument=instrument, payload=payload, ts_local=int(ts_local or 0))
        self.publish_quote(quote)

    @staticmethod
    def _transport_key(instrument: InstrumentId) -> str:
        return f"{instrument.exchange}:{instrument.market_type}"
