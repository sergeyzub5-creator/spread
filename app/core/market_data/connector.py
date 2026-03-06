from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable

from app.core.models.instrument import InstrumentId


class PublicMarketDataConnector(ABC):
    """Exchange connector contract for public L1 market data transport."""

    @abstractmethod
    def connect(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def subscribe_l1(self, instrument: InstrumentId) -> None:
        raise NotImplementedError

    @abstractmethod
    def unsubscribe_l1(self, instrument: InstrumentId) -> None:
        raise NotImplementedError

    @abstractmethod
    def on_quote(self, callback: Callable[[object], None]) -> None:
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError
