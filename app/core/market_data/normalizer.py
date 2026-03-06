from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from app.core.models.instrument import InstrumentId
from app.core.models.market_data import QuoteL1


class QuoteNormalizer(ABC):
    """Converts raw exchange payload into canonical QuoteL1."""

    @abstractmethod
    def normalize_l1(self, instrument: InstrumentId, payload: dict[str, Any], ts_local: int) -> QuoteL1:
        raise NotImplementedError
