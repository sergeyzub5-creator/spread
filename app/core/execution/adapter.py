from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from app.core.models.instrument import InstrumentId


class ExecutionAdapter(ABC):
    """Exchange execution contract for future private/trading websocket routing."""

    @abstractmethod
    def route_name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def place_order(self, instrument: InstrumentId, request: dict[str, Any]) -> None:
        raise NotImplementedError
