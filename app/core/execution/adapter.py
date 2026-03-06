from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any

from app.core.models.execution import ExecutionOrderRequest, ExecutionOrderResult, ExecutionStreamEvent


class ExecutionAdapter(ABC):
    """Exchange execution contract for private/trading websocket routing."""

    @abstractmethod
    def route_name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def connect(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def on_execution_event(self, callback: Callable[[ExecutionStreamEvent], None]) -> None:
        raise NotImplementedError

    @abstractmethod
    def place_order(
        self,
        request: ExecutionOrderRequest,
        on_request_sent: Callable[[dict[str, Any]], None] | None = None,
    ) -> ExecutionOrderResult:
        raise NotImplementedError

    @abstractmethod
    def cancel_order(
        self,
        *,
        symbol: str,
        order_id: str | None = None,
        client_order_id: str | None = None,
    ) -> ExecutionOrderResult:
        raise NotImplementedError

    @abstractmethod
    def query_order(
        self,
        *,
        symbol: str,
        order_id: str | None = None,
        client_order_id: str | None = None,
    ) -> ExecutionOrderResult:
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError
