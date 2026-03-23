from __future__ import annotations

from typing import Protocol

from app.charts.history.models import ChartHistoryRequest, RawHistoryBar


class ChartHistoryProvider(Protocol):
    def load_history(self, request: ChartHistoryRequest) -> list[RawHistoryBar]:
        ...
