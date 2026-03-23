from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class SlotSelection:
    exchange: str | None = None
    market_type: str | None = None
    symbol: str | None = None

    def __getitem__(self, key: str):
        return getattr(self, key)

    def __setitem__(self, key: str, value) -> None:
        setattr(self, key, value)

    def get(self, key: str, default=None):
        return getattr(self, key, default)

    def update(self, values: dict[str, object]) -> None:
        for key, value in values.items():
            setattr(self, key, value)

    def is_complete(self) -> bool:
        return bool(self.exchange and self.market_type and self.symbol)


@dataclass
class PositionManagerUiState:
    left: SlotSelection = field(default_factory=SlotSelection)
    right: SlotSelection = field(default_factory=SlotSelection)

    @property
    def slots(self) -> dict[str, SlotSelection]:
        return {"left": self.left, "right": self.right}

    def is_ready(self) -> bool:
        return self.left.is_complete() and self.right.is_complete()
