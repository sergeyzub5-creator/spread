from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass, field

from PySide6.QtCore import QObject, Signal


@dataclass(slots=True)
class PerpRowState:
    kind: str
    canonical: str = ""
    volume_usdt: int = 0
    price_value: float | None = None
    bid_price_value: float | None = None
    ask_price_value: float | None = None
    price_text: str = "-"
    accent: str | None = None
    funding_text: str = "-"
    funding_sort_value: float | None = None
    funding_rate_raw: str | None = None
    interval_hours: int | None = None
    timer_text: str = "-"


@dataclass(slots=True)
class PerpSnapshot:
    exchange_id: str
    title: str
    status_ok: bool
    is_fresh: bool = False
    snapshot_age_ms: int | None = None
    loading: bool = False
    status_hint: str = ""
    rows: list[PerpRowState] = field(default_factory=list)


@dataclass(slots=True)
class StarterRowState:
    kind: str
    canonical: str = ""
    bookmarked: bool = False


@dataclass(slots=True)
class StarterSnapshot:
    total_pairs: int
    rows: list[StarterRowState]


@dataclass(slots=True)
class WorkspaceHeaderSnapshot:
    top_volume_text: str
    loaded_pairs_count: int
    pairs_status_text: str
    add_notification_enabled: bool = True
    refresh_enabled: bool = True
    loading: bool = False


@dataclass(slots=True)
class BaseComparisonSnapshot:
    accents_by_exchange: dict[str, dict[str, str | None]]


@dataclass(slots=True)
class WorkspaceSnapshot:
    top_volume_limit: int
    sort_role: str | None = None
    sort_source_id: str | None = None
    sort_key: str | None = None
    sort_descending: bool = True
    column_stretch_by_role: dict[str, int] = field(default_factory=dict)
    runtime_roles: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class OutputRowState:
    kind: str
    canonical: str = ""
    value_text: str = "-"
    sort_value: float | None = None
    accent: str | None = None


@dataclass(slots=True)
class OutputSnapshot:
    runtime_id: str
    title: str
    rows: list[OutputRowState] = field(default_factory=list)


class BasePerpRuntime(QObject):
    snapshot_changed = Signal()
    loading_changed = Signal(bool)

    @abstractmethod
    def snapshot(self) -> PerpSnapshot:
        raise NotImplementedError

    @abstractmethod
    def set_top_volume_limit(self, top_volume_limit: int | None) -> None:
        raise NotImplementedError

    @abstractmethod
    def force_refresh(self) -> None:
        raise NotImplementedError


class BaseOutputRuntime(QObject):
    snapshot_changed = Signal()

    @abstractmethod
    def snapshot(self) -> OutputSnapshot:
        raise NotImplementedError


__all__ = [
    "BaseComparisonSnapshot",
    "BaseOutputRuntime",
    "BasePerpRuntime",
    "OutputRowState",
    "OutputSnapshot",
    "PerpRowState",
    "PerpSnapshot",
    "StarterRowState",
    "StarterSnapshot",
    "WorkspaceHeaderSnapshot",
    "WorkspaceSnapshot",
]
