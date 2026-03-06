from __future__ import annotations

from dataclasses import asdict, dataclass, field


@dataclass(frozen=True, slots=True)
class ExchangeCredentials:
    exchange: str
    api_key: str
    api_secret: str
    api_passphrase: str = ""
    account_profile: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ExchangeAccountSnapshot:
    exchange: str
    status_text: str
    balance_text: str
    positions_text: str
    pnl_text: str
    spot_enabled: bool
    futures_enabled: bool
    can_trade: bool
    account_profile: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ClosePositionsResult:
    exchange: str
    closed_count: int
    closed_symbols: tuple[str, ...]
    account_snapshot: ExchangeAccountSnapshot

    def to_dict(self) -> dict:
        payload = asdict(self)
        payload["account_snapshot"] = self.account_snapshot.to_dict()
        return payload
