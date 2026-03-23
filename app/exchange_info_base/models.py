from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class PriceTypeDefinition:
    key: str
    title: str
    meaning: str
    derived: bool = False


@dataclass(frozen=True, slots=True)
class AccountTypeDefinition:
    key: str
    title: str
    description: str
    tradable_products: tuple[str, ...] = ()
    notes: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class MarketTypeDefinition:
    key: str
    title: str
    native_categories: tuple[str, ...] = ()
    contract_types: tuple[str, ...] = ()
    settlement_types: tuple[str, ...] = ()
    notes: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class EndpointSpec:
    key: str
    group: str
    market: str
    title: str
    method: str
    path: str
    auth: str
    symbol_mode: str
    response_symbol_field: str
    response_fields: tuple[str, ...]
    price_types: tuple[str, ...]
    intended_use: str
    notes: tuple[str, ...] = ()
    source_urls: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ExchangeReference:
    exchange_id: str
    title: str
    verified_scope: str
    account_types: tuple[AccountTypeDefinition, ...]
    market_types: tuple[MarketTypeDefinition, ...]
    price_types: tuple[PriceTypeDefinition, ...]
    endpoints: tuple[EndpointSpec, ...]
