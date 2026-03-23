from __future__ import annotations

from app.exchange_info_base.exchanges.binance.reference import BINANCE_REFERENCE
from app.exchange_info_base.exchanges.bitget.reference import BITGET_REFERENCE
from app.exchange_info_base.exchanges.bybit.reference import BYBIT_REFERENCE
from app.exchange_info_base.exchanges.mexc.reference import MEXC_REFERENCE
from app.exchange_info_base.exchanges.okx.reference import OKX_REFERENCE
from app.exchange_info_base.models import (
    AccountTypeDefinition,
    EndpointSpec,
    ExchangeReference,
    MarketTypeDefinition,
    PriceTypeDefinition,
)

_REGISTRY: dict[str, ExchangeReference] = {
    "binance": BINANCE_REFERENCE,
    "bitget": BITGET_REFERENCE,
    "bybit": BYBIT_REFERENCE,
    "mexc": MEXC_REFERENCE,
    "okx": OKX_REFERENCE,
}


def list_exchange_ids() -> list[str]:
    return sorted(_REGISTRY.keys())


def get_exchange_reference(exchange_id: str) -> ExchangeReference | None:
    return _REGISTRY.get(str(exchange_id or "").strip().lower())


def get_price_type_definition(exchange_id: str, price_type: str) -> PriceTypeDefinition | None:
    reference = get_exchange_reference(exchange_id)
    if reference is None:
        return None
    normalized = str(price_type or "").strip().lower()
    for item in reference.price_types:
        if item.key == normalized:
            return item
    return None


def get_account_type_definition(exchange_id: str, account_type: str) -> AccountTypeDefinition | None:
    reference = get_exchange_reference(exchange_id)
    if reference is None:
        return None
    normalized = str(account_type or "").strip().lower()
    for item in reference.account_types:
        if item.key == normalized:
            return item
    return None


def get_market_type_definition(exchange_id: str, market_type: str) -> MarketTypeDefinition | None:
    reference = get_exchange_reference(exchange_id)
    if reference is None:
        return None
    normalized = str(market_type or "").strip().lower()
    for item in reference.market_types:
        if item.key == normalized:
            return item
    return None


def list_endpoint_keys(exchange_id: str) -> list[str]:
    reference = get_exchange_reference(exchange_id)
    if reference is None:
        return []
    return sorted(item.key for item in reference.endpoints)


def get_endpoint_spec(exchange_id: str, endpoint_key: str) -> EndpointSpec | None:
    reference = get_exchange_reference(exchange_id)
    if reference is None:
        return None
    normalized = str(endpoint_key or "").strip().lower()
    for item in reference.endpoints:
        if item.key == normalized:
            return item
    return None
