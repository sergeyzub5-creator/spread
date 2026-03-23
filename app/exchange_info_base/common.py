from __future__ import annotations

from app.exchange_info_base.models import (
    AccountTypeDefinition,
    EndpointSpec,
    MarketTypeDefinition,
    PriceTypeDefinition,
)


STANDARD_PRICE_TYPES: dict[str, PriceTypeDefinition] = {
    "last": PriceTypeDefinition(
        key="last",
        title="Last",
        meaning="Последняя цена сделки из ticker/trade snapshot.",
    ),
    "bid": PriceTypeDefinition(
        key="bid",
        title="Best Bid",
        meaning="Лучшая цена покупателя в верхушке стакана.",
    ),
    "ask": PriceTypeDefinition(
        key="ask",
        title="Best Ask",
        meaning="Лучшая цена продавца в верхушке стакана.",
    ),
    "mid": PriceTypeDefinition(
        key="mid",
        title="Mid",
        meaning="Средняя между лучшим bid и лучшим ask.",
        derived=True,
    ),
    "mark": PriceTypeDefinition(
        key="mark",
        title="Mark",
        meaning="Марковая цена дериватива, используемая биржей для риска и funding.",
    ),
    "index": PriceTypeDefinition(
        key="index",
        title="Index",
        meaning="Индексная цена, на которую биржа опирает mark и расчёты.",
    ),
    "close": PriceTypeDefinition(
        key="close",
        title="OHLC Close",
        meaning="Цена закрытия свечи/kline для выбранного таймфрейма.",
    ),
    "open": PriceTypeDefinition(
        key="open",
        title="OHLC Open",
        meaning="Цена открытия свечи/kline.",
    ),
    "high": PriceTypeDefinition(
        key="high",
        title="OHLC High",
        meaning="Максимальная цена свечи/kline.",
    ),
    "low": PriceTypeDefinition(
        key="low",
        title="OHLC Low",
        meaning="Минимальная цена свечи/kline.",
    ),
    "fair": PriceTypeDefinition(
        key="fair",
        title="Fair",
        meaning="Справедливая цена/справедливая стоимость контракта.",
    ),
}


def price_types(*keys: str) -> tuple[PriceTypeDefinition, ...]:
    return tuple(STANDARD_PRICE_TYPES[key] for key in keys)


def account_type(
    key: str,
    title: str,
    description: str,
    *,
    tradable_products: tuple[str, ...] = (),
    notes: tuple[str, ...] = (),
) -> AccountTypeDefinition:
    return AccountTypeDefinition(
        key=key,
        title=title,
        description=description,
        tradable_products=tradable_products,
        notes=notes,
    )


def market_type(
    key: str,
    title: str,
    *,
    native_categories: tuple[str, ...] = (),
    contract_types: tuple[str, ...] = (),
    settlement_types: tuple[str, ...] = (),
    notes: tuple[str, ...] = (),
) -> MarketTypeDefinition:
    return MarketTypeDefinition(
        key=key,
        title=title,
        native_categories=native_categories,
        contract_types=contract_types,
        settlement_types=settlement_types,
        notes=notes,
    )


def endpoint(
    key: str,
    *,
    group: str,
    market: str,
    title: str,
    method: str,
    path: str,
    auth: str,
    symbol_mode: str,
    response_symbol_field: str,
    response_fields: tuple[str, ...],
    price_types: tuple[str, ...],
    intended_use: str,
    notes: tuple[str, ...] = (),
    source_urls: tuple[str, ...] = (),
) -> EndpointSpec:
    return EndpointSpec(
        key=key,
        group=group,
        market=market,
        title=title,
        method=method,
        path=path,
        auth=auth,
        symbol_mode=symbol_mode,
        response_symbol_field=response_symbol_field,
        response_fields=response_fields,
        price_types=price_types,
        intended_use=intended_use,
        notes=notes,
        source_urls=source_urls,
    )
