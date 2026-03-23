from __future__ import annotations

from app.exchange_info_base.common import account_type, endpoint, market_type, price_types
from app.exchange_info_base.models import ExchangeReference

BYBIT_REFERENCE = ExchangeReference(
    exchange_id="bybit",
    title="Bybit",
    verified_scope="Official Bybit V5 docs for market, account and trade flows.",
    account_types=(
        account_type("classic", "Classic Account", "Старый раздельный аккаунт Bybit: spot отдельно, contracts отдельно.", tradable_products=("spot", "linear", "inverse"), notes=("По docs classic account уже legacy, но API-поведение всё ещё описано.",)),
        account_type("uta1", "UTA 1.0", "Unified Trading Account 1.0.", tradable_products=("spot", "linear", "inverse", "option")),
        account_type("uta1_pro", "UTA 1.0 Pro", "UTA 1.0 Pro с тем же продуктовым покрытием и иным performance profile.", tradable_products=("spot", "linear", "inverse", "option")),
        account_type("uta2", "UTA 2.0", "Новый unified account режим Bybit.", tradable_products=("spot", "linear", "inverse", "option")),
        account_type("uta2_pro", "UTA 2.0 Pro", "UTA 2.0 Pro.", tradable_products=("spot", "linear", "inverse", "option")),
    ),
    market_types=(
        market_type("spot", "Spot", native_categories=("spot",)),
        market_type("linear", "Linear Contracts", native_categories=("linear",), contract_types=("USDT perpetual", "USDC perpetual", "USDT futures", "USDC futures"), settlement_types=("USDT", "USDC")),
        market_type("inverse", "Inverse Contracts", native_categories=("inverse",), contract_types=("inverse perpetual", "inverse futures"), settlement_types=("coin-margined",)),
        market_type("option", "Options", native_categories=("option",), settlement_types=("USDC",)),
    ),
    price_types=price_types("last", "bid", "ask", "mid", "mark", "index", "open", "high", "low", "close"),
    endpoints=(
        endpoint("market_tickers", group="market_data", market="spot", title="V5 Get Tickers", method="GET", path="/v5/market/tickers", auth="public", symbol_mode="category_and_optional_symbol", response_symbol_field="list[].symbol", response_fields=("lastPrice", "bid1Price", "ask1Price", "markPrice", "indexPrice", "volume24h", "turnover24h", "fundingRate", "nextFundingTime", "fundingIntervalHour"), price_types=("last", "bid", "ask", "mid", "mark", "index"), intended_use="Основной bulk snapshot цен по Spot/Linear/Inverse/Option.", source_urls=("https://bybit-exchange.github.io/docs/v5/market/tickers",)),
        endpoint("market_instruments_info", group="market_data", market="linear", title="V5 Get Instruments Info", method="GET", path="/v5/market/instruments-info", auth="public", symbol_mode="category_and_optional_symbol", response_symbol_field="list[].symbol", response_fields=("status", "symbolType", "settleCoin", "baseCoin", "quoteCoin", "contractType", "deliveryTime", "launchTime"), price_types=(), intended_use="Universe инструментов, статусы и contract metadata.", source_urls=("https://bybit-exchange.github.io/docs/v5/market/instrument",)),
        endpoint("market_orderbook", group="market_data", market="spot", title="V5 Get Orderbook", method="GET", path="/v5/market/orderbook", auth="public", symbol_mode="category_and_symbol", response_symbol_field="s", response_fields=("b", "a", "ts"), price_types=("bid", "ask"), intended_use="Top-of-book / стакан Bybit.", notes=("Сами цены best bid/ask обычно уже приходят и в /v5/market/tickers, но orderbook route полезен как reference для depth.",), source_urls=("https://bybit-exchange.github.io/docs/v5/market/orderbook",)),
        endpoint("market_kline", group="market_data", market="spot", title="V5 Get Kline", method="GET", path="/v5/market/kline", auth="public", symbol_mode="category_and_symbol", response_symbol_field="symbol(request)", response_fields=("open", "high", "low", "close"), price_types=("open", "high", "low", "close"), intended_use="Исторические OHLC close/open/high/low.", source_urls=("https://bybit-exchange.github.io/docs/v5/market/kline",)),
        endpoint("market_mark_price_kline", group="market_data", market="linear", title="V5 Get Mark Price Kline", method="GET", path="/v5/market/mark-price-kline", auth="public", symbol_mode="category_and_symbol", response_symbol_field="symbol(request)", response_fields=("open", "high", "low", "close"), price_types=("mark", "close"), intended_use="История mark price по контрактам.", source_urls=("https://bybit-exchange.github.io/docs/v5/market/mark-kline",)),
        endpoint("market_index_price_kline", group="market_data", market="linear", title="V5 Get Index Price Kline", method="GET", path="/v5/market/index-price-kline", auth="public", symbol_mode="category_and_symbol", response_symbol_field="symbol(request)", response_fields=("open", "high", "low", "close"), price_types=("index", "close"), intended_use="История index price по контрактам.", source_urls=("https://bybit-exchange.github.io/docs/v5/market/index-kline",)),
        endpoint("account_info", group="account", market="account", title="V5 Get Account Info", method="GET", path="/v5/account/info", auth="user_data", symbol_mode="none", response_symbol_field="", response_fields=("unifiedMarginStatus", "marginMode", "spotHedgingStatus"), price_types=(), intended_use="Определение account mode и margin mode Bybit.", source_urls=("https://bybit-exchange.github.io/docs/v5/account/account-info", "https://bybit-exchange.github.io/docs/v5/acct-mode")),
        endpoint("wallet_balance", group="account", market="account", title="V5 Get Wallet Balance", method="GET", path="/v5/account/wallet-balance", auth="user_data", symbol_mode="account_type", response_symbol_field="list[].coin[].coin", response_fields=("accountType", "totalEquity", "coin", "walletBalance", "availableToWithdraw"), price_types=(), intended_use="Баланс unified/classic account.", source_urls=("https://bybit-exchange.github.io/docs/v5/account/wallet-balance",)),
        endpoint("asset_all_coin_balance", group="account", market="account", title="V5 Get All Coins Balance", method="GET", path="/v5/asset/transfer/query-account-coins-balance", auth="user_data", symbol_mode="account_type", response_symbol_field="balance[].coin", response_fields=("accountType", "coin", "walletBalance", "transferBalance"), price_types=(), intended_use="Балансы по accountType на уровне asset module.", source_urls=("https://bybit-exchange.github.io/docs/v5/asset/balance/all-balance",)),
        endpoint("order_create", group="trade", market="spot", title="V5 Place Order", method="POST", path="/v5/order/create", auth="trade", symbol_mode="category_and_symbol", response_symbol_field="symbol(request)", response_fields=("orderId", "orderLinkId"), price_types=(), intended_use="Единый order endpoint Bybit V5 для spot/linear/inverse/option.", source_urls=("https://bybit-exchange.github.io/docs/v5/order/create-order",)),
    ),
)
