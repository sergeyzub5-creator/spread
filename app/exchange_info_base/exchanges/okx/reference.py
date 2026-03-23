from __future__ import annotations

from app.exchange_info_base.common import account_type, endpoint, market_type, price_types
from app.exchange_info_base.models import ExchangeReference

OKX_REFERENCE = ExchangeReference(
    exchange_id="okx",
    title="OKX",
    verified_scope="Official OKX V5 docs for trading account, market data and order routes.",
    account_types=(
        account_type("spot_mode", "Spot Mode", "OKX acctLv=1.", tradable_products=("spot",)),
        account_type("futures_mode", "Futures Mode", "OKX acctLv=2.", tradable_products=("spot", "margin", "swap", "futures", "option")),
        account_type("multi_currency_margin", "Multi-currency Margin", "OKX acctLv=3.", tradable_products=("spot", "margin", "swap", "futures", "option")),
        account_type("portfolio_margin", "Portfolio Margin", "OKX acctLv=4.", tradable_products=("spot", "margin", "swap", "futures", "option")),
    ),
    market_types=(
        market_type("spot", "SPOT", native_categories=("SPOT",)),
        market_type("margin", "MARGIN", native_categories=("MARGIN",)),
        market_type("swap", "SWAP", native_categories=("SWAP",), contract_types=("perpetual",)),
        market_type("futures", "FUTURES", native_categories=("FUTURES",), contract_types=("expiry futures",)),
        market_type("option", "OPTION", native_categories=("OPTION",)),
    ),
    price_types=price_types("last", "bid", "ask", "mid", "mark", "index", "open", "high", "low", "close"),
    endpoints=(
        endpoint("market_tickers", group="market_data", market="swap", title="Get Tickers", method="GET", path="/api/v5/market/tickers", auth="public", symbol_mode="inst_type", response_symbol_field="data[].instId", response_fields=("last", "askPx", "bidPx", "vol24h", "volCcy24h"), price_types=("last", "bid", "ask", "mid"), intended_use="Bulk latest price snapshot by instType.", notes=("Docs snippet explicitly lists GET /api/v5/market/tickers with instType=SPOT/MARGIN/SWAP/FUTURES/OPTION.",), source_urls=("https://app.okx.com/docs-v5/en/",)),
        endpoint("market_ticker", group="market_data", market="swap", title="Get Ticker", method="GET", path="/api/v5/market/ticker", auth="public", symbol_mode="inst_id", response_symbol_field="data[].instId", response_fields=("last", "askPx", "bidPx", "vol24h", "volCcy24h"), price_types=("last", "bid", "ask", "mid"), intended_use="Selective ticker by instId.", source_urls=("https://app.okx.com/docs-v5/en/",)),
        endpoint("market_books", group="market_data", market="spot", title="Get Order Book", method="GET", path="/api/v5/market/books", auth="public", symbol_mode="inst_id", response_symbol_field="instId(request)", response_fields=("asks", "bids", "ts"), price_types=("bid", "ask"), intended_use="Depth/book route for best bid/ask and local book maintenance.", source_urls=("https://app.okx.com/docs-v5/en/",)),
        endpoint("market_candles", group="market_data", market="spot", title="Get Candlesticks", method="GET", path="/api/v5/market/candles", auth="public", symbol_mode="inst_id", response_symbol_field="instId(request)", response_fields=("open", "high", "low", "close"), price_types=("open", "high", "low", "close"), intended_use="Последние свечи / near-history.", source_urls=("https://app.okx.com/docs-v5/en/",)),
        endpoint("market_history_candles", group="market_data", market="spot", title="Get Candlesticks History", method="GET", path="/api/v5/market/history-candles", auth="public", symbol_mode="inst_id", response_symbol_field="instId(request)", response_fields=("open", "high", "low", "close"), price_types=("open", "high", "low", "close"), intended_use="Исторические OHLC за более длинный период.", source_urls=("https://app.okx.com/docs-v5/en/",)),
        endpoint("public_instruments", group="market_data", market="swap", title="Get Public Instruments", method="GET", path="/api/v5/public/instruments", auth="public", symbol_mode="inst_type", response_symbol_field="data[].instId", response_fields=("instType", "instFamily", "state", "ctType", "uly", "settleCcy"), price_types=(), intended_use="Universe публичных инструментов и contract metadata.", source_urls=("https://app.okx.com/docs-v5/en/",)),
        endpoint("public_funding_rate", group="market_data", market="swap", title="Get Funding Rate", method="GET", path="/api/v5/public/funding-rate", auth="public", symbol_mode="inst_id", response_symbol_field="data[].instId", response_fields=("fundingRate", "nextFundingTime", "settFundingRate"), price_types=(), intended_use="Funding snapshot per swap instrument.", source_urls=("https://app.okx.com/docs-v5/en/",)),
        endpoint("public_mark_price", group="market_data", market="swap", title="Get Mark Price", method="GET", path="/api/v5/public/mark-price", auth="public", symbol_mode="inst_type_or_inst_id", response_symbol_field="data[].instId", response_fields=("markPx",), price_types=("mark",), intended_use="Mark price snapshot by instType and optionally instId.", source_urls=("https://app.okx.com/docs-v5/en/",)),
        endpoint("account_config", group="account", market="account", title="Get Account Configuration", method="GET", path="/api/v5/account/config", auth="user_data", symbol_mode="none", response_symbol_field="", response_fields=("acctLv", "posMode", "autoLoan", "greeksType"), price_types=(), intended_use="Определение account mode и position mode.", source_urls=("https://app.okx.com/docs-v5/en/",)),
        endpoint("account_balance", group="account", market="account", title="Get Balance", method="GET", path="/api/v5/account/balance", auth="user_data", symbol_mode="optional_ccy", response_symbol_field="data[].details[].ccy", response_fields=("totalEq", "adjEq", "details"), price_types=(), intended_use="Баланс trading account.", source_urls=("https://app.okx.com/docs-v5/en/",)),
        endpoint("account_instruments", group="account", market="account", title="Get Account Instruments", method="GET", path="/api/v5/account/instruments", auth="user_data", symbol_mode="inst_type", response_symbol_field="data[].instId", response_fields=("instType", "instId", "instFamily", "tradeQuoteCcyList"), price_types=(), intended_use="Инструменты, доступные текущему аккаунту.", source_urls=("https://app.okx.com/docs-v5/en/",)),
        endpoint("trade_order", group="trade", market="spot", title="Place Order", method="POST", path="/api/v5/trade/order", auth="trade", symbol_mode="inst_id", response_symbol_field="instId(request)", response_fields=("ordId", "clOrdId"), price_types=(), intended_use="Единый trade order endpoint OKX.", source_urls=("https://app.okx.com/docs-v5/en/",)),
    ),
)
