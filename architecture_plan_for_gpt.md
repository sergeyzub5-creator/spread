# Architecture Plan For GPT

## 1. Scope

This document defines the target architecture for a new cross-exchange arbitrage application.

Current state:
- old system is discarded
- current repository contains only a UI shell
- system is built for REAL mode only

Out of scope for this phase:
- full strategy implementation
- full risk engine
- full order execution
- demo mode
- fallback or legacy flows

This phase must deliver:
- clean project structure
- explicit data contracts
- layer boundaries
- worker-ready runtime architecture

## 2. Target Application Scheme

### 2.1 UI Layer

Responsibilities:
- select exchanges
- request available instruments for selected exchange
- display available instruments
- store selected pair as `InstrumentId`
- create `WorkerTask`
- start and stop workers
- show worker status and aggregated telemetry

UI must not:
- calculate spread
- contain trading logic
- send orders directly
- process raw tick streams
- hold exchange websocket connections

UI output to backend:
- `WorkerTask`
- start/stop commands
- exchange/instrument selection requests

UI input from backend:
- `WorkerState`
- `WorkerEvent`
- instrument lists
- aggregated health/status

### 2.2 Market Data Layer

Responsibilities:
- manage public websocket sessions per exchange
- subscribe to L1 order book for selected `InstrumentId`
- normalize exchange-specific payloads into `QuoteL1`
- fan out normalized quotes to interested worker runtimes

Rules:
- market data lives outside UI
- market data lives outside execution layer
- no demo/fallback transport
- no symbol-only subscription API at internal boundaries
- no shared strategy cache inside market data service
- worker-local last quote cache belongs to worker runtime

Core units:
- exchange-specific public WS connector
- market data subscription router
- quote normalizer
- quote distribution hub

### 2.3 Instrument Registry

Responsibilities:
- load available instruments per exchange
- expose instrument search/filter for UI
- store canonical instrument identity
- prevent ambiguity between spot, linear perp, futures, inverse, etc.

Rules:
- internal system must use `InstrumentId`
- plain strings like `BTCUSDT` are not valid business identifiers
- websocket routing metadata belongs to the registered instrument
- UI must expose only two common instrument classes: `spot` and `perpetual`
- exchange-specific market types must be mapped into those UI classes without losing real internal identity

### 2.4 Worker Layer

Responsibilities:
- each worker is an isolated trading runtime
- receives one `WorkerTask`
- subscribes to market data for task instruments
- consumes normalized `QuoteL1`
- publishes `WorkerEvent`
- exposes current `WorkerState`

Rules:
- one runtime class for all workers
- no duplicated worker-specific classes
- runtime must be keyed by `worker_id`
- worker receives only quotes for its own instruments

### 2.5 Execution Layer

Responsibilities:
- define exchange-specific order routing adapters
- define future private/trading websocket transport contracts
- receive execution intents from worker runtime
- publish execution acknowledgements/fills/events back to runtime

Rules:
- architecture only in this phase
- no REST legacy routing model
- no old mixed execution paths

## 3. Data Contracts

## 3.1 Instrument Identity Model

Use three contracts instead of one overloaded identifier.

### InstrumentKey

Identity only:
- `exchange`
- `market_type`
- `symbol`

### InstrumentSpec

Trading and listing metadata:
- `base_asset`
- `quote_asset`
- `contract_type`
- `settle_asset`
- `price_precision`
- `qty_precision`
- `min_qty`
- `min_notional`

### InstrumentRouting

Transport and execution routing:
- `ws_channel`
- `ws_symbol`
- `order_route`

### InstrumentId

Composite canonical object:
- `key: InstrumentKey`
- `spec: InstrumentSpec`
- `routing: InstrumentRouting`

Rule:
- every quote, subscription, task, and execution intent references `InstrumentId`
- `InstrumentKey` must stay stable if routing or listing metadata changes

### UI instrument type

UI is allowed to work only with:
- `spot`
- `perpetual`

Important:
- this is a UI classification only
- internal runtime still keeps real exchange-specific market identity
- mapping examples:
  - `spot` -> `spot`
  - `linear_perp` -> `perpetual`
  - `inverse_perp` -> `perpetual`

## 3.2 QuoteL1

Purpose:
- normalized L1 top-of-book quote

Required fields:
- `instrument_id`
- `bid`
- `ask`
- `bid_qty`
- `ask_qty`
- `ts_exchange`
- `ts_local`
- `source`

## 3.3 WorkerTask

Purpose:
- immutable runtime task definition coming from UI

Required fields:
- `worker_id`
- `left_instrument`
- `right_instrument`
- `entry_threshold`
- `exit_threshold`
- `target_notional`
- `step_notional`
- `execution_mode`
- `run_mode`

## 3.4 WorkerState

Purpose:
- current worker lifecycle snapshot for UI and supervision

Required fields:
- `worker_id`
- `status`
- `current_pair`
- `last_error`
- `started_at`
- `stopped_at`
- `metrics`

## 3.5 WorkerEvent

Purpose:
- unified event envelope from worker runtime to UI/logging/monitoring

Required fields:
- `worker_id`
- `event_type`
- `timestamp`
- `payload`

## 4. Project Structure

```text
app/
  ui/
    __init__.py
    coordinator.py
  core/
    __init__.py
    execution/
      __init__.py
      adapter.py
      binance_usdm_adapter.py
      binance_usdm_trade_ws.py
      binance_usdm_user_data_stream.py
    events/
      __init__.py
      bus.py
    instruments/
      __init__.py
      registry.py
    logging/
      __init__.py
      logger_factory.py
    market_data/
      __init__.py
      service.py
    models/
      __init__.py
      execution.py
      instrument.py
      market_data.py
      workers.py
    workers/
      __init__.py
      manager.py
      runtime.py
```

### File roles

- `app/ui/coordinator.py`: bridge between GUI shell and backend services
- `app/core/models/instrument.py`: `InstrumentId` contract
- `app/core/models/execution.py`: `ExecutionOrderRequest`, `ExecutionOrderResult`
- `app/core/models/market_data.py`: `QuoteL1` contract
- `app/core/models/workers.py`: `WorkerTask`, `WorkerState`, `WorkerEvent`
- `app/core/instruments/registry.py`: canonical instrument storage and lookup
- `app/core/market_data/service.py`: public WS orchestration and quote fan-out
- `app/core/workers/manager.py`: create/start/stop worker runtimes
- `app/core/workers/runtime.py`: single worker runtime implementation
- `app/core/execution/adapter.py`: execution adapter contracts for private/trading WS
- `app/core/execution/binance_usdm_trade_ws.py`: Binance USD-M trade websocket transport
- `app/core/execution/binance_usdm_adapter.py`: Binance USD-M execution adapter over WS transport
- `app/core/execution/binance_usdm_user_data_stream.py`: Binance USD-M user data stream for order lifecycle events
- `app/core/events/bus.py`: lightweight event bus for worker and system events
- `app/core/logging/logger_factory.py`: contextual logger creation with `worker_id`

## 5. Instrument Selection Pipeline

1. User selects exchange in UI.
2. UI requests instruments from `InstrumentRegistry`.
3. Registry returns a list of canonical `InstrumentId` entries.
4. User selects an instrument from that list.
5. UI stores selected `InstrumentId`, not a plain symbol.
6. UI creates `WorkerTask` using `left_instrument` and `right_instrument`.
7. `WorkerManager` starts runtime using those exact instrument identifiers.
8. `MarketDataService` subscribes using `instrument.ws_channel` and `instrument.ws_symbol`.

Result:
- no ambiguity between different markets sharing the same display symbol

## 6. Price Stream Architecture

### Target flow

1. `MarketDataService` asks exchange connector for public WS session.
2. Connector subscribes to L1 order book using canonical `InstrumentId`.
3. Raw exchange payload is normalized into `QuoteL1`.
4. `MarketDataService` routes the quote to registered worker subscribers.
5. `WorkerRuntime` consumes only quotes for its own left/right instruments.
6. `WorkerRuntime` updates internal state and emits `WorkerEvent`.
7. UI receives only aggregated `WorkerState` and `WorkerEvent`.

### Rules

- UI does not consume each tick
- no direct quote path from WS connector to UI
- quote normalization happens before worker delivery
- `MarketDataService` must not become a business-state cache
- worker runtime owns its own last quote cache for subscribed instruments

### Exchange connector contract

Required public market data connector methods:
- `connect()`
- `subscribe_l1(instrument)`
- `unsubscribe_l1(instrument)`
- `on_quote(callback)`
- `close()`

Required normalizer contract:
- `raw payload -> QuoteL1`

## 7. Worker Architecture

### Start flow

1. UI builds `WorkerTask`.
2. UI sends task to `WorkerManager.create_worker(task)`.
3. `WorkerManager` allocates one `WorkerRuntime` keyed by `worker_id`.
4. `WorkerRuntime.start()` registers interest in both instruments.
5. `MarketDataService` begins or reuses subscriptions.
6. Runtime receives quotes and emits lifecycle events.

### Stop flow

1. UI calls `WorkerManager.stop_worker(worker_id)`.
2. Runtime unsubscribes from quote delivery.
3. Runtime emits stop event and final state.
4. `MarketDataService` drops exchange subscription only when no workers need it.

### Scaling rule

- all workers use the same `WorkerRuntime` class
- worker identity is always `worker_id`
- no per-worker custom code paths
- runtime models must remain process-friendly and serialization-friendly
- no Qt objects inside runtime
- no direct UI references inside runtime

## 8. Logging Architecture

Must log:
- worker creation with `worker_id`
- worker start and stop
- instrument subscription requests
- first quote received per instrument per worker
- market data transport errors
- execution routing decisions
- runtime exceptions

Logging requirements:
- all log records include contextual identifiers
- `worker_id` is mandatory for worker-related logs
- instrument logs should include exchange and symbol

## 9. Execution Architecture

Target shape:
- `ExecutionAdapter` is exchange-specific
- adapter uses private/trading websocket transport
- adapter receives canonical execution requests tied to `InstrumentId`
- adapter returns acknowledgements/fills/events to worker runtime

Implemented now:
- Binance USD-M execution uses Binance WebSocket API trade transport
- signed requests are sent ad hoc per request using `apiKey`, `timestamp`, `recvWindow`, `signature`
- current supported methods:
  - `order.place`
  - `order.cancel`
  - `order.status`
- Binance USD-M user data stream normalizes:
  - `ORDER_TRADE_UPDATE`
  - other futures user stream events as generic execution events
- `Close positions` in account cards now routes order placement through this WS execution transport

UNDECIDED:
- exact fill event schema for worker runtime
- retry model beyond explicit reconnect per request path

## 10. Explicit Non-Goals / No Legacy

Do not add:
- demo mode
- legacy REST execution routing
- fallback quote transports
- symbol-only identifiers
- mixed UI+trading logic classes
- reconnect chaos copied from old system

## 11. Immediate TODOs

- TODO: define concrete exchange connector interfaces for public WS
- TODO: define instrument loading source per exchange
- TODO: connect normalized execution stream events to worker state/event bus
- TODO: move worker start/stop flow from UI shell onto `WorkerManager`
- TODO: choose initial runtime host model; design must remain compatible with process isolation from the start

## 12. Done Criteria

This architecture phase is complete when:
- UI is separated from trading core
- instruments are canonical and unambiguous
- market data is separated from UI and worker management
- worker model supports multiple runtimes without code duplication
- repository contains a clean scaffold for further implementation
