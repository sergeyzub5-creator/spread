# Chart History

This package is the groundwork for historical spread charts.

## First target

- timeframe: `1m`
- chart type: line
- output series: spread percent, not raw price

## Storage rule

Store raw minute bars for each side separately:

- exchange
- market type
- symbol
- timeframe
- open time
- close price

Do not store spread as the source of truth on disk.
Build spread in memory from two aligned raw series.

## Why raw bars first

- the spread formula may change later
- we may add alternative modes (`last`, `mark`, `mid`)
- the same raw cache can be reused for many spread views

## Recommended first implementation

- `spot` -> regular `1m` kline close
- `perpetual` -> `mark` price kline close when available
- `delivery futures` -> regular `1m` kline close

## Load flow

1. Read cached raw bars from disk for both sides
2. Detect missing time range
3. Fetch missing `1m` bars from REST
4. Save merged raw bars back to cache
5. Align both sides by exact candle `open_time_ms`
6. Build spread line in memory

## Initial limits

- default first load: `1440` bars (`1 day` on `1m`)
- keep larger history on disk later if needed

## Live continuation

After initial REST history:

- keep the current open minute in memory only
- update it from live price feeds
- finalize it when the next minute starts

This keeps the chart responsive without hammering REST every second.
