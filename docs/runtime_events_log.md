# Логи runtime

## По умолчанию

- `logs/session_trace.log` — основной подробный текстовый лог всего приложения
- `logs/runtime_events.log` — отдельный JSONL-журнал `emit_event`

Оба файла сбрасываются при запуске через `py 1.py`.

## Что смотреть первым

- Для разбора зависаний, стартов, reconnection, hedge/entry/exit логики — `logs/session_trace.log`
- Для компактного следа runtime-событий по `worker_id` — `logs/runtime_events.log`

## Переменные окружения

| Переменная | Эффект |
|------------|--------|
| `SPREAD_SNIPER_SESSION_TRACE_LOG=0` | Не писать подробный текстовый лог в `session_trace.log` |
| `SPREAD_SNIPER_EVENTS_LOG=0` | Не писать JSONL-события в `runtime_events.log` |
| `SPREAD_SNIPER_EVENTS_LOG_EXCLUDE=a,b` | Исключить события, где `event_type` содержит `a` или `b` |
| `SPREAD_SNIPER_EVENTS_LOG_ALL=1` | Писать все типы событий, включая шумные |
| `SPREAD_SNIPER_EVENTS_LOG_COMPACT=0` | Не ужимать payload: оставлять `raw` и `null` |

## Что отфильтровано в `runtime_events.log`

По умолчанию туда не пишется шум:

- тики котировок
- `rest_poll_*`
- сырые `*_order_event`
- дубликаты `*_sent`, `*_ack`, `*_fill`
- health-события execution stream теперь пишутся, потому что они важны для отладки reconnect/auth

## Формат `runtime_events.log`

Первая строка после сброса — маркер схемы:

```json
{"_schema":"runtime_events_v1","note":"timestamp_ms UTC-ish; event_type + payload; no quote ticks; no raw exchange blobs"}
```

Дальше одна строка = один JSON:

```json
{"worker_id":"...","event_type":"entry_done","timestamp":1234567890,"payload":{"cycle_id":1}}
```
