# Futures Spread Scanner v2 — паспорт разработчика

Этот файл — полная карта нового сканера `v2`.

Цель документа:
- быстро ввести разработчика или AI-агента в проект;
- объяснить, как устроен новый сканер;
- показать, где что лежит;
- зафиксировать жизненный цикл вкладок, runtime-слоёв и UI;
- дать понятный путь для добавления новых runtime и новых виджетов.

## 1. Что это за приложение

`v2` — это новый самостоятельный сканер внутри проекта, построенный на новой runtime-архитектуре.

Сейчас это уже почти полноценное отдельное приложение:
- своё окно;
- свои вкладки;
- рабочие runtime-цепочки;
- конструктор вкладок;
- режим редактирования вкладок;
- постоянная вкладка `Уведомления`;
- Telegram-подключение;
- собственное хранилище схем вкладок;
- собственные common-модули, storage, settings, logs и assets.

Точка входа:
- `scanner_v2.py`

Основной пакет:
- `app/futures_spread_scanner_v2/`

Текущий важный статус:
- `v2` полностью отвязан от старого scanner package `app.scanners.futures_spread_scanner.*`;
- `v2` также отвязан от общих модулей `app/ui/*`, `app/core/*`, `app/exchange_info_base/*`;
- весь код, нужный для работы `v2`, теперь живёт внутри `app/futures_spread_scanner_v2/`.

## 2. Главные архитектурные принципы

### 2.1. Вкладка хранится как схема, а не как живой QWidget

В `v2` рабочая вкладка существует в двух формах:

1. **Схема вкладки**
   - хранится на диске;
   - описывает состав runtime-узлов и параметры вкладки;
   - не содержит живых Qt-объектов.

2. **Живая session**
   - создаётся из схемы;
   - содержит runtime-объекты и виджеты;
   - живёт только в памяти.

Это ключевое правило всей системы.

### 2.2. Редактирование = пересборка по новой схеме

При редактировании мы не мутируем рабочую вкладку “на месте”.

Правильный flow:
- берём существующую схему;
- открываем её в конструкторе;
- пользователь правит черновик;
- по `Применить` старая схема заменяется;
- рабочая session пересобирается заново;
- по `Отмена` старая рабочая вкладка возвращается как была.

### 2.3. Shared runtime и local runtime разделены

В системе есть два класса runtime:

- **Shared runtime**
  - общие на всё приложение `v2`;
  - обычно это сетевые source runtime бирж;
  - пример: `BinancePerpRuntime`, `BybitPerpRuntime`.

- **Local runtime**
  - принадлежат конкретной вкладке;
  - создаются из схемы вкладки;
  - пример: `StarterPairsRuntime`, `SpreadRuntime`, `RateDeltaRuntime`, `WorkspaceHeaderRuntime`, `WorkspaceRuntime`.

При удалении вкладки удаляются только local runtime этой вкладки.
Shared runtime не удаляются.

### 2.4. `v2` полностью автономен внутри своей папки

Это правило уже выполнено.

`v2` больше не импортирует код из:
- `app.scanners.futures_spread_scanner.*`
- `app/ui/*`
- `app/core/*`
- `app/exchange_info_base/*`

Если в будущем в `v2` появляется такой импорт, это считается архитектурной регрессией.

## 3. Карта папки `app/futures_spread_scanner_v2`

### Корневые файлы

- `window.py`
  - окно нового сканера.

- `workspace_tabs.py`
  - контейнер вкладок;
  - создаёт рабочие вкладки, `+`-вкладку-конструктор и фиксированную вкладку `Уведомления`;
  - отвечает за enter edit mode / apply / cancel / delete.

- `workspace_tab.py`
  - одна рабочая вкладка;
  - собирает layout колонок из `WorkspaceSession`;
  - сохраняет state вкладки: сортировку, закладки, `топ`.

- `constructor_tab.py`
  - вкладка-конструктор;
  - используется и для создания новой вкладки, и для редактирования существующей.

- `constructor_draft.py`
  - черновик конструктора;
  - хранит название вкладки, автоимя, набор runtime-узлов и их selections;
  - валидирует, готова ли схема к `Создать/Применить`.

- `definitions.py`
  - формальные dataclass-схемы;
  - содержит `WorkspaceDefinition`, `WorkspaceNodeDefinition`;
  - умеет строить схему вкладки из `ConstructorDraft`.

- `storage.py`
  - JSON-хранилище схем вкладок `v2`.

- `manager.py`
  - главный менеджер вкладок;
  - загружает/сохраняет схемы;
  - создаёт и кеширует `WorkspaceSession`;
  - удаляет вкладки и их session.

- `session.py`
  - сборщик живой session из схемы;
  - связывает runtime-узлы между собой;
  - создаёт header/runtime graph и column bindings.

- `notifications_tab.py`
  - постоянная фиксированная вкладка уведомлений;
  - сейчас содержит Telegram-подключение.

- `telegram_dialog.py`
  - окно подключения/редактирования Telegram-бота;
  - включает тестовую отправку сообщения.

- `settings.py`
  - отдельный settings store для `v2`-специфичных мелких настроек;
  - сейчас хранит `telegram_credential_ref`.

- `catalog.py`
  - локальный runtime-catalog `v2`;
  - описывает доступные starter/base/output runtime для конструктора.

- `PASSPORT.md`
  - этот документ.

### Папки верхнего уровня

- `common/`
  - локальные общие модули `v2`;
  - внутри лежат собственные аналоги бывших общих зависимостей приложения.

- `runtime/`
  - runtime-слой нового сканера.

- `views/`
  - UI-слой нового сканера.

- `assets/`
  - локальные ассеты `v2`.

- `data/`
  - локальные JSON/secure-store файлы `v2`.

- `logs/`
  - локальные логи `v2`.

## 4. Пакет `common/`

Папка:
- `app/futures_spread_scanner_v2/common/`

Содержит локальные самостоятельные модули, чтобы `v2` не зависел от внешнего `app/ui` и `app/core`.

Сейчас там:
- `i18n.py`
- `theme.py`
- `workspace_header.py`
- `price_format.py`
- `volume_parse.py`
- `global_focus.py`
- `secure_credential_store.py`
- `telegram_bot.py`
- `brand_header.py`
- `logger.py`
- `__init__.py`

### Назначение

- `i18n.py`
  - локальные переводы `v2`.

- `theme.py`
  - локальная тема, палитра и app stylesheet `v2`.

- `workspace_header.py`
  - общий верхний bar с `Топ`, `Обновить`, `Добавить`, шестерёнкой и статусом.

- `price_format.py`
  - компактное форматирование цен.

- `volume_parse.py`
  - парсинг и форматирование поля `топа`.

- `global_focus.py`
  - глобальный blur/hover/click affordance для `v2`.

- `secure_credential_store.py`
  - локальный secure store `v2` для exchange/telegram credentials.

- `telegram_bot.py`
  - локальный Qt-клиент Telegram API.

- `brand_header.py`
  - локальный бренд-хедер и app icon.

- `logger.py`
  - локальная logging-инфраструктура `v2`.

## 5. Пакет `runtime/`

Папка:
- `app/futures_spread_scanner_v2/runtime/`

### `contracts.py`
Общие dataclass-контракты runtime-слоя:
- snapshots;
- row state;
- формальные контракты для perp/output runtime.

### `perp_runtime.py`
Source/base runtime для бирж.

Сейчас там живут:
- `BasePerpRuntime`
- `BinancePerpRuntime`
- `BybitPerpRuntime`
- shared getters:
  - `get_shared_binance_perp_runtime(...)`
  - `get_shared_bybit_perp_runtime(...)`

Назначение:
- ходят в сеть;
- получают market data;
- формируют `PerpSnapshot`;
- применяют `top`;
- хранят рабочий universe пар.

### `market_backend.py`
Локальный backend-агрегатор `v2` для market-data слоя.

Назначение:
- endpoint resolution для Binance/Bybit;
- tradable symbol fetch;
- full snapshot fetch;
- price snapshot polling/cache;
- преобразование сетевых ответов в `ExchangeCell`.

### `endpoint_registry.py`
Локальный реестр endpoint’ов, используемых `v2`.

Сейчас там лежат endpoint-spec’ы только для того, что реально нужно новому сканеру:
- Binance futures
- Bybit linear futures

### `market_helpers.py`
Локальные market/helper-сущности `v2`.

Сейчас там:
- `ExchangeCell`
- `resolve_price(...)`
- `select_low_high_exchange_ids(...)`
- `format_spread_pct(...)`

### `funding_utils.py`
Локальные funding/helper-функции `v2`.

Сейчас там:
- `funding_rate_to_percent_signed(...)`
- `ms_until_next_funding(...)`
- `format_countdown(...)`

### `starter_runtime.py`
`StarterPairsRuntime`

Назначение:
- собирает реальные пары из base runtime;
- строит видимый список строк;
- управляет bookmarks;
- управляет поиском;
- выдаёт starter snapshot.

### `output_runtimes.py`
Derived output runtime.

Сейчас там:
- `RateDeltaRuntime`
- `SpreadRuntime`

Назначение:
- не ходят в сеть;
- подписываются на starter + base runtime;
- считают локальный вычислительный результат;
- выдают output snapshot.

### `comparison_runtime.py`
Слой сравнения base runtime между собой.

Назначение:
- определяет `low / high / same`;
- считает accent для цены.

### `workspace_runtime.py`
Главный runtime вкладки.

Назначение:
- знает состав base runtime текущей вкладки;
- хранит общую sorting policy;
- хранит layout/stretch policy по ролям;
- даёт оркестрацию на уровне всей вкладки.

### `header_runtime.py`
`WorkspaceHeaderRuntime`

Назначение:
- top-volume limit;
- статус загрузки;
- количество пар;
- refresh/add/settings-related действия.

### `view_models.py`
ViewModel-слой между runtime и view.

Сейчас там:
- `PerpColumnViewModel`
- `StarterPairsViewModel`
- `OutputColumnViewModel`
- `WorkspaceHeaderViewModel`

## 6. Пакет `views/`

Папка:
- `app/futures_spread_scanner_v2/views/`

Разделение такое:

### Runtime widgets
- `header_view.py`
- `starter_view.py`
- `base_exchange_view.py`
- `output_view.py`

Это “наружные” виджеты колонок, которые живут на runtime/view-model.

### Column/canvas drawing
- `starter_column.py`
- `base_exchange_column.py`
- `output_column.py`

Это низкоуровневые отрисовщики строк внутри колонок.

### `common.py`
Общие UI-helper’ы:
- иконки;
- общие стили;
- маленькие общие функции для новых виджетов `v2`.

## 7. Хранилище и постоянный state

### 7.1. Схемы вкладок

Файл:
- `app/futures_spread_scanner_v2/data/futures_spread_scanner_v2_workspaces.json`

Хранит:
- список вкладок;
- active workspace;
- title;
- top-volume limit;
- runtime nodes;
- sort state;
- bookmark order starter-блоков;
- layout policy.

### 7.2. Настройки `v2`

Файл:
- `app/futures_spread_scanner_v2/data/futures_spread_scanner_v2_settings.json`

Сейчас хранит:
- `telegram_credential_ref`

### 7.3. Secure credentials

Файл:
- `app/futures_spread_scanner_v2/data/credentials.secure.json`

Хранит:
- exchange credentials;
- Telegram credentials.

### 7.4. Логи `v2`

Папка:
- `app/futures_spread_scanner_v2/logs/`

Основной лог:
- `app/futures_spread_scanner_v2/logs/scanner_v2_trace.log`

Также там могут жить:
- `session_trace.log`
- `runtime_events.log`
- другие локальные логи `v2`

## 8. Assets

Папка:
- `app/futures_spread_scanner_v2/assets/`

Сейчас локально лежат, например:
- `app/futures_spread_scanner_v2/assets/logos/alarm.svg`
- `app/futures_spread_scanner_v2/assets/logos/telegram.svg`

Правило:
- если `v2` нужен ассет, он должен лежать внутри `v2/assets/`.
- Не тянуть графику из старых путей или из общих директорий проекта.

## 9. Как создаётся вкладка

### Шаг 1. Пользователь собирает draft

Во вкладке-конструкторе пользователь строит черновик:
- название;
- starter/base/output узлы;
- top-volume limit.

Черновик живёт в:
- `ConstructorDraft`

### Шаг 2. Draft превращается в схему

Функция:
- `build_workspace_definition_from_draft(...)`

Создаёт:
- `WorkspaceDefinition`

Схема содержит:
- `workspace_id`
- `title`
- `top_volume_limit`
- `nodes`
- sort state
- stretch policy

### Шаг 3. Manager сохраняет схему

`WorkspaceManager`:
- либо создаёт новую вкладку;
- либо обновляет существующую;
- пишет всё в storage.

### Шаг 4. Session factory строит runtime graph

`WorkspaceSessionFactory` -> `WorkspaceSession`

Session:
- связывает starter с base runtime;
- связывает output с ближайшим левым starter и его base runtime;
- создаёт header runtime;
- создаёт column bindings.

### Шаг 5. Рабочая вкладка собирает UI

`FuturesSpreadWorkspaceTab`:
- получает session;
- создаёт нужные widgets;
- раскладывает их по ролям и bindings.

## 10. Как работает редактирование вкладки

Flow edit-mode:

1. В рабочей вкладке нажимается шестерёнка.
2. `workspace_tabs.py` открывает editor для этого `workspace_id`.
3. Исходная рабочая вкладка не уничтожается сразу:
   - она уходит в suspended-state;
   - продолжает существовать до `Применить` или `Отмена`.
4. Открывается `constructor_tab`, уже загруженный из текущей схемы вкладки.

### `Отмена`
- editor закрывается;
- исходная suspended вкладка возвращается как была;
- никакие изменения не применяются.

### `Применить`
- draft превращается в новую схему;
- manager сохраняет схему;
- старая session удаляется;
- создаётся новая рабочая session;
- вкладка пересобирается.

### `Удалить`
- удаляется схема вкладки из storage;
- живая session уничтожается;
- вкладка исчезает из UI.

## 11. Runtime-группы и зависимости

Во вкладке может быть несколько starter-групп.

Правило:
- `base` относится к ближайшему левому `starter`;
- `output` относится к ближайшему левому starter, у которого уже есть starter + base.

Это правило зафиксировано на уровне схемы через `depends_on`.

### Пример

Схема:
- starter_1
- base_1
- base_2
- output_1
- output_2
- starter_2
- base_3
- output_3

Тогда:
- `output_1`, `output_2` работают от `starter_1`;
- `output_3` работает от `starter_2`.

## 12. Layout policy

Layout не должен зависеть от жёстко захардкоженного количества колонок.

В `WorkspaceDefinition` хранится:
- `column_stretch_by_role`

Сейчас по умолчанию:
- `starter = 14`
- `base = 20`
- `output = 8`

Идея:
- layout считает stretch по ролям;
- число колонок может меняться, правило остаётся стабильным.

## 13. Что сейчас уже умеет рабочая вкладка

Рабочая вкладка `v2` сейчас умеет:
- header runtime с `Топ по объёму`, refresh и шестерёнкой;
- starter:
  - пары
  - поиск
  - закладки
  - reorder bookmark drag
- base runtime:
  - Binance perpetual futures
  - Bybit perpetual futures
- output runtime:
  - `Дел. ставки`
  - `Спред`
- сортировки;
- сохранение сортировки;
- сохранение bookmarks;
- сохранение порядка bookmarks;
- сохранение top-volume;
- edit mode;
- fixed `Уведомления`;
- Telegram connect/edit/test/remove.

## 14. Как добавить новый base runtime

Пример: новая биржа или новый market type.

### Шаги

1. Реализовать новый runtime в:
   - `runtime/perp_runtime.py`

2. Он должен соответствовать `BasePerpRuntime` contract:
   - отдавать snapshot нужного типа;
   - поддерживать refresh;
   - поддерживать top-volume limit;
   - работать как source runtime.

3. Добавить shared getter:
   - по аналогии с `get_shared_binance_perp_runtime(...)`

4. Добавить mapping `runtime_id -> runtime` в:
   - `WorkspaceSession._shared_base_runtime_for(...)`

5. Добавить выбор в конструктор:
   - если это новый exchange/asset-type, он должен появиться в `ConstructorDraft`/constructor UI.

6. UI base-колонки менять не нужно, если snapshot соответствует контракту.

### Важное правило
Новая биржа должна вставать в:
- тот же `BasePerpRuntime` contract;
- ту же `PerpColumnViewModel`;
- тот же `BaseExchangeRuntimeWidget`.

Если для новой биржи нужно менять UI — это сигнал, что контракт сделан плохо.

## 15. Как добавить новый output runtime

Пример:
- новый вычислительный блок;
- новый spread variant;
- новый notification-like output later.

### Шаги

1. Реализовать derived runtime в:
   - `runtime/output_runtimes.py`

2. Он должен:
   - не ходить в сеть;
   - подписываться на starter + нужные base runtime;
   - считать свой snapshot;
   - отдавать sort values.

3. Добавить его сборку в:
   - `WorkspaceSession._build_output_runtime(...)`

4. Добавить его выбор в конструктор.

5. Если контракт совпадает с уже существующим `OutputSnapshot`, новый отдельный widget не нужен.

### Важное правило
Output runtime должен жить как отдельный runtime, а не как “вычисления прямо во view-model”.

Это уже принятая архитектура `v2`.

## 16. Как добавить новый постоянный системный tab

Сейчас такой tab уже есть:
- `Уведомления`

Если нужен ещё один fixed tab:

1. Создать отдельный widget в корне `v2`.
2. Добавить его в `workspace_tabs.py`.
3. Если он должен быть визуально закреплён справа:
   - сделать его скрытым tab внутри `QTabWidget`;
   - открыть через отдельную corner button.

Это уже рабочий pattern в `v2`.

## 17. Что здесь считается локальным модулем

Для `v2` локальным считается всё, что находится внутри:
- `app/futures_spread_scanner_v2/`

Сюда входят:
- `common/*`
- `runtime/*`
- `views/*`
- `definitions.py`
- `storage.py`
- `manager.py`
- `session.py`
- `catalog.py`
- `constructor_*`
- `notifications_tab.py`
- `telegram_dialog.py`
- `settings.py`
- `assets/*`
- `data/*`
- `logs/*`

Это и есть зона полной автономности нового сканера.

## 18. Что важно не ломать

1. **Не превращать вкладку обратно в “живой QWidget-state вместо схемы”.**
   - Вкладка должна храниться как `WorkspaceDefinition`.

2. **Не мутировать runtime graph по кускам без необходимости.**
   - Для edit-mode правильнее полная пересборка.

3. **Не смешивать source runtime и derived runtime.**
   - Base runtime ходят в сеть.
   - Output runtime считают локально.

4. **Не хранить чувствительные данные в открытом виде.**
   - credentials должны идти через `common/secure_credential_store.py`.

5. **Не тащить внешние app-модули обратно в `v2`.**
   - Если нужен перенос, переносить код сюда отдельным самостоятельным модулем.
   - Любой новый импорт из `app.scanners.*`, `app.ui.*`, `app.core.*`, `app.exchange_info_base.*` считается ошибкой дизайна.

6. **Не тянуть ассеты и служебные файлы наружу.**
   - Ассеты должны лежать в `v2/assets/`.
   - storage/settings/logs должны жить в `v2/data/` и `v2/logs/`.

## 19. Где начинать разбираться новому разработчику

Рекомендуемый порядок чтения:

1. `scanner_v2.py`
2. `window.py`
3. `workspace_tabs.py`
4. `workspace_tab.py`
5. `constructor_tab.py`
6. `constructor_draft.py`
7. `definitions.py`
8. `manager.py`
9. `session.py`
10. `common/`
11. `runtime/`
12. `views/`

Если задача про Telegram/уведомления:
- `notifications_tab.py`
- `telegram_dialog.py`
- `settings.py`
- `common/secure_credential_store.py`
- `common/telegram_bot.py`

Если задача про хранение вкладок:
- `definitions.py`
- `storage.py`
- `manager.py`

Если задача про новый тип runtime:
- `runtime/contracts.py`
- `runtime/perp_runtime.py`
- `runtime/output_runtimes.py`
- `session.py`

## 20. Короткий operational summary

Если нужно “понять проект за минуту”, то формула такая:

- `WorkspaceDefinition` — сохранённая схема вкладки.
- `WorkspaceManager` — управляет схемами и session.
- `WorkspaceSession` — строит живой runtime graph из схемы.
- `workspace_tab.py` — рисует рабочую вкладку из session.
- `constructor_tab.py` — редактирует схему.
- `common/*` — локальная инфраструктура `v2`.
- `runtime/*` — данные и вычисления.
- `views/*` — отображение колонок.
- `notifications_tab.py` — фиксированная системная вкладка.

## 21. Что можно делать дальше без архитектурной ломки

На этой базе уже можно безопасно:
- добавлять новые base runtime;
- добавлять новые output runtime;
- развивать уведомления;
- добавлять notification runtime внутри fixed tab `Уведомления`;
- расширять constructor;
- вводить экспорт/импорт схем вкладок;
- добавить preset system поверх схем;
- при необходимости выносить `v2` в вообще отдельный репозиторий/приложение.

---

Если после изменений в `v2` поведение стало непонятным или “что-то живёт не там”:
- сначала сверить это с этим паспортом;
- потом смотреть `app/futures_spread_scanner_v2/logs/scanner_v2_trace.log`;
- и только потом чинить код.
