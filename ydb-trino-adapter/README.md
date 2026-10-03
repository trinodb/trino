# YDB Trino Adapter

План развития и покрытие Trino connector tests: [ROADMAP.md](ROADMAP.md).

## Совместимость

Адаптер собирается для Trino 483 и требует 64-битную Java 25 версии 25.0.1
или новее в линейке Java 25. Версия Java 25 задана в `pom.xml` и в CI workflow
модуля.

# Инструкция по сборке

Из каталога `ydb-trino-adapter`:

```bash
bash start.sh
```

Скрипт собирает плагин и runtime-зависимости в `examples/trino/plugin`,
затем перезапускает пример через `examples/docker-compose.yml`.

## Запуск Trino CLI

```bash
docker-compose -f examples/docker-compose.yml exec trino trino
```

## Каталог и схема

Имя каталога задаёт файл конфигурации Trino. В примере
[`local.properties`](examples/trino/etc/catalog/local.properties) каталог `local`
подключён к базе YDB `/local`. Для другой базы создайте отдельный файл каталога
с её JDBC URL. Внутри каталога адаптер показывает схему `default`:

```sql
SELECT * FROM local.default.orders;
```

Прежние обращения `catalog.ydb.table` нужно заменить на `catalog.default.table`.

## Создание таблиц

YDB требует первичный ключ, поэтому `CREATE TABLE` и `CREATE TABLE AS` должны
задавать упорядоченное свойство `primary_key`. Адаптер не добавляет скрытый ключ:

```sql
CREATE TABLE events (tenant bigint, event_id bigint, payload varchar)
WITH (primary_key = ARRAY['tenant', 'event_id']);

CREATE TABLE events_copy
WITH (primary_key = ARRAY['tenant', 'event_id'])
AS SELECT tenant, event_id, payload FROM events;
```

Имена ключей в CTAS относятся к выходным столбцам запроса. Отсутствующий,
пустой, повторяющийся или неизвестный `primary_key` отклоняется.
Обычный INSERT использует временную таблицу с native-типами и отдельным
Serial-ключом; публикация в целевую таблицу выполняется одним `INSERT ... SELECT`.
`TestYdbCreateTable` проверяет этот режим на production client, включая ошибку
повторяющегося ключа и очистку временных таблиц.

## UPDATE, DELETE и MERGE

Полностью передаваемые в YDB UPDATE/DELETE возвращают число строк через YQL
`RETURNING` в том же запросе. Изменение физического первичного ключа явно
отклоняется с `NOT_SUPPORTED`: [ограничение YQL UPDATE](https://ydb.tech/docs/en/yql/reference/syntax/update).

Для MERGE и UPDATE/DELETE, которые Trino выполняет построчно, нужен явный режим:

```properties
merge.non-transactional-merge.enabled=true
```

Либо `SET SESSION local.non_transactional_merge = true`. Такой запрос
**не атомарен целиком**: уже завершённые пакеты остаются после ошибки или отмены.
Каждый пакет размером не более `write.batch-size` использует собственные
соединение и транзакцию; операции сохраняют порядок, nullable-составной ключ
сопоставляется без потери NULL, а запись сохраняет native-типы.
Ошибка откатывает текущий пакет; соединения и statements закрываются до
возврата из обработки пакета. Автоматического replay нет, query/task retry запрещён.
Ограничение числа writer tasks в Trino 483 не обеспечивает атомарность MERGE.

## JOIN pushdown

По умолчанию JOIN выполняет Trino. Для пробного pushdown задайте
`join_pushdown_enabled=true` в сессии каталога. Адаптер передаёт YDB
`INNER`, `LEFT`, `RIGHT` и `FULL JOIN` только по равенству исходных столбцов
с совместимыми отображениями: `Bool`, знаковые и беззнаковые целые,
`Float`/`Double`, текст, байты, даты и timestamps. Поддерживается составной ключ.
`NULL` и `NaN` не совпадают по обычному равенству; `-0.0` совпадает с `0.0`.
Для YQL ключи с `NaN` заменяются на `NULL`, а нули нормализуются.
Native `Decimal`, null-safe equality, неравенства, вычисляемые ключи и
неподдерживаемые преобразования остаются в Trino.
См. [правила YQL JOIN](https://ydb.tech/docs/ru/yql/reference/syntax/select/join).

## Числовые типы

`Uint8`, `Uint16`, `Uint32` отображаются соответственно в `smallint`,
`integer`, `bigint`. `Uint64` отображается в `decimal(20,0)` и сохраняет
весь диапазон до `18446744073709551615`, без превращения старшего бита в знак.
Запись проверяет границы unsigned-типа. `Decimal` записывается с точными
precision и scale, включая `NULL`; максимальная поддерживаемая precision — 35.
Native decimal `NaN` и бесконечности не представимы в Trino `decimal`.

Целочисленные и decimal `sum`/`avg`, а также группировки и `min`/`max`
для floating-point выполняет Trino там, где YQL отличается по переполнению,
порядку `NaN` или знаковому нулю. Top-N явно учитывает SQL `NULL` и порядок
`NaN`, используя типизированные floating-point значения в YQL.

## Текст и байты

Вычисления целочисленного сложения, вычитания, умножения и отрицания остаются
в Trino: переполнение в YQL не обязано приводить к той же ошибке. Деление и остаток
передаётся в YDB только для целочисленного результата с ненулевым постоянным
делителем, отличным от `-1`: для минимального signed-значения YQL возвращает
NULL при `% -1`, а Trino — ноль. `strpos` считает позиции Unicode-символов и
сохраняет SQL `NULL`, а не заменяет его нулём.
`trim` остаётся в Trino: byte-oriented `String::Strip` не заменяет Unicode trim.

YDB `Text` отображается в Trino как `varchar`, а `Bytes` — как `varbinary` без
декодирования UTF-8. При создании таблиц адаптер использует типы `Text` и `Bytes`.

## Даты

Новые столбцы Trino `date` создаются как YDB `Date32`.
Существующие столбцы YDB `Date` и `Date32` читаются как Trino `date`.
Новые столбцы Trino `timestamp(3)` и `timestamp(6)` создаются как YDB `Timestamp64`.
Существующие YDB `Datetime`, `Datetime64`, `Timestamp` и `Timestamp64` читаются как Trino `timestamp(6)`;
исходная точность `timestamp(3)` в метаданных не сохраняется.
Чтение timestamps использует UTC, а не часовой пояс JVM. Запись дробных секунд
в `Datetime`/`Datetime64` отклоняется без молчаливого усечения.
При записи адаптер сохраняет native-тип из `TYPE_NAME` и не задаёт `forceSignedDatetimes`:
`Date`/`Date32` получают дни от эпохи, `Datetime`/`Datetime64` — секунды UTC, а `Timestamp` — `Instant` с микросекундами.
Для `Timestamp64` передаются типизированные SDK-значения. SDK 2.4.10 ошибочно
исключает допустимую верхнюю границу в фабрике `newTimestamp64`; для этой
точки адаптер сохраняет native-тип и Int64-микросекунды непосредственно
в протокольном значении. Это не добавляет Optional к NOT NULL столбцам.
В построчных UPDATE/DELETE собственный merge sink использует исходный
`JdbcTypeHandle` и те же native write mappings, что и обычная запись.
Date32- и Timestamp64-предикаты передаются в YDB только с представимыми
границами. Диапазон Timestamp64 в микросекундах от эпохи:
`[-4611669897600000000, 4611669811199999999]`.
Предикаты с выходящими за него значениями остаются в Trino, а запись таких
значений отклоняется. Предикаты для legacy temporal типов также остаются в Trino.

## JDBC-контексты

Для JDBC 2.4.1 адаптер задаёт `cacheConnectionsInDriver=false`: при одновременном
открытии и закрытии последнего соединения cache драйвера может вернуть уже
закрываемый контекст. Каждый JDBC connection поэтому владеет своим контекстом.
Это не retry и не replay записей. Параметры JDBC URL имеют приоритет над
properties; не включайте `cacheConnectionsInDriver=true` на этой версии драйвера.
Также задаётся `replaceJdbcInByYqlList=false`: оптимизированный параметр списка
в JDBC 2.4.1 не поддерживает типизированные SDK-значения Timestamp64.
Предикаты `IN` продолжают передаваться в YDB с отдельными параметрами.
Не включайте `replaceJdbcInByYqlList=true` в JDBC URL на этой версии драйвера.
