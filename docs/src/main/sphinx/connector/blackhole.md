# Black Hole connector

Primarily Black Hole connector is designed for high performance testing of
other components. It works like the `/dev/null` device on Unix-like
operating systems for data writing and like `/dev/null` or `/dev/zero`
for data reading. However, it also has some other features that allow testing Trino
in a more controlled manner. Metadata for any tables created via this connector
is kept in memory on the coordinator and discarded when Trino restarts.
Created tables are by default always empty, and any data written to them
is ignored and data reads return no rows.

During table creation, a desired rows number can be specified.
In such cases, writes behave in the same way, but reads
always return the specified number of some constant rows.
You shouldn't rely on the content of such rows.

## Configuration

Create `etc/catalog/example.properties` to mount the `blackhole` connector
as the `example` catalog, with the following contents:

```text
connector.name=blackhole
```

## Examples

Create a table using the blackhole connector:

```
CREATE TABLE example.test.nation AS
SELECT * from tpch.tiny.nation;
```

Insert data into a table in the blackhole connector:

```
INSERT INTO example.test.nation
SELECT * FROM tpch.tiny.nation;
```

Select from the blackhole connector:

```
SELECT count(*) FROM example.test.nation;
```

The above query always returns zero.

Create a table with a constant number of rows (500 * 1000 * 2000):

```
CREATE TABLE example.test.nation (
  nationkey BIGINT,
  name VARCHAR
)
WITH (
  split_count = 500,
  pages_per_split = 1000,
  rows_per_page = 2000
);
```

Now query it:

```
SELECT count(*) FROM example.test.nation;
```

The above query returns 1,000,000,000.

Length of variable length columns can be controlled using the `field_length`
table property (default value is equal to 16):

```
CREATE TABLE example.test.nation (
  nationkey BIGINT,
  name VARCHAR
)
WITH (
  split_count = 500,
  pages_per_split = 1000,
  rows_per_page = 2000,
  field_length = 100
);
```

The consuming and producing rate can be slowed down
using the `page_processing_delay` table property.
Setting this property to `5s` leads to a 5 second
delay before consuming or producing a new page:

```
CREATE TABLE example.test.delay (
  dummy BIGINT
)
WITH (
  split_count = 1,
  pages_per_split = 1,
  rows_per_page = 1,
  page_processing_delay = '5s'
);
```

(blackhole-sql-support)=

## SQL support

The connector provides {ref}`globally available <sql-globally-available>`,
{ref}`read operation <sql-read-operations>`, and supports the following
additional features:

- {doc}`/sql/insert`
- {doc}`/sql/update`
- {doc}`/sql/delete`
- {doc}`/sql/merge`
- {doc}`/sql/create-table`
- {doc}`/sql/create-table-as`
- {doc}`/sql/show-create-table`
- {doc}`/sql/drop-table`
- {doc}`/sql/comment`
- {doc}`/sql/create-schema`
- {doc}`/sql/drop-schema`
- {doc}`/sql/create-view`
- {doc}`/sql/show-create-view`
- {doc}`/sql/drop-view`

:::{note}
The connector discards all written data. While read operations are supported,
they return rows with all NULL values, with the number of rows controlled
via table properties.
:::
