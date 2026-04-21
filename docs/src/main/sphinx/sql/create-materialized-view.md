# CREATE MATERIALIZED VIEW

## Synopsis

```text
CREATE [ OR REPLACE ] MATERIALIZED VIEW
[ IF NOT EXISTS ] view_name
[ GRACE PERIOD interval ]
[ WHEN STALE ( INLINE | FAIL ) ]
[ COMMENT string ]
[ WITH properties ]
AS query
```

## Description

Create and validate the definition of a new materialized view `view_name` of a
{doc}`select` `query`. You need to run the {doc}`refresh-materialized-view`
statement after the creation to populate the materialized view with data. This
materialized view is a physical manifestation of the query results at time of
refresh. The data is stored, and can be referenced by future queries.

Queries accessing materialized views are typically faster than retrieving data
from a view created with the same query. Any computation, aggregation, and other
operation to create the data is performed once during refresh of the
materialized views, as compared to each time of accessing the view. Multiple
reads of view data over time, or by multiple users, all trigger repeated
processing. This is avoided for materialized views.

The optional `OR REPLACE` clause causes the materialized view to be replaced
if it already exists rather than raising an error.

The optional `IF NOT EXISTS` clause causes the materialized view only to be
created if it does not exist yet.

Note that `OR REPLACE` and `IF NOT EXISTS` are mutually exclusive clauses.

(mv-grace-period)=
The optional `GRACE PERIOD` clause specifies how long the query materialization
is used for querying:

* Within the grace period since last refresh, data retrieval is highly
  performant because the query materialization is used. However, the data may
  not be up to date with the base tables.
* After the grace period has elapsed, the data of the materialized view is
  computed on-the-fly using the `query`. Retrieval is therefore slower, but the
  data is up to date with the base tables.
* If not specified, the grace period defaults to infinity, and therefore all
  queries are within the grace period.
* Every [](refresh-materialized-view) operation resets the start time for the
  grace period.

(mv-when-stale)=
The optional `WHEN STALE` clause specifies the behavior when the materialized
view is stale (outside the grace period):

* `INLINE` - When the materialized view is stale, it is expanded like a logical
  view, and queries accessing the materialized view will use the underlying
  query definition to retrieve up-to-date data. This is the default behavior
  when `WHEN STALE` is not specified.
* `FAIL` - When the materialized view is stale, queries accessing it fail with
  an error. 

Note that the `WHEN STALE` clause requires connector support. If a connector 
does not support this feature, using `WHEN STALE` with any value other than 
`INLINE` results in an error.

The optional `COMMENT` clause causes a `string` comment to be stored with
the metadata about the materialized view. The comment is displayed with the
{doc}`show-create-materialized-view` statement and is available in the table
`system.metadata.materialized_view_properties`.

The optional `WITH` clause is used to define properties for the materialized
view creation. Separate multiple property/value pairs by commas. The connector
uses the properties as input parameters for the materialized view refresh
operation. The supported properties are different for each connector and
detailed in the SQL support section of the specific connector's documentation.

After successful creation, all metadata about the materialized view is available
in a {ref}`system table <system-metadata-materialized-views>`.

## Examples

Create a simple materialized view `cancelled_orders` over the `orders` table
that only includes cancelled orders. Note that `orderstatus` is a numeric
value that is potentially meaningless to a consumer, yet the name of the view
clarifies the content:

```
CREATE MATERIALIZED VIEW cancelled_orders
AS
    SELECT orderkey, totalprice
    FROM orders
    WHERE orderstatus = 3;
```

Create or replace a materialized view `order_totals_by_date` that summarizes
`orders` across all orders from all customers:

```
CREATE OR REPLACE MATERIALIZED VIEW order_totals_by_date
AS
    SELECT orderdate, sum(totalprice) AS price
    FROM orders
    GROUP BY orderdate;
```

Create a materialized view for a catalog using the Iceberg connector, with a
comment and partitioning on two fields in the storage:

```
CREATE MATERIALIZED VIEW orders_nation_mkgsegment
COMMENT 'Orders with nation and market segment data'
WITH ( partitioning = ARRAY['mktsegment', 'nationkey'] )
AS
    SELECT o.*, c.nationkey, c.mktsegment
    FROM orders AS o
    JOIN customer AS c
    ON o.custkey = c.custkey;
```

Set multiple properties:

```
WITH ( format = 'ORC', partitioning = ARRAY['_date'] )
```

Create a materialized view with a grace period and WHEN STALE behavior that
fails when the view is stale:

```
CREATE MATERIALIZED VIEW orders_summary
GRACE PERIOD INTERVAL '1' HOUR
WHEN STALE FAIL
AS
    SELECT orderdate, sum(totalprice) AS price
    FROM orders
    GROUP BY orderdate;
```

Create a materialized view with explicit INLINE behavior (default):

```
CREATE MATERIALIZED VIEW orders_summary
GRACE PERIOD INTERVAL '1' HOUR
WHEN STALE INLINE
AS
    SELECT orderdate, sum(totalprice) AS price
    FROM orders
    GROUP BY orderdate;
```

Show defined materialized view properties for all catalogs:

```
SELECT * FROM system.metadata.materialized_view_properties;
```

Show metadata about the materialized views in all catalogs:

```
SELECT * FROM system.metadata.materialized_views;
```

## See also

- {doc}`drop-materialized-view`
- {doc}`show-create-materialized-view`
- {doc}`refresh-materialized-view`
