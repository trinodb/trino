# TPC-H connector

The TPC-H connector provides a set of schemas to support the
[TPC Benchmark™ H (TPC-H)](http://www.tpc.org/tpch/). TPC-H is a database
benchmark used to measure the performance of highly-complex decision support databases.

This connector can be used to test the capabilities and query
syntax of Trino without configuring access to an external data
source. When you query a TPC-H schema, the connector generates the
data on the fly using a deterministic algorithm.

Use the [](/connector/faker) to create and query arbitrary data.

## Configuration

To configure the TPC-H connector, create a catalog properties file
`etc/catalog/example.properties` with the following contents:

```text
connector.name=tpch
```

In the TPC-H specification, each column is assigned a prefix based on its
corresponding table name, such as `l_` for the `lineitem` table. By default, the
TPC-H connector simplifies column names by excluding these prefixes with the
default of `tpch.column-naming` to `SIMPLIFIED`. To use the long, standard
column names, use the configuration in the catalog properties file:

```text
tpch.column-naming=STANDARD
```

## TPC-H schemas

The TPC-H connector supplies several schemas:

```
SHOW SCHEMAS FROM example;
```

```text
       Schema
--------------------
 information_schema
 sf1
 sf100
 sf1000
 sf10000
 sf100000
 sf300
 sf3000
 sf30000
 tiny
(11 rows)
```

Ignore the standard schema `information_schema`, which exists in every
catalog, and is not directly provided by the TPC-H connector.

Every TPC-H schema provides the same set of tables. Some tables are
identical in all schemas. Other tables vary based on the *scale factor*,
which is determined based on the schema name. For example, the schema
`sf1` corresponds to scale factor `1` and the schema `sf300`
corresponds to scale factor `300`. The TPC-H connector provides an
infinite number of schemas for any scale factor, not just the few common
ones listed by `SHOW SCHEMAS`. The `tiny` schema is an alias for scale
factor `0.01`, which is a very small data set useful for testing.

(tpch-type-mapping)=
## Type mapping

Trino supports all data types used within the TPC-H schemas so no mapping
is required.

(tpch-sql-support)=
## SQL support

The connector provides {ref}`globally available <sql-globally-available>` and
{ref}`read operation <sql-read-operations>` statements to access data and
metadata in the TPC-H dataset.
