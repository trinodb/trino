# Table statistics

Trino supports statistics based optimizations for queries. For a query to take
advantage of these optimizations, Trino must have statistical information for
the tables in that query.

Table statistics are estimates about the stored data. They are provided to the
query planner by connectors and enable performance improvements for query
processing.

## Available statistics

The following statistics are available in Trino:

- For a table:

  - **row count**: the total number of rows in the table

- For each column in a table:

  - **data size**: the size of the data that needs to be read
  - **nulls fraction**: the fraction of null values
  - **distinct value count**: the number of distinct values
  - **low value**: the smallest value in the column
  - **high value**: the largest value in the column

The set of statistics available for a particular query depends on the connector
being used and can also vary by table. For example, the
Hive connector does not currently provide statistics on data size.

Table statistics can be displayed via the Trino SQL interface using the
[](/sql/show-stats) command.

Depending on the connector support, table statistics are updated by Trino when
executing [data management statements](sql-data-management) like `INSERT`,
`UPDATE`, or `DELETE`. For example, the [Delta Lake
connector](delta-lake-table-statistics), the [Hive connector](hive-analyze), and
the [Iceberg connector](iceberg-table-statistics) all support table statistics
management from Trino. 

You can also initialize statistics collection with the [](/sql/analyze) command.
This is needed when other systems manipulate the data without Trino, and
therefore statistics tracked by Trino are out of date. Other connectors rely on
the underlying data source to manage table statistics or do not support table
statistics use at all.
