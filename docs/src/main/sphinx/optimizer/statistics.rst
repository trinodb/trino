================
Table statistics
================

Trino supports statistics based optimizations for queries. For a query to take
advantage of these optimizations, Trino must have statistical information for
the tables in that query.

Table statistics are provided to the query planner by connectors.

Available statistics
--------------------

The following statistics are available in Trino:

* For a table:

  * **row count**: the total number of rows in the table

* For each column in a table:

  * **data size**: the size of the data that needs to be read
  * **nulls fraction**: the fraction of null values
  * **distinct value count**: the number of distinct values
  * **low value**: the smallest value in the column
  * **high value**: the largest value in the column

The set of statistics available for a particular query depends on the connector
being used and can also vary by table. For example, the
Hive connector does not currently provide statistics on data size.

Table statistics can be displayed via the Trino SQL interface using the
:doc:`/sql/show-stats` command. For the Hive connector, refer to the
:ref:`Hive connector <hive-analyze>` documentation to learn how to update table
statistics.
