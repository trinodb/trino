# Use cases

This section puts Trino into perspective, so that prospective
administrators and end users know what to expect from Trino.

## What Trino is not

Since Trino is being called a *database* by many members of the community,
it makes sense to begin with a definition of what Trino is not.

Do not mistake the fact that Trino understands SQL with it providing
the features of a standard database. Trino is not a general-purpose
relational database. It is not a replacement for databases like MySQL,
PostgreSQL or Oracle. Trino was not designed to handle Online
Transaction Processing (OLTP). This is also true for many other
databases designed and optimized for data warehousing or analytics.

## What Trino is

Trino is a tool designed to efficiently query vast amounts of data
using distributed queries. If you work with terabytes or petabytes of
data, you are likely using tools that interact with Hadoop and HDFS.
Trino was designed as an alternative to tools that query HDFS
using pipelines of MapReduce jobs, such as Hive or Pig, but Trino
is not limited to accessing HDFS. Trino can be and has been extended
to operate over different kinds of data sources, including traditional
relational databases and other data sources such as Cassandra.

Trino was designed to handle data warehousing and analytics: data analysis,
aggregating large amounts of data and producing reports. These workloads
are often classified as Online Analytical Processing (OLAP).
