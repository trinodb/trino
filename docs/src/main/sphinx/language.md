# SQL language

Trino is an ANSI SQL compliant query engine. ANSI compliance means that all SQL
language implemented in Trino adheres to the ANSI SQL specification, but not
that Trino fully implements every part of the specification. Other SQL engines
often deviate from the ANSI standard with custom features, keywords, and
functions that lead to limitations to interoperability. Trino's compliance to
the ANSI specification allows Trino users to more easily integrate their
favorite data tools, including BI and ETL tools with any underlying data source.

Trino validates and translates the received SQL statements into the necessary
operations on the connected data source.

This section provides a reference to the supported SQL data types and other
general characteristics of the SQL support of Trino.

Refer to the following sections for further details:

* [SQL statement and syntax reference](/sql)
* [SQL functions and operators](/functions)


```{toctree}
:maxdepth: 2

language/sql-support
language/types
```

```{toctree}
:maxdepth: 1

language/reserved
language/comments
```
