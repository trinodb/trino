---
layout: docu
title: Introduction
---

This page contains the specification for the DuckLake format, version 0.3.

## Building Blocks

DuckLake requires two main components:

* **Catalog database:** DuckLake requires a database that supports transactions and primary key constraints as defined by the [SQL-92 standard](https://en.wikipedia.org/wiki/SQL-92).
* **Data storage:** The DuckLake specification requires a storage component for storing the data in [Parquet format](https://parquet.apache.org/docs/file-format/).

### Catalog Database

DuckLake uses SQL tables and queries to define the catalog information (metadata, statistics, etc.).
This specification explains the schema and semantics of these:

* [Data Types]({% link docs/preview/specification/data_types.md %})
* [Queries]({% link docs/preview/specification/queries.md %})
* [Tables]({% link docs/preview/specification/tables/overview.md %})

If you are reading this specification for the first time,
we recommend starting with the [“Queries” page]({% link docs/preview/specification/queries.md %}),
which introduces the queries used by DuckLake.

### Data Storage

DuckLake uses [Parquet](https://parquet.apache.org/docs/file-format/) files to represent its tables.
These files can be stored in [object storage (blob storage)](https://en.wikipedia.org/wiki/Object_storage), block storage or file storage.
