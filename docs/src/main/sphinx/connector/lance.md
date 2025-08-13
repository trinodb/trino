# Lance connector

## General configuration

To configure the Lance connector, create a catalog properties file `etc/catalog/example.properties` with the following content

```text
connector.name=lance
```

You must configure a [namespace](https://lancedb.github.io/lance/format/namespace/) type.
Currently only [directory namespace](https://lancedb.github.io/lance/format/namespace/impls/dir/) is supported.

```text
lance.namespace.type=directory
```

## Lance Namespace configuration
### Directory namespace
Lance directory namespace is a lightweight and simple single-level Lance namespace that contains only a list of tables. All tables reside in the default namespace.

The following configuration properties are available:

:::{list-table}
:widths: 30, 10, 60
:header-rows: 1

* - Property name
  - Required
  - Description
* - `lance.namespace.directory.warehouse.location`
  - Yes
  - The root directory URI of the namespace where tables are stored.
:::


## File system access configuration

The connector supports accessing the following file systems:

* [](/object-storage/file-system-azure)
* [](/object-storage/file-system-gcs)
* [](/object-storage/file-system-s3)
* [](/object-storage/file-system-hdfs)

You must enable and configure the specific file system access. 
