# DROP CATALOG

## Synopsis

```text
DROP CATALOG catalog_name
```

## Description

Drops an existing catalog. Dropping a catalog does not interrupt any running
queries that use it, but makes it unavailable to any new queries.

:::{warning}
Some connectors are known not to release all resources when dropping a catalog
that uses such connector. This includes all connectors that can read data from
HDFS, S3, GCS, or Azure, which are [](/connector/hive),
[](/connector/iceberg), [](/connector/delta-lake), and
[](/connector/hudi).
:::

:::{note}
This command requires the [catalog management type](/admin/properties-catalog)
to be set to `dynamic`.
:::

## Examples

Drop the catalog `example`:

```
DROP CATALOG example;
```

## See also

* [](/sql/create-catalog)
* [](/admin/properties-catalog)
