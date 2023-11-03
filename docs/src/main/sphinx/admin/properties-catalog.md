# Catalog management properties

The following properties are used to configure catalog management with further
controls for dynamic catalog management.

(prop-catalog-management)=
## `catalog.management`

- **Type:** [](prop-type-string)
- **Allowed values:** `static`, `dynamic`
- **Default value:** `static`

When set to `static`, Trino reads catalog property files and configures
available catalogs only on server startup. When set to `dynamic`, catalog
configuration can also be managed using [](/sql/create-catalog) and
[](/sql/drop-catalog). New worker nodes joining the cluster receive the current
catalog configuration from the coordinator node.

:::{warning}
This feature is experimental only. Because of the security implications the
syntax might change and be backward incompatible.
:::

:::{warning}
Some connectors are known not to release all resources when dropping a catalog
that uses such connector. This includes all connectors that can read data from
HDFS, S3, GCS, or Azure, which are [](/connector/hive),
[](/connector/iceberg), [](/connector/delta-lake), and
[](/connector/hudi).
:::

:::{warning}
The complete `CREATE CATALOG` query is logged, and visible in the [Web
UI](/admin/web-interface). This includes any sensitive properties, like
passwords and other credentials. See [](/security/secrets).
:::

## `catalog.prune.update-interval`

- **Type:** [](prop-type-duration)
- **Default value:** `5s`
- **Minimum value:** `1s`

Requires [](prop-catalog-management) to be set to `dynamic`. Interval for
pruning dropped catalogs. Dropping a catalog does not interrupt any running
queries that use it, but makes it unavailable to any new queries.

(prop-catalog-store)=
## `catalog.store`

- **Type:** [](prop-type-string)
- **Allowed values:** `file`, `memory`
- **Default value:** `file`

Requires [](prop-catalog-management) to be set to `dynamic`. When set to
`file`, creating and dropping catalogs using the SQL commands adds and removes
catalog property files on the coordinator node. Trino server process requires
write access in the catalog configuration directory. Existing catalog files are
also read on the coordinator startup. When set to `memory`, catalog
configuration is only managed in memory, and any existing files are ignored on
startup.

## `catalog.config-dir`

- **Type:** [](prop-type-string)
- **Default value:** `etc/catalog/`

Requires [](prop-catalog-management) to be set to `static` or
[](prop-catalog-store) to be set to `file`. The directory with catalog property
files.

## `catalog.disabled-catalogs`

- **Type:** [](prop-type-string)

Requires [](prop-catalog-management) to be set to `static` or
[](prop-catalog-store) to be set to `file`. Comma-separated list of catalogs to
ignore on startup.

## `catalog.read-only`

- **Type:** [](prop-type-string)
- **Default value:** `false`

Requires [](prop-catalog-store) to be set to `file`. If true, existing catalog
property files cannot be removed with `DROP CATALOG`, and no new catalog files
can be written with identical names with `CREATE CATALOG`. As a result, a
coordinator restart resets the known catalogs to the existing files only.
