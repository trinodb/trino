# Local file system support

Trino includes support to access a local file system with a catalog using the
Delta Lake, Hive, Hudi, or Iceberg connectors. The local file system must be a
local mount point for a shared file system, such as NFS, available at the
same path on the coordinator and all worker nodes.

Support for local file system is not enabled by default, but can be activated by
setting the `fs.local.enabled` property to `true` in your catalog configuration
file.

## General configuration

Use the following properties to configure general aspects of local file system
support:

:::{list-table}
:widths: 40, 60
:header-rows: 1

* - Property
  - Description
* - `fs.local.enabled`
  - Activate the support for local file system access. Defaults to `false`. Set
    to `true` to use local file system and enable all other properties.
* - `local.location`
  - A plain file system path, not a URI, to the root of the shared mount point
    on every node, for example `/storage/datalake`.
* - `local.legacy-prefix`
  - Optional plain file system path. If set, a `file://` location whose path
    starts with this prefix is rebased under `local.location` instead of being
    used literally. Use this when migrating tables whose stored locations
    still reference a mount point that has since moved, for example after
    remounting an NFS share at a new path.

:::

## Location schemes

Two location schemes are available, and they address storage differently:

* `file://` locations are the literal absolute path on the shared mount,
  matching how every other tool (Hadoop's `file:` scheme, `hive.metastore.uri`
  external locations, pre-existing table metadata) already writes local paths.
  For example, with `local.location=/storage/datalake`, the location
  `file:///storage/datalake/default/table1` addresses the real path
  `/storage/datalake/default/table1`.
* `local://` locations are a virtual namespace, always resolved relative to
  `local.location`. For example, with the same configuration,
  `local:///default/table1` addresses the same real path.

`local.location` is enforced as an access boundary for both schemes: any
location that resolves outside of it, through either scheme, is rejected.

Use `file://` when specifying locations explicitly, for example in
`CREATE SCHEMA ... WITH (location = ...)` or `external_location`, since it is
the same real path regardless of how `local.location` is configured. Trino
uses `local://` internally to generate default schema and table locations,
which keeps real absolute paths out of persisted metadata.

The following example displays the related section from a
`etc/catalog/example.properties` catalog configuration using the Hive connector.
The coordinator and all worker nodes have an external storage mounted at
`/storage/datalake`.

```properties
connector.name=hive
...
fs.local.enabled=true
local.location=/storage/datalake
```

Creating a schema named `default` results in the path
`/storage/datalake/default`. Tables within that schema result in separated
directories such as `/storage/datalake/default/table1`.

## Limitations

* A remote, standalone Hive metastore service (Thrift) is not supported as
  the metastore for a local file system catalog, since the metastore process
  validates and manages locations using its own file system access and is
  unaware of Trino's `local`/`file` schemes. Use the embedded file metastore
  instead, activated with `hive.metastore=file`, which runs in the
  coordinator process and shares the same local file system view as Trino, or
  a REST or JDBC-based catalog for Iceberg.
* `local://` and `file://` locations must resolve to a path other than the
  root itself; a location must include a path segment beyond `local.location`.
* Atomic, conflict-detecting writes -- used for example by the Delta Lake
  transaction log and by table creation in the file metastore -- rely on the
  underlying mount correctly implementing exclusive file creation and hard
  links. This holds for local disks and NFS, but administrators using NFSv3
  mounts must ensure their client and server correctly implement the
  exclusive-create verifier (RFC 1813 §3.3.8); modern Linux NFS clients handle
  this transparently.
