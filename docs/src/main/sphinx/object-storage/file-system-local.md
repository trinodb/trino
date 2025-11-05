# Local file system support

Trino includes support to access a local file system with a catalog using the
Delta Lake, Hive, Hudi, or Iceberg connectors. The local file system must be a
local mount point for a shared file system available on all cluster nodes.

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
* - `fs.native-local.enabled`
  - Activate the support for local file system access. Defaults to `false`. Set
    to `true` to use local file system and enable all other properties.
* - `local.location`
  - Local path on all nodes to the root of the shared file system using the prefix
    `local://` with the path to the mount point.

:::

The following example displays the related section from a
`etc/catalog/example.properties` catalog configuration using the Hive connector.
The coordinator and all workers nodes have an external storage mounted at
`/storage/datalake`, resulting in the location `local:///storage/datalake`.

```properties
connector.name=hive
...
fs.native-local.enabled=true
local.location=local:///storage/datalake
```

Creating a schema named `default` results in the path
`/storage/datalake/default`. Tables within that schema result in separated
directories such as `/storage/datalake/default/table1`.
