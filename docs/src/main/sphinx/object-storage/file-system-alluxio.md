# Alluxio file system support

Trino includes a native implementation to access
[Alluxio](https://docs.alluxio.io/os/user/stable/en/Overview.html) as a file
system with a catalog using the Delta Lake, Hive, Hudi, or Iceberg connectors.
An Alluxio cluster acts as caching layer for one or more Trino catalogs and even
clusters or other systems, in front of the actual object storage.

For comparison the [](/object-storage/file-system-cache) caches data locally on
the Trino cluster nodes and is separate for each catalog and cluster.

Enable the Alluxio file system with `fs.alluxio.enabled=true` in your catalog
properties file.

## Configuration

Use the following properties to configure general aspects of Alluxio file system
support in your catalog properties file:

:::{list-table}
:widths: 40, 60
:header-rows: 1

* - Property
  - Description
* - `fs.alluxio.enabled`
  - Activate the Alluxio file system support. Defaults to `false`.
:::

## Alluxio client configuration

The Alluxio cluster connection is configured in the `alluxio-site.properties`
properties file. The same config file must be located in `/opt/alluxio/conf` on
all Trino cluster nodes. Follow the [Alluxio client configuration
documentation](https://docs.alluxio.io/os/user/stable/en/operation/Configuration.html)
for more details.

Example content of `alluxio-site.properties`:

```properties
alluxio.master.hostname=127.0.0.1
alluxio.master.port=19998
alluxio.security.authentication.type=NOSASL
```
