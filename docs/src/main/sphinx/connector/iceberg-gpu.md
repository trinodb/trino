# Iceberg GPU connector

The Iceberg GPU connector is the [Iceberg connector](/connector/iceberg) with
filtered aggregations answered on an NVIDIA GPU, through
[TornadoVM](https://github.com/beehive-lab/TornadoVM) and RAPIDS libcudf. It is
configured as the Iceberg connector is, reads and writes the same tables, and
gives the same results. Queries it cannot run on the GPU, writes, metadata
tables and tables with delete files are the Iceberg connector's.

A query runs on the GPU when it is a `sum`, `avg` or `count` aggregation,
grouped by at most four columns, over an Iceberg table whose current snapshot
has only local Parquet data files and no delete files, where:

- predicates are single ranges over numeric or date columns without nulls,
- aggregated values are `DOUBLE` columns, or products of terms `a * column + b`
  of them, such as `extendedprice * (1 - discount) * (1 + tax)`,
- group keys are `INTEGER`, `BIGINT`, `DATE` columns, or `VARCHAR` columns whose
  values are at most 8 bytes long.

The columns a table's queries use stay on the GPU between queries.

## Requirements

- An NVIDIA GPU with CUDA 13.
- The TornadoVM 8.1 SDK for JDK 22 or later, with CUDA, for example from SDKMAN:
  `sdk install tornadovm 8.1.0-jdk22plus-cuda`.
- RAPIDS libcudf 26.08 for CUDA 13, for example the `libcudf-cu13` wheel from
  `https://pypi.nvidia.com`, unpacked, with its library directories on
  `LD_LIBRARY_PATH` of the Trino server.

## General configuration

Add TornadoVM's argument file and the GPU memory TornadoVM may use to
`etc/jvm.config`:

```text
@/path/to/tornadovm-8.1.0-jdk22plus-cuda/tornado-argfile
-Dtornado.device.memory=16GB
```

Share TornadoVM with plugins in `etc/config.properties`:

```properties
plugin.shared-packages=uk.ac.manchester.tornado.
```

Then create a catalog properties file, for example `etc/catalog/example.properties`,
with the properties of an [Iceberg catalog](/connector/iceberg) and the
connector name `iceberg_gpu`:

```properties
connector.name=iceberg_gpu
iceberg.catalog.type=hive_metastore
hive.metastore.uri=thrift://example.net:9083
```

Without TornadoVM, the catalog works as an Iceberg catalog and runs nothing on
the GPU.

## Checking where a query runs

`EXPLAIN` shows an aggregation that runs on the GPU as a table scan of a
`GpuTableHandle` with its `aggregation` set.
