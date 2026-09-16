# General properties

## `join-distribution-type`

- **Type:** {ref}`prop-type-string`
- **Allowed values:** `AUTOMATIC`, `PARTITIONED`, `BROADCAST`
- **Default value:** `AUTOMATIC`
- **Session property:** `join_distribution_type`

The type of distributed join to use.  When set to `PARTITIONED`, Trino
uses hash distributed joins.  When set to `BROADCAST`, it broadcasts the
right table to all nodes in the cluster that have data from the left table.
Partitioned joins require redistributing both tables using a hash of the join key.
This can be slower, sometimes substantially, than broadcast joins, but allows much
larger joins. In particular broadcast joins are faster, if the right table is
much smaller than the left.  However, broadcast joins require that the tables on the right
side of the join after filtering fit in memory on each node, whereas distributed joins
only need to fit in distributed memory across all nodes. When set to `AUTOMATIC`,
Trino makes a cost based decision as to which distribution type is optimal.
It considers switching the left and right inputs to the join.  In `AUTOMATIC`
mode, Trino defaults to hash distributed joins if no cost could be computed, such as if
the tables do not have statistics.

## `legacy-type-resolver`

- **Type:** {ref}`prop-type-boolean`
- **Default value:** `false`
- **Session property:** `legacy_type_resolver`

Use the legacy function type resolver instead of the constraint solver when
resolving functions, methods, operators, and casts. Set this property to `true`
to restore legacy resolution for all sessions by default. Individual sessions
can override the configured default with `SET SESSION legacy_type_resolver = true`
or `false`.

The constraint solver limits work and nesting during a function resolution,
including candidate attempts and repeated inference for lambdas. Exceeding a
limit fails with `TYPE_RESOLUTION_LIMIT_EXCEEDED`; it does not skip the candidate
or automatically retry with the legacy resolver.

## `redistribute-writes`

- **Type:** {ref}`prop-type-boolean`
- **Default value:** `true`
- **Session property:** `redistribute_writes`

This property enables redistribution of data before writing. This can
eliminate the performance impact of data skew when writing by hashing it
across nodes in the cluster. It can be disabled, when it is known that the
output data set is not skewed, in order to avoid the overhead of hashing and
redistributing all the data across the network.

(file-compression)=
## File compression and decompression

Trino uses the [aircompressor](https://github.com/airlift/aircompressor) library
to compress and decompress ORC, Parquet, and other files using the LZ4, zstd,
Snappy, and other algorithms. The library takes advantage of using embedded,
higher performing, native implementations for these algorithms by default. 

If necessary, this behavior can be deactivated to fall back on JVM-based
implementations with the following configuration in the [](jvm-config):

```properties
-Dio.airlift.compress.v3.disable-native=true
```

The library relies on the [temporary directory used by the JVM](tmp-directory),
including the execution of code in the directory, to load the embedded shared
libraries. If this directory is mounted with `noexec`, and therefore not
suitable, you can configure usage of a separate directory with an absolute path
set with the following configuration in the [](jvm-config):

```properties
-Daircompressor.tmpdir=/mnt/example
```
