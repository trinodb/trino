# Spilling properties

These properties control {doc}`spill`.

## `spill-enabled`

- **Type:** {ref}`prop-type-boolean`
- **Default value:** `false`
- **Session property:** `spill_enabled`

Try spilling memory to disk to avoid exceeding memory limits for the query.

Spilling works by offloading memory to disk. This process can allow a query with a large memory
footprint to pass at the cost of slower execution times. Spilling is supported for
aggregations, joins (inner and outer), sorting, and window functions. This property does not
reduce memory usage required for other join types.

## `spiller-spill-path`

- **Type:** {ref}`prop-type-string`
- **No default value.** Must be set when spilling is enabled

Directory where spilled content is written. It can be a comma separated
list to spill simultaneously to multiple directories, which helps to utilize
multiple drives installed in the system.

It is not recommended to spill to system drives. Most importantly, do not spill
to the drive on which the JVM logs are written, as disk overutilization might
cause JVM to pause for lengthy periods, causing queries to fail.

## `spiller-max-used-space-threshold`

- **Type:** {ref}`prop-type-double`
- **Default value:** `0.9`

If disk space usage ratio of a given spill path is above this threshold,
this spill path is not eligible for spilling.

## `spiller-threads`

- **Type:** {ref}`prop-type-integer`
- **Default value:** `4`

Number of spiller threads. Increase this value if the default is not able
to saturate the underlying spilling device (for example, when using RAID).

## `max-spill-per-node`

- **Type:** {ref}`prop-type-data-size`
- **Default value:** `100GB`

Max spill space to use by all queries on a single node. This only needs to be
configured on worker nodes.

## `query-max-spill-per-node`

- **Type:** {ref}`prop-type-data-size`
- **Default value:** `100GB`

Max spill space to use by a single query on a single node. This only needs to be
configured on worker nodes.

## `aggregation-operator-unspill-memory-limit`

- **Type:** {ref}`prop-type-data-size`
- **Default value:** `4MB`

Limit for memory used for unspilling a single aggregation operator instance.

(prop-spill-compression-codec)=
## `spill-compression-codec`

- **Type:** {ref}`prop-type-string`
- **Allowed values:** `NONE`, `LZ4`, `ZSTD`
- **Default value:** `NONE`

The compression codec to use when spilling pages to disk.

## `spill-encryption-enabled`

- **Type:** {ref}`prop-type-boolean`
- **Default value:** `false`

Enables using a randomly generated secret key (per spill file) to encrypt and decrypt
data spilled to disk.
