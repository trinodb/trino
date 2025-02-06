# Client protocol properties

The following sections provide a reference for all properties related to the
[client protocol](/client/client-protocol). 

(prop-protocol-spooling)=
## Spooling protocol properties

The following properties are related to the [](protocol-spooling).

### `protocol.spooling.enabled`

- **Type:** [](prop-type-boolean)
- **Default value:** `true`
- **Session property:** `spooling_enabled`

Enable the support for the client [](protocol-spooling). The protocol is used if
client drivers and applications request usage, otherwise the direct protocol is
used automatically.

### `protocol.spooling.shared-secret-key`

- **Type:** [](prop-type-string)

A required 256 bit, base64-encoded secret key used to secure spooled metadata
exchanged with the client. Create a suitable value with the following command:

```shell
openssl rand -base64 32
```

### `protocol.spooling.retrieval-mode`

- **Type:** [](prop-type-string)
- **Default value:** `STORAGE`

Determines how the client retrieves the segment. Following are possible values:

* `STORAGE` - client accesses the storage directly with the pre-signed URI. Uses
  one client HTTP request per data segment. 
* `COORDINATOR_STORAGE_REDIRECT` - client first accesses the coordinator, which
  redirects the client to the storage with the pre-signed URI. Uses two client
  HTTP requests per data segment.
* `COORDINATOR_PROXY` - client accesses the coordinator and gets data segment
  through it. Uses one client HTTP request per data segment, but requires a
  coordinator HTTP request to the storage.
* `WORKER_PROXY` - client accesses the coordinator, which redirects to an
  available worker node. It fetches the data from the storage and provides it
  to the client. Uses two client HTTP requests, and requires a worker request to
  the storage.

### `protocol.spooling.encoding.json.enabled`

- **Type:** [](prop-type-boolean)
- **Default value:** `true`

Activate support for using uncompressed JSON encoding for spooled segments.

### `protocol.spooling.encoding.json+zstd.enabled`

- **Type:** [](prop-type-boolean)
- **Default value:** `true`

Activate support for using JSON encoding with Zstandard compression for spooled
segments.

### `protocol.spooling.encoding.json+lz4.enabled`

- **Type:** [](prop-type-boolean)
- **Default value:** `true`

Activate support for using JSON encoding with LZ4 compression for spooled
segments.

### `protocol.spooling.encoding.compression.threshold`

- **Type:** [](prop-type-data-size)
- **Default value:** `8KB`
- **Minimum value:** `1KB`
- **Maximum value:** `4MB`

Threshold for enabling compression with larger segments.

### `protocol.spooling.initial-segment-size`

- **Type:** [](prop-type-data-size)
- **Default value:** `8MB`
- **Minimum value:** `1KB`
- **Maximum value:** `128MB`
- **Session property:** `spooling_initial_segment_size`

Initial size of the spooled segments.

### `protocol.spooling.max-segment-size`

- **Type:** [](prop-type-data-size)
- **Default value:** `16MB`
- **Minimum value:** `1KB`
- **Maximum value:** `128MB`
- **Session property:** `spooling_max_segment_size`

Maximum size for each spooled segment.

### `protocol.spooling.inlining.enabled`

- **Type:** [](prop-type-boolean)
- **Default value:** `true`
- **Session property:** `spooling_inlining_enabled`

Allow spooled protocol to inline initial rows to decrease time to return the
first row.

### `protocol.spooling.inlining.max-rows`

- **Type:** [](prop-type-integer)
- **Default value:** `1000`
- **Minimum value:** `1`
- **Maximum value:** `1000000`
- **Session property:** `spooling_inlining_max_rows`

Maximum number of rows to inline per worker.

### `protocol.spooling.inlining.max-size`

- **Type:** [](prop-type-data-size)
- **Default value:** `128kB`
- **Minimum value:** `1KB`
- **Maximum value:** `1MB`
- **Session property:** `spooling_inlining_max_size`

Maximum size of rows to inline per worker.

(prop-spooling-file-system)=
## Spooling file system properties

The following properties are used to configure the object storage used with the
[](protocol-spooling).

### `fs.azure.enabled`

- **Type:** [](prop-type-boolean)
- **Default value:** `false`

Activate [](/object-storage/file-system-azure) for spooling segments. 

### `fs.s3.enabled`

- **Type:** [](prop-type-boolean)
- **Default value:** `false`

Activate [](/object-storage/file-system-s3) for spooling segments. 

### `fs.gcs.enabled`

- **Type:** [](prop-type-boolean)
- **Default value:** `false`

Activate [](/object-storage/file-system-gcs) for spooling segments. 

### `fs.location`

- **Type:** [](prop-type-string)

The object storage location to use for spooling segments. Must be accessible by
the coordinator and all workers. With the `protocol.spooling.retrieval-mode`
retrieval modes `STORAGE` and `COORDINATOR_STORAGE_REDIRECT` the location must
also be accessible by all clients. Valid location values vary by object storage
type, and typically follow a pattern of `scheme://bucketName/path/`.

Examples:

* `s3://my-spooling-bucket/my-segments/`

:::{caution}
The specified object storage location must not be used for spooling for another
Trino cluster or any object storage catalog. When using the same object storage
for multiple services, you must use separate locations for each one. For
example:

* `s3://my-spooling-bucket/my-segments/cluster1-spooling`
* `s3://my-spooling-bucket/my-segments/cluster2-spooling`
* `s3://my-spooling-bucket/my-segments/iceberg-catalog`
:::

### `fs.segment.ttl`

- **Type:** [](prop-type-duration)
- **Default value:** `12h`

Maximum available time for the client to retrieve spooled segment before it
expires and is pruned.

### `fs.segment.direct.ttl`

- **Type:** [](prop-type-duration)
- **Default value:** `1h`

Maximum available time for the client to retrieve spooled segment using the
pre-signed URI.

### `fs.segment.encryption`

- **Type:** [](prop-type-boolean)
- **Default value:** `true`

Encrypt segments with ephemeral keys using Server-Side Encryption with Customer
key (SSE-C).

### `fs.segment.explicit-ack`

- **Type:** [](prop-type-boolean)
- **Default value:** `true`

Activate pruning of segments on client acknowledgment of a successful read of
each segment.

### `fs.segment.pruning.enabled`

- **Type:** [](prop-type-boolean)
- **Default value:** `true`

Activate periodic pruning of expired segments.

### `fs.segment.pruning.interval`

- **Type:** [](prop-type-duration)
- **Default value:** `5m`

Interval to prune expired segments.

### `fs.segment.pruning.batch-size`

- **Type:** integer
- **Default value:** `250`

Number of expired segments to prune as a single batch operation.

(prop-protocol-shared)=
## Shared protocol properties

The following properties are related to the [](protocol-spooling) and the
[](protocol-direct), formerly named the V1 protocol.

### `protocol.v1.prepared-statement-compression.length-threshold`

- **Type:** [](prop-type-integer)
- **Default value:** `2048`

Prepared statements that are submitted to Trino for processing, and are longer
than the value of this property, are compressed for transport via the HTTP
header to improve handling, and to avoid failures due to hitting HTTP header
size limits.

### `protocol.v1.prepared-statement-compression.min-gain`

- **Type:** [](prop-type-integer)
- **Default value:** `512`

Prepared statement compression is not applied if the size gain is less than the
configured value. Smaller statements do not benefit from compression, and are
left uncompressed.

