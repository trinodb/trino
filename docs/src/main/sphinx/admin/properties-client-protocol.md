# Client protocol properties

bla bla intro with links to clients

(prop-protocol-v1)=
## v1 protocol

blabla intro

### `protocol.v1.alternate-header-name`

**Type:** [](prop-type-string)

The 351 release of Trino changes the HTTP client protocol headers to start with
`X-Trino-`. Clients for versions 350 and lower expect the HTTP headers to
start with `X-Presto-`, while newer clients expect `X-Trino-`. You can support these
older clients by setting this property to `Presto`.

The preferred approach to migrating from versions earlier than 351 is to update
all clients together with the release, or immediately afterwards, and then
remove usage of this property.

Ensure to use this only as a temporary measure to assist in your migration
efforts.

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

(prop-protocol-spooling)=
## Spooling protocol

intro and more - do we want a separate page in the admin section like we do for
FTE instead or in addition? maybe a separate page since the file system details
need to be configured

### `protocol.spooling.worker-access`

- **Type:** [](prop-type-boolean)
- **Default value:** `false`

Use worker nodes to retrieve data from spooling location


### `protocol.spooling.direct-storage-access`

- **Type:** [](prop-type-boolean)
- **Default value:** `true`

Retrieve segments directly from the spooling location

### `protocol.spooling.direct-storage-fallback`

- **Type:** [](prop-type-boolean)
- **Default value:** `false`

Fallback segment retrieval through the coordinator when direct storage access is
not possible.

### `protocol.spooling.initial-segment-size`

- **Type:** [](prop-type-data-size)
- **Default value:** 8MB

Initial size of the spooled segments in bytes

### `protocol.spooling.maximum-segment-size`

- **Type:** [](prop-type-data-size)
- **Default value:** 16MB

tbd

### `protocol.spooling.inline-segments`

- **Type:** [](prop-type-boolean)
- **Default value:** `false`

Allow protocol to inline data

### `protocol.spooling.shared-secret-key`

- **Type:** [](prop-type-string)

256 bit, base64-encoded secret key used to secure segment identifiers.


(prop-spooling-filesystem)=
## Spooling filesystem

mabye this should go onto another page .. but I actually think not .. it fits
here unless there is any other usage for the spooling file system besides the
spooling protocol


### `fs.azure.enabled`

- **Type:** [](prop-type-boolean)

tbd, link to azure object storage for more details, exclusive to other


### `fs.s3.enabled`

- **Type:** [](prop-type-boolean)


### `fs.gcs.enabled`

- **Type:** [](prop-type-boolean)


### `fs.location`

- **Type:** 

### `fs.layout`

- **Type:** 

layout class, some sort of `SIMPLE` or `PARTITIONED`

Spooling segments file system layout


### `fs.layout.partitions`

integer

default 32

min 2, max 1024


only applicable for fs.layout=PARTITIONED


### `fs.segment.ttl`

- **Type:** 

Maximum duration for the client to retrieve spooled segment before it expires


### `fs.segment.encryption`

- **Type:** 

Encrypt segments with ephemeral keys


### `fs.segment.pruning.enabled`

- **Type:** 

Prune expired segments periodically


### `fs.segment.pruning.interval`

- **Type:** 

Interval to prune expired segments


### `fs.segment.pruning.batch-size`

- **Type:** 

Prune expired segments in batches of provided size