# Client protocol

The Trino client protocol is a HTTP-based protocol that allows
[clients](/client) to submit SQL queries and receive results.

The protocol is a sequence of REST API calls to the
[coordinator](trino-concept-coordinator) of the Trino
[cluster](trino-concept-cluster). Following is a high-level overview:

1. Client submits SQL query text to the coordinator of the Trino cluster.
2. The coordinator starts processing the query.
3. The coordinator returns a result set and a URI `nextUri` on the coordinator.
4. The client receives the result set and initiates another request for more
   data from the URI `nextUri`.
5. The coordinator continues processing the query and returns further data with
   a new URI.
6. The client and coordinator continue with steps 4. and 5. until all
   result set data is returned to the client or the client stops requesting
   more data.
7. If the client fails to fetch the result set, the coordinator does not initiate
   further processing, fails the query, and returns a `USER_CANCELED` error.
8. The final response when the query is complete is `FINISHED`.

The client protocol supports two modes. Configure the [spooling
protocol](protocol-spooling) for optimal throughput for your clients.

(protocol-spooling)=
## Spooling protocol

The spooling protocol uses an object storage location to store the data for
retrieval by the client. The coordinator and all workers can write result set
data to the storage in parallel. The coordinator only provides the URLs to all
the individual data segments on the object storage to the cluster. The spooling
protocol also allows compression of the data.

Data on the object storage is automatically removed after download by the
client.

The spooling protocol has the following characteristics, compared to the [direct
protocol](protocol-direct).

* Provides higher throughput for data transfer, specifically for queries that
  return more data.
* Results in faster query processing completion on the cluster, independent of
  the client retrieving all data, since data is read from the object storage.
* Requires object storage and configuration on the Trino cluster.
* Reduces CPU and I/O load on the coordinator.
* Automatically falls back to the direct protocol for queries that don't benefit
  from using the spooling protocol.
* Requires newer client drivers or client applications that support the spooling
  protocol and actively request usage of the spooling protocol.
* Clients must have access to the object storage.
* Works with older client drivers and client applications by automatically
  falling back to the direct protocol if spooling protocol is not supported.

### Configuration

The following steps are necessary to configure support for the spooling protocol
on a Trino cluster:

* Configure the spooling protocol usage in [](config-properties) using the
  [](prop-protocol-spooling).
* Choose a suitable object storage that is accessible to your Trino cluster and
  your clients.
* Create a location in your object storage that is not shared with any object
  storage catalog or spooling for any other Trino clusters.
* Configure the object storage in `etc/spooling-manager.properties` using the
  [](prop-spooling-file-system).

Minimal configuration in [](config-properties):

```properties
protocol.spooling.enabled=true
protocol.spooling.shared-secret-key=jxTKysfCBuMZtFqUf8UJDQ1w9ez8rynEJsJqgJf66u0=
```

Refer to [](prop-protocol-spooling) for further optional configuration.

Suitable object storage systems for spooling are S3 and compatible systems,
Azure Storage, and Google Cloud Storage. The object storage system must provide
good connectivity for all cluster nodes as well as any clients. 

Activate the desired system with
`fs.s3.enabled`, `fs.azure.enabled`, or `fs.s3.enabled=true` in
`etc/spooling-manager.properties`and configure further details using relevant
properties from [](prop-spooling-file-system),
[](/object-storage/file-system-s3), [](/object-storage/file-system-azure), and
[](/object-storage/file-system-gcs).

The `spooling-manager.name` property must be set to `filesystem`.

Following is a minimalistic example for using the S3-compatible MinIO object
storage:

```properties
spooling-manager.name=filesystem
fs.s3.enabled=true
fs.location=s3://spooling
s3.endpoint=http://minio:9080/
s3.region=fake-value
s3.aws-access-key=minio-access-key
s3.aws-secret-key=minio-secret-key
s3.path-style-access=true
```

Refer to [](prop-spooling-file-system) for further configuration properties.

The system assumes the object storage to be unbounded in terms of data and data
transfer volume. Spooled segments on object storage are automatically removed by
the clients after reads as well as the coordinator in specific intervals. Sizing
and transfer demands vary with the query workload on your cluster.

Segments on object storage are encrypted, compressed, and can only be used by
the specific client who initiated the query.

The following client drivers and client applications support the spooling protocol.

* [Trino JDBC driver](jdbc-spooling-protocol), version 466 and newer
* [Trino command line interface](cli-spooling-protocol), version 466 and newer
* [Trino Python client](https://github.com/trinodb/trino-python-client), version
  0.332.0 and newer

Refer to the documentation for other your specific client drivers and client
applications for up to date information.

(protocol-direct)=
## Direct protocol

The direct protocol transfers all data from the workers to the coordinator, and
from there directly to the client.

The direct protocol, also know as the `v1` protocol, has the following
characteristics, compared to the spooling protocol:

* Provides lower performance, specifically for queries that return more data.
* Results in slower query processing completion on the cluster, since data is
  provided by the coordinator and read by the client sequentially.
* Requires **no** object storage or configuration in the Trino cluster.
* Increases CPU and I/O load on the coordinator.
* Works with older client drivers and client applications without support for
  the spooling protocol.

### Configuration

Use of the direct protocol requires not configuration. Find optional
configuration properties in [](prop-protocol-shared).

## Development and reference information

Further technical details about the client protocol, including information
useful for developing a client driver, are available in the [Trino client REST
API developer reference](/develop/client-protocol).
