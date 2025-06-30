# ScyllaDB connector

```{raw} html
<img src="../_static/img/cassandra.png" class="connector-logo">
```

The ScyllaDB connector allows querying data stored in
[ScyllaDB](https://www.scylladb.com/).

## Requirements

To connect to ScyllaDB, you need:

- ScyllaDB version 6.2 or higher.
- Network access from the Trino coordinator and workers to ScyllaDB.
  Port 9042 is the default port.

## Configuration

To configure the ScyllaDB connector, create a catalog properties file
`etc/catalog/example.properties` with the following contents,
replacing `host1,host2` with a comma-separated list of the ScyllaDB
nodes, used to discover the cluster topology:

```text
connector.name=scylladb
cassandra.contact-points=host1,host2
```

You also need to set `cassandra.native-protocol-port`, if your
ScyllaDB nodes are not using the default port 9042.

## Compatibility with Cassandra connector

The ScyllaDB connector is very similar to the Cassandra connector with the
only difference being the underlying driver.
See [Cassandra connector](cassandra) for more details.
