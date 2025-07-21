# MySQL event listener

The MySQL event listener plugin allows streaming of query events to an external
MySQL database. The query history in the database can then be accessed directly
in MySQL or via Trino in a catalog using the [MySQL connector](/connector/mysql).

## Rationale

This event listener is a first step to store the query history of your Trino
cluster. The query events can provide CPU and memory usage metrics, what data is
being accessed with resolution down to specific columns, and metadata about the
query processing.

Running the capture system separate from Trino reduces the performance impact
and avoids downtime for non-client-facing changes.

## Requirements

You need to perform the following steps:

- Create a MySQL database.
- Determine the JDBC connection URL for the database.
- Ensure network access from the Trino coordinator to MySQL is available.
  Port 3306 is the default port.

(mysql-event-listener-configuration)=
## Configuration

To configure the MySQL event listener plugin, create an event listener properties
file in `etc` named `mysql-event-listener.properties` with the following contents
as an example:

```properties
event-listener.name=mysql
mysql-event-listener.db.url=jdbc:mysql://example.net:3306
mysql-event-listener.db.terminate-on-initialization-failure=true
```

The `mysql-event-listener.db.url` defines the connection to a MySQL database
available at the domain `example.net` on port 3306. You can pass further
parameters to the MySQL JDBC driver. The supported parameters for the URL are
documented in the [MySQL Developer
Guide](https://dev.mysql.com/doc/connector-j/en/connector-j-reference-configuration-properties.html).

And set `event-listener.config-files` to `etc/mysql-event-listener.properties`
in {ref}`config-properties`:

```properties
event-listener.config-files=etc/mysql-event-listener.properties
```

If another event listener is already configured, add the new value
`etc/mysql-event-listener.properties` with a separating comma.

After this configuration and successful start of the Trino cluster, the table
`trino_queries` is created in the MySQL database. From then on, any query
processing event is captured by the event listener and a new row is inserted
into the table. The table includes many columns, such as query identifier, query
string, user, catalog, and others with information about the query processing.

To control how Trino behaves when the MySQL event listener fails to initialize
(e.g., due to the database being unavailable), you can use the
`mysql-event-listener.terminate-on-initialization-failure` property.

### Configuration properties

:::{list-table}
:widths: 40, 60
:header-rows: 1

* - Property name
  - Description
* - `mysql-event-listener.db.url`
  - JDBC connection URL to the database including credentials
* - `mysql-event-listener.terminate-on-initialization-failure`
  - A [boolean](prop-type-boolean) flag that determines whether Trino should fail
    to start if the MySQL event listener cannot initialize—typically due to the
    database being unavailable.
    When set to `true`, any initialization failure will raise an exception,
    preventing the Trino Coordinator from starting. When set to `false`, the
    Coordinator will continue to start even if the listener fails to initialize
:::
