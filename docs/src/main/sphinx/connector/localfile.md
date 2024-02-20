# Local file connector

The local file connector allows querying the HTTP request log files stored on
the local file system of each worker.

## Configuration

To configure the local file connector, create a catalog properties file under
`etc/catalog` named, for example, `example.properties` with the following
contents:

```text
connector.name=localfile
```

## Configuration properties

| Property name                          | Description                                                                                |
| -------------------------------------- | ------------------------------------------------------------------------------------------ |
| `trino-logs.http-request-log.location` | Directory or file where HTTP request logs are written                                      |
| `trino-logs.http-request-log.pattern`  | If the log location is a directory, this glob is used to match file names in the directory |

## Local file connector schemas and tables

The local file connector provides a single schema named `logs`.
You can see all the available tables by running `SHOW TABLES`:

```
SHOW TABLES FROM example.logs;
```

### `http_request_log`

This table contains the HTTP request logs from each node on the cluster.
