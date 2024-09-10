# Trino Loki Connector

## Testing

The test query runner in `src/test/java/io.trino.loki` starts a Docker compose with Loki and Grafana. It can be used with `trino-cli  http://127.0.0.1:8080/loki/default` or through
Grafana under http://localhost:3000.

The are some example queries:

```sql
SELECT * FROM TABLE(default.loki(QUERY => 'sum by(level)(bytes_over_time({source="stdout"} | logfmt [5m]))'));
```

```sql
SELECT labels['service_name'], COUNT(*) FROM TABLE(default.loki(QUERY => '{source="stdout"}')) group by labels['service_name'];
```
