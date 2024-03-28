# Cloud-scale Trino metrics with OpenMetrics

Trino supports the metrics standard [OpenMetrics](https://openmetrics.io/), that
originated with the open-source systems monitoring and alerting toolkit
[Prometheus](https://prometheus.io/).

Metrics are automatically enabled and available on the coordinator at the
`/metrics` endpoint. The endpoint is protected with the configured
[authentication](security-authentication), identical to the
[](/admin/web-interface) and the [](/develop/client-protocol).  

For example, you can retrieve metrics data from an unsecured Trino server
running on `localhost:8080` with random username `example`:

```shell
curl -H X-Trino-User:foo localhost:8080/metrics
```

The result follows the [OpenMetrics
specification](https://github.com/OpenObservability/OpenMetrics/blob/main/specification/OpenMetrics.md)
and looks similar to the following example output:

```
# TYPE io_airlift_http_client_type_HttpClient_name_ForDiscoveryClient_CurrentResponseProcessTime_Min gauge
io_airlift_http_client_type_HttpClient_name_ForDiscoveryClient_CurrentResponseProcessTime_Min NaN
# TYPE io_airlift_http_client_type_HttpClient_name_ForDiscoveryClient_CurrentResponseProcessTime_P25 gauge
io_airlift_http_client_type_HttpClient_name_ForDiscoveryClient_CurrentResponseProcessTime_P25 NaN
# TYPE io_airlift_http_client_type_HttpClient_name_ForDiscoveryClient_CurrentResponseProcessTime_Total gauge
io_airlift_http_client_type_HttpClient_name_ForDiscoveryClient_CurrentResponseProcessTime_Total 0.0
# TYPE io_airlift_http_client_type_HttpClient_name_ForDiscoveryClient_CurrentResponseProcessTime_P90 gauge
io_airlift_http_client_type_HttpClient_name_ForDiscoveryClient_CurrentResponseProcessTime_P90 NaN
```

The same data is available when using a browser, and logging manually.

Typically, Prometheus or a similar application is configured to monitor the
endpoint. The same application can then be used to inspect the metrics data.

Trino also includes a [](/connector/prometheus) that allows you to query
Prometheus data using SQL.

## Example use

The following steps provide a simple demo setup to run
[Prometheus](https://prometheus.io/) and Trino locally in Docker containers.

Create a shared network for both servers called `platform`:

```shell
docker network create platform
```

Start Trino in the background:

```shell
docker run -d \
  --name=trino \
  --network=platform \
  --network-alias=trino \
  -p 8080:8080 \
  trinodb/trino:latest
```

The preceding command starts Trino and adds it to the `platform` network with
the hostname `trino`. 

Create a `prometheus.yml` configuration file with the following content, that
point Prometheus at the `trino` hostname:

```yaml
scrape_configs:
- job_name: trino
  basic_auth:
    username: trino-user
  static_configs:
    - targets:
      - trino:8080
```

Start Prometheus from the same directory as the configuration file:

```shell
docker run -d \
  --name=prometheus \
  --network=platform \
  -p 9090:9090 \
  --mount type=bind,source=$PWD/prometheus.yml,target=/etc/prometheus/prometheus.yml \
  prom/prometheus
```

The preceding command adds Prometheus to the `platform` network. It also mounts
the configuration file into the container so that metrics from Trino are
gathered by Prometheus.

Now everything is running.

Install and run the [Trino CLI](/client/cli) or any other client application and
submit a query such as `SHOW CATALOGS;` or `SELECT * FROM tpch.tiny.nation;`.

Optionally, log into the [Trino Web UI](/admin/web-interface) at
[http://localhost:8080](http://localhost:8080) with a random username. Press
the **Finished** button and inspect the details for the completed queries.

Access the Prometheus UI at [http://localhost:9090/](http://localhost:9090/),
select **Status** > **Targets** and see the configured endpoint for Trino
metrics.

To see an example graph, select **Graph**, add the metric name
`trino_execution_name_QueryManager_RunningQueries` in the input field and press
**Execute**. Press **Table** for the raw data or **Graph** for a visualization.

As a next step, run more queries and inspect the effect on the metrics.

Once you are done you can stop the containers:

```shell
docker stop prometheus
docker stop trino
```

You can start them again for further testing:

```shell
docker start trino
docker start prometheus
```

Use the following commands to completely remove the network and containers:

```shell
docker rm trino
docker rm prometheus
docker network rm platform
```
