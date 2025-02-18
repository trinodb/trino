# Trino metrics with OpenMetrics

Trino supports the metrics standard [OpenMetrics](https://openmetrics.io/), that
originated with the open-source systems monitoring and alerting toolkit
[Prometheus](https://prometheus.io/).

Metrics are automatically enabled and available on the coordinator at the
`/metrics` endpoint. The endpoint is protected with the configured
[authentication](security-authentication), identical to the
[](/admin/web-interface) and the [](/client/client-protocol).

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

The user, `foo` in the example, must have read permission to system information
on a secured deployment, and the URL and port must be adjusted accordingly.

Each Trino node, so the coordinator and all workers, provide separate metrics
independently.

Use the property `openmetrics.jmx-object-names` in [](config-properties) to
define  the JMX object names to include when retrieving all metrics. Multiple
object names are must be separated with `|`.  Metrics use the package namespace
for any metric. Use `:*` to expose all metrics. Use `name` to select specific
classes or `type` for specific metric types.

Examples:

* `trino.plugin.exchange.filesystem:name=FileSystemExchangeStats` for metrics
  from the `FileSystemExchangeStats` class in the
  `trino.plugin.exchange.filesystem` package.
* `trino.plugin.exchange.filesystem.s3:name=S3FileSystemExchangeStorageStats`
  for metrics from the `S3FileSystemExchangeStorageStats` class in the
  `trino.plugin.exchange.filesystem.s3` package.
* `io.trino.hdfs:*` for all metrics in the `io.trino.hdfs` package.
* `java.lang:type=Memory` for all memory metrics in the `java.lang` package.

Typically, Prometheus or a similar application is configured to monitor the
endpoint. The same application can then be used to inspect the metrics data.

Trino also includes a [](/connector/prometheus) that allows you to query
Prometheus data using SQL.

## Examples

The following sections provide tips and tricks for your usage with small
examples.

Other configurations with tools such as
[grafana-agent](https://grafana.com/docs/agent/latest/) or [grafana alloy
opentelemetry agent](https://grafana.com/docs/alloy/latest/) are also possible,
and can use platforms such as [Cortex](https://cortexmetrics.io/) or [Grafana
Mimir](https://grafana.com/oss/mimir/mimir) for metrics storage and related
monitoring and analysis.

### Simple example with Docker and Prometheus

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

## Coordinator and worker metrics with Kubernetes

To get a complete picture of the metrics on your cluster, you must access the
coordinator and the worker metrics. This section details tips for setting up for
this scenario with the [Trino Helm chart](https://github.com/trinodb/charts) on
Kubernetes.

Add an annotation to flag all cluster nodes for scraping in your values for the
Trino Helm chart:

```yaml
coordinator:
  annotations:
    prometheus.io/trino_scrape: "true"
worker:
  annotations:
    prometheus.io/trino_scrape: "true"
```

Configure metrics retrieval from the workers in your Prometheus configuration:

```yaml
    - job_name: trino-metrics-worker
      scrape_interval: 10s
      scrape_timeout: 10s
      kubernetes_sd_configs:
        - role: pod
      relabel_configs:
      - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_trino_scrape]
        action: keep # scrape only pods with the trino scrape anotation
        regex: true
      - source_labels: [__meta_kubernetes_pod_container_name]
        action: keep # dont try to scrape non trino container
        regex: trino-worker
      - action: hashmod
        modulus: $(SHARDS)
        source_labels:
        - __address__
        target_label: __tmp_hash
      - action: keep
        regex: $(SHARD)
        source_labels:
        - __tmp_hash
      - source_labels: [__meta_kubernetes_pod_name]
        action: replace
        target_label: pod
      - source_labels: [__meta_kubernetes_pod_container_name]
        action: replace
        target_label: container
      metric_relabel_configs:
          - source_labels: [__name__]
            regex: ".+_FifteenMinute.+|.+_FiveMinute.+|.+IterativeOptimizer.+|.*io_airlift_http_client_type_HttpClient.+"
            action: drop # droping some highly granular metrics 
          - source_labels: [__meta_kubernetes_pod_name]
            regex: ".+"
            target_label: pod
            action: replace 
          - source_labels: [__meta_kubernetes_pod_container_name]
            regex: ".+"
            target_label: container
            action: replace 
            
      scheme: http
      tls_config:
        insecure_skip_verify: true
      basic_auth:
        username: mysuer # replace with a user with system information permission 
        # DO NOT ADD PASSWORD
```

The worker authentication uses a user with access to the system information, yet
does not add a password and uses access via HTTP.

Configure metrics retrieval from the coordinator in your Prometheus
configuration:

```yaml
    - job_name: trino-metrics-coordinator
      scrape_interval: 10s
      scrape_timeout: 10s
      kubernetes_sd_configs:
        - role: pod
      relabel_configs:
      - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_trino_scrape]
        action: keep # scrape only pods with the trino scrape anotation
        regex: true
      - source_labels: [__meta_kubernetes_pod_container_name]
        action: keep # dont try to scrape non trino container
        regex: trino-coordinator
      - action: hashmod
        modulus: $(SHARDS)
        source_labels:
        - __address__
        target_label: __tmp_hash
      - action: keep
        regex: $(SHARD)
        source_labels:
        - __tmp_hash
      - source_labels: [__meta_kubernetes_pod_name]
        action: replace
        target_label: pod
      - source_labels: [__meta_kubernetes_pod_container_name]
        action: replace
        target_label: container
      - action: replace  # overide the address to the https ingress address 
        target_label: __address__
        replacement: {{ .Values.trinourl }} 
      metric_relabel_configs:
          - source_labels: [__name__]
            regex: ".+_FifteenMinute.+|.+_FiveMinute.+|.+IterativeOptimizer.+|.*io_airlift_http_client_type_HttpClient.+"
            action: drop # droping some highly granular metrics 
          - source_labels: [__meta_kubernetes_pod_name]
            regex: ".+"
            target_label: pod
            action: replace 
          - source_labels: [__meta_kubernetes_pod_container_name]
            regex: ".+"
            target_label: container
            action: replace 
            
      scheme: https
      tls_config:
        insecure_skip_verify: true
      basic_auth:
        username: mysuer # replace with a user with system information permission 
        password_file: /some/password/file
```

The coordinator authentication uses a user with access to the system information
and requires authentication and access via HTTPS.
