```markdown
# Grafana Monitoring with Trino


## Overview

Starburst is a powerful distributed SQL engine that allows teams to query data across multiple sources. However, when it comes to monitoring Starburst clusters, there are some limitations:

- The Starburst web UI gives basic cluster health
- The Starburst API exposes some system metrics
- But full metrics (CPU, JVM, memory, total queries, slow queries, etc.) require additional integration

We manage **50+ Starburst clusters** running across multiple environments (OpenShift, Kubernetes, Linux). Manually going into the Starburst UI or CLI to run queries and check metrics was becoming too complex.

That’s why we came up with a solution using:

- Grafana dashboards
- Grafana Trino Plugin
- Prometheus + JMX Exporter → full Starburst metrics export

## Why Native Starburst Metrics Are Limited

Out of the box, Starburst exposes basic system metrics:

- Active queries
- Running nodes
- Query latency
- Query errors

But for deeper monitoring, you need:

- JVM metrics (CPU usage, memory, GC, threads)
- Detailed query metrics (count, latency, slow queries)
- Resource usage per cluster
- Cluster-wide trends over time
- Alerting on custom thresholds

Starburst does not expose all of these metrics directly in the UI or API. This is why we need an additional metrics pipeline.

## Solution Architecture

To fully monitor Starburst, we built this architecture:

```

+---------------------+    JMX Exporter    +---------------+
\| Starburst Cluster 1 |  --> Port 8081 -->  | Prometheus    |
+---------------------+                   +---------------+

+---------------------+    JMX Exporter    +---------------+
\| Starburst Cluster N |  --> Port 8081 -->  | Prometheus    |
+---------------------+                   +---------------+

```
            Prometheus --> Grafana --> Dashboards / Alerts
            + Grafana Trino Plugin --> SQL-based dashboards
```

````

### Components:

- **JMX Exporter** → exposes Starburst JVM + query metrics to Prometheus
- **Prometheus** → scrapes all Starburst clusters at regular intervals
- **Grafana** → visualizes metrics
- **Grafana Trino Plugin** → allows you to run SQL queries on Starburst directly and show results in Grafana panels

## Why Use Grafana Trino Plugin?

The Grafana Trino Plugin is extremely useful for Starburst monitoring because:

- It connects Grafana directly to Starburst as a SQL data source
- You can run Starburst SQL queries in Grafana panels
- You can refresh results dynamically
- You can build dashboards with panels per cluster
- It scales to multiple clusters easily

**Without** this plugin, you would need to:

- Manually log into each Starburst UI
- Run the same query multiple times
- Manually copy/paste results  
→ Very slow and not scalable.

**With** Grafana Trino Plugin:

- Configure each cluster as a Trino data source in Grafana
- Write SQL queries once
- Save dashboards
- Refresh all clusters with one click
- Add alerts and trends → no manual work

## Grafana Trino Plugin - Overview

The Trino datasource plugin allows you to query and visualize Trino (and Starburst) data inside Grafana.

### Installation

Run Grafana with the plugin using Docker:

```bash
docker run -d -p 3000:3000 \
  -v "$(pwd):/var/lib/grafana/plugins/trino" \
  -e "GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS=trino-datasource" \
  --name=grafana \
  grafana/grafana-oss
````

### Features

* Authentication: HTTP Basic, TLS client, OAuth, JWT
* Raw SQL editor — run any query
* Works with Starburst OSS, Galaxy, and Enterprise
* Macros supported → useful for time series panels

### Macros Supported

* `$timeFrom($column)` — lower boundary of time range
* `$timeTo($column)` — upper boundary of time range
* `$timeGroup($column, $interval)` — group by time
* `$dateFilter($column)` — date range filter
* `$timeFilter($column)` — timestamp range filter
* `$unixEpochFilter($column)` — unix timestamp range

### Example Query Using Macros

```sql
SELECT
  atimestamp AS time,
  metric_value AS value
FROM starburst_metrics_table
WHERE $__timeFilter(atimestamp) AND cluster_name IN($cluster)
ORDER BY atimestamp ASC
```

## How To Set It Up

### 1. Enable JMX Exporter in Starburst

Configure JMX agent in your Starburst deployment:

```bash
start.args=-javaagent:/opt/starburst/jmx_prometheus_javaagent.jar=8081:/opt/starburst/jmx_exporter_config.yaml
```

### 2. Configure Prometheus

Add all your Starburst clusters:

```yaml
- job_name: 'starburst'
  static_configs:
    - targets: ['starburst-cluster-1:8081', 'starburst-cluster-2:8081']
```

### 3. Install Grafana Trino Plugin

* Add Trino Plugin in Grafana → Data Sources
* Provide Starburst cluster URL + credentials
* Test connection

### 4. Build Dashboards

* Create a panel → select Trino Plugin data source
* Write your Starburst SQL query:

```sql
SELECT state, count(*) FROM system.runtime.queries GROUP BY state
```

* Save panel → use macros for time filters
* Build dashboards with multiple panel per cluster

## Benefits

* Monitor multiple Starburst clusters from one Grafana dashboard
* Run Starburst SQL queries as panels → live refresh
* Combine Prometheus metrics + SQL panels
* Full CPU, JVM, memory, query stats
* Add Slack / PagerDuty alerts for Starburst anomalies

## Limitations and Tips

* Starburst native metrics are limited → configure JMX Exporter carefully
* Grafana Trino Plugin is best for SQL panels → use Prometheus panels for JVM metrics
* Test scrape intervals carefully → avoid overloading clusters
* Use macros to make dashboards dynamic and reusable across clusters

## Conclusion

If you want full Starburst monitoring, this is the best architecture:

* Enable JMX Exporter → Prometheus → Grafana
* Use Grafana Trino Plugin → SQL dashboards → live queries
* Monitor multiple clusters from one place
* Add alerts, trends, historical analysis
* Share dashboards with teams

## Final Architecture Summary

```
Starburst --> JMX Exporter --> Prometheus --> Grafana Panels + Trino Plugin SQL Queries --> Slack / Alerts / Visual Dashboards
```