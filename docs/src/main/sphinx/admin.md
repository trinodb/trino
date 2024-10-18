# Administration

The following documents cover a number of different aspect of configuring,
running, and managing Trino clusters.

```{toctree}
:maxdepth: 1

admin/web-interface
admin/preview-web-interface
admin/tuning
admin/jmx
admin/opentelemetry
admin/spill
admin/resource-groups
admin/session-property-managers
admin/dist-sort
admin/dynamic-filtering
admin/graceful-shutdown
admin/fault-tolerant-execution
```

Details about connecting [data sources](trino-concept-data-source) as
[catalogs](trino-concept-catalog) are available in the [connector
documentation](/connector).

(admin-event-listeners)=
## Event listeners

```{toctree}
:titlesonly: true

admin/event-listeners-http
admin/event-listeners-kafka
admin/event-listeners-mysql
admin/event-listeners-openlineage
```

## Properties reference

Many aspects for running Trino are [configured with properties](config-properties).
The following pages provide an overview and details for specific topics.

```{toctree}
:maxdepth: 1
:hidden:
admin/properties
```

* [Properties reference overview](admin/properties)
* [](admin/properties-general)
* [](admin/properties-http-server)
* [](admin/properties-resource-management)
* [](admin/properties-query-management)
* [](admin/properties-catalog)
* [](admin/properties-sql-environment)
* [](admin/properties-spilling)
* [](admin/properties-exchange)
* [](admin/properties-task)
* [](admin/properties-write-partitioning)
* [](admin/properties-writer-scaling)
* [](admin/properties-node-scheduler)
* [](admin/properties-optimizer)
* [](admin/properties-logging)
* [](admin/properties-web-interface)
* [](admin/properties-regexp-function)
* [](admin/properties-http-client)
