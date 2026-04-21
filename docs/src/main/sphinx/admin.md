# Administration

The following documents cover a number of different aspect of configuring,
running, and managing Trino clusters.

```{toctree}
:maxdepth: 1

admin/web-interface
admin/preview-web-interface
admin/logging
admin/tuning
admin/jmx
admin/opentelemetry
admin/openmetrics
admin/properties
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

Event listeners are plugins that allow streaming of query events, such as query
started or query finished, to an external system. 

Using an event listener you can process and store the query events in a separate
system for long periods of time. Some of these external systems can be queried
with Trino for further analysis or reporting.

The following event listeners are available:

```{toctree}
:titlesonly: true

admin/event-listeners-http
admin/event-listeners-kafka
admin/event-listeners-mysql
admin/event-listeners-openlineage
```

Unrelated to event listeners, the coordinator stores information about recent
queries in memory for usage by the [](/admin/web-interface) - see also
`query.max-history` and `query.min-expire-age` in
[](/admin/properties-query-management).

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
* [](admin/properties-client-protocol)
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
