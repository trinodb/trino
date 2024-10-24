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

Event listeners are plugins that allow streaming of query events, such query
started or query finished to an external system. 

By default, query events are only reflected as internal state changes and as
change of the displayed query information in the [](/admin/web-interface). The
number of queries is controlled by `query.max-history`, see
[](/admin/properties-query-management). 

Using an event listener you can process and store the query events in a separate
system for more queries and therefore longer periods of time. Some of these
external systems can potentially be queried with Trino for further analysis or
reporting.

The following event listeners are available:

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
