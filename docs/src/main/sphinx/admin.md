# Administration

```{toctree}
:maxdepth: 1

admin/web-interface
admin/preview-web-interface
admin/tuning
admin/jmx
admin/opentelemetry
admin/properties
admin/spill
admin/resource-groups
admin/session-property-managers
admin/dist-sort
admin/dynamic-filtering
admin/graceful-shutdown
admin/fault-tolerant-execution
```

(admin-event-listeners)=
## Event listeners

Event listeners are plugins that allow streaming of query events to an external
system. By default, query events are only kept in memory for a limited number of
queries configured with `query.max-history`, see
[](/admin/properties-query-management). Only these events are available in the
[](/admin/web-interface). Using an event listener you can process and store the
query history in a separate system for more queries and longer periods of time.
Some of these external systems can potentially be queried with Trino for further
analysis or reporting.

The following event listeners are available:

```{toctree}
:titlesonly: true

admin/event-listeners-http
admin/event-listeners-kafka
admin/event-listeners-mysql
admin/event-listeners-openlineage
```
