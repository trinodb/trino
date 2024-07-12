Some helpful notes about the Prometheus connector code:

The Prometheus server URI ending in `api/v1/label/__name__/values` is a special URI
used to list metric names which will be table names in our mapping.

Since Prometheus' metric listing endpoint, above, just lists the current metrics
we treat the returned metric names as table names with known, fixed, schema.

The Prometheus metrics backing each table will not change and so
the only columns need to be in the form:
`label VARCHAR`, `value DOUBLE`, `timestamp TIMESTAMP_WITH_TIME_ZONE`

We hard code the table structure, so metadata-uri is not needed.

Splits and thus Prometheus queries are created by choosing endTime. An
offset is added to make sure inclusive chunks do not overlap.

`default` is the name of the only schema.

The EffectiveLimits class handles predicate bounds:

We have a 2x2 matrix of responses that govern how we integrate lower or
upper bounds set in the predicate versus those set by the config
properties. The EffectiveLimits inner class handles the calculations around
the bounds setting.

There are tests for all 4 possibilities of settings for the predicate
bounds.
