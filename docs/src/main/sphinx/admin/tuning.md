# Tuning Trino

The default Trino settings should work well for most workloads. The following
information may help you, if your cluster is facing a specific performance problem.

## Config properties

See {doc}`/admin/properties`.

## JVM settings

The following can be helpful for diagnosing garbage collection (GC) issues:

```text
-Xlog:gc*,safepoint::time,level,tags,tid
```
