# Spill to disk

## Overview

In the case of memory intensive operations, Trino allows offloading
intermediate operation results to disk. The goal of this mechanism is to
enable execution of queries that require amounts of memory exceeding per query
or per node limits.

The mechanism is similar to OS level page swapping. However, it is
implemented on the application level to address specific needs of Trino.

Properties related to spilling are described in {doc}`properties-spilling`.

## Memory management and spill

By default, Trino kills queries, if the memory requested by the query execution
exceeds session properties `query_max_memory` or
`query_max_memory_per_node`. This mechanism ensures fairness in allocation
of memory to queries, and prevents deadlock caused by memory allocation.
It is efficient when there is a lot of small queries in the cluster, but
leads to killing large queries that don't stay within the limits.

To overcome this inefficiency, the concept of revocable memory was introduced. A
query can request memory that does not count toward the limits, but this memory
can be revoked by the memory manager at any time. When memory is revoked, the
query runner spills intermediate data from memory to disk and continues to
process it later.

In practice, when the cluster is idle, and all memory is available, a memory
intensive query may use all of the memory in the cluster. On the other hand,
when the cluster does not have much free memory, the same query may be forced to
use disk as storage for intermediate data. A query, that is forced to spill to
disk, may have a longer execution time by orders of magnitude than a query that
runs completely in memory.

Please note that enabling spill-to-disk does not guarantee execution of all
memory intensive queries. It is still possible that the query runner fails
to divide intermediate data into chunks small enough so that every chunk fits into
memory, leading to `Out of memory` errors while loading the data from disk.

## Spill disk space

Spilling intermediate results to disk, and retrieving them back, is expensive
in terms of IO operations. Thus, queries that use spill likely become
throttled by disk. To increase query performance, it is recommended to
provide multiple paths on separate local devices for spill (property
`spiller-spill-path` in {doc}`properties-spilling`).

The system drive should not be used for spilling, especially not to the drive where the JVM
is running and writing logs. Doing so may lead to cluster instability. Additionally,
it is recommended to monitor the disk saturation of the configured spill paths.

Trino treats spill paths as independent disks (see [JBOD](https://wikipedia.org/wiki/Non-RAID_drive_architectures#JBOD)), so
there is no need to use RAID for spill.

## Spill compression

When spill compression is enabled (`spill-compression-enabled` property in
{doc}`properties-spilling`), spilled pages are compressed, before being
written to disk. Enabling this feature can reduce disk IO at the cost
of extra CPU load to compress and decompress spilled pages.

## Spill encryption

When spill encryption is enabled (`spill-encryption-enabled` property in
{doc}`properties-spilling`), spill contents are encrypted with a randomly generated
(per spill file) secret key. Enabling this increases CPU load and reduces throughput
of spilling to disk, but can protect spilled data from being recovered from spill files.
Consider reducing the value of `memory-revoking-threshold` when spill
encryption is enabled, to account for the increase in latency of spilling.

## Supported operations

Not all operations support spilling to disk, and each handles spilling
differently. Currently, the mechanism is implemented for the following
operations.

### Joins

During the join operation, one of the tables being joined is stored in memory.
This table is called the build table. The rows from the other table stream
through and are passed onto the next operation, if they match rows in the build
table. The most memory-intensive part of the join is this build table.

When the task concurrency is greater than one, the build table is partitioned.
The number of partitions is equal to the value of the `task.concurrency`
configuration parameter (see {doc}`properties-task`).

When the build table is partitioned, the spill-to-disk mechanism can decrease
the peak memory usage needed by the join operation. When a query approaches the
memory limit, a subset of the partitions of the build table gets spilled to disk,
along with rows from the other table that fall into those same partitions. The
number of partitions, that get spilled, influences the amount of disk space needed.

Afterward, the spilled partitions are read back one-by-one to finish the join
operation.

With this mechanism, the peak memory used by the join operator can be decreased
to the size of the largest build table partition. Assuming no data skew, this
is `1 / task.concurrency` times the size of the whole build table.

### Aggregations

Aggregation functions perform an operation on a group of values and return one
value. If the number of groups you're aggregating over is large, a significant
amount of memory may be needed. When spill-to-disk is enabled, if there is not
enough memory, intermediate cumulated aggregation results are written to disk.
They are loaded back and merged with a lower memory footprint.

### Order by

If your trying to sort a larger amount of data, a significant amount of memory
may be needed. When spill to disk for `order by` is enabled, if there is not enough
memory, intermediate sorted results are written to disk. They are loaded back and
merged with a lower memory footprint.

### Window functions

Window functions perform an operator over a window of rows, and return one value
for each row. If this window of rows is large, a significant amount of memory may
be needed. When spill to disk for window functions is enabled, if there is not enough
memory, intermediate results are written to disk. They are loaded back and merged
when memory is available. There is a current limitation that spill does not work
in all cases, such as when a single window is very large.
