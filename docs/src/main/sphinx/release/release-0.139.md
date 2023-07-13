# Release 0.139

## Dynamic split concurrency

The number of running leaf splits per query is now dynamically adjusted to improve
overall cluster throughput. `task.initial-splits-per-node` can be used to set
the initial number of splits, and `task.split-concurrency-adjustment-interval`
can be used to change how frequently adjustments happen. The session properties
`initial_splits_per_node` and `split_concurrency_adjustment_interval` can
also be used.

## General

- Fix planning bug that causes some joins to not be redistributed when
  `distributed-joins-enabled` is true.
- Fix rare leak of stage objects and tasks for queries using `LIMIT`.
- Add experimental `task.join-concurrency` config which can be used to increase
  concurrency for the probe side of joins.

## Hive

- Remove cursor-based readers for ORC and DWRF file formats, as they have been
  replaced by page-based readers.
- Fix creating tables on S3 with {doc}`/sql/create-table-as`.
