# Release 483 (DD Mmm YYYY)

## Elasticsearch connector

* Add support for [table statistics](/optimizer/statistics). ({issue}`XXXXX`)
* Add support for [dynamic filtering](/admin/dynamic-filtering). ({issue}`XXXXX`)
* Improve performance of aggregation queries, including queries with `GROUP BY`,
  by pushing `count`, `sum`, `min`, `max`, `avg`, and `approx_distinct` down to
  Elasticsearch. ({issue}`XXXXX`)
* Improve performance of queries with predicates or aggregations on `text`
  fields that have a `keyword` sub-field. ({issue}`XXXXX`)
* Add an opt-in full-text pushdown mode that pushes predicates and dynamic
  filters on analyzed `text` fields to Elasticsearch as `match_phrase`
  queries. ({issue}`XXXXX`)
