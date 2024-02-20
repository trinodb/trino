# Release 0.165

## General

- Make `AT` a non-reserved keyword.
- Improve performance of {func}`transform`.
- Improve exchange performance by deserializing in parallel.
- Add support for compressed exchanges. This can be enabled with the `exchange.compression-enabled`
  config option.
- Add input and hash collision statistics to {doc}`/sql/explain-analyze` output.

## Hive

- Add support for MAP and ARRAY types in optimized Parquet reader.

## MySQL and PostgreSQL

- Fix connection leak on workers.
