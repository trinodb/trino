# Object storage file formats

Object storage connectors support one or more file formats specified by the
underlying data source.

(orc-format-configuration)=
## ORC format configuration properties

The following properties are used to configure the read and write operations
with ORC files performed by supported object storage connectors:

:::{list-table} ORC format configuration properties
:widths: 30, 50, 20
:header-rows: 1

* - Property Name
  - Description
  - Default
* - `orc.time-zone`
  - Sets the default time zone for legacy ORC files that did not declare a time
    zone.
  - JVM default
* - `orc.bloom-filters.enabled`
  - Enable bloom filters for predicate pushdown.
  - `false`
* - `orc.read-legacy-short-zone-id`
  - Allow reads on ORC files with short zone ID in the stripe footer.
  - `false`
:::

[](file-compression) is automatically performed and some details can be
configured.

## ORC support limitations

[Trino ignores Calendar entry in ORC file metadata.](https://github.com/trinodb/trino/issues/26865)
As a result Trino always treats dates and timestamps as values written using
proleptic Gregorian calendar. This causes incorrect values read when reading
date/time values before Oct 15, 1582 that were written using hybrid
Julian-Gregorian calendar.

(parquet-format-configuration)=
## Parquet format configuration properties

The following properties are used to configure the read and write operations
with Parquet files performed by supported object storage connectors:

:::{list-table} Parquet format configuration properties
:widths: 30, 50, 20
:header-rows: 1

* - Property Name
  - Description
  - Default
* - `parquet.time-zone`
  - Adjusts timestamp values to a specific time zone. For Hive 3.1+, set this to
    UTC.
  - JVM default
* - `parquet.writer.validation-percentage`
  - Percentage of parquet files to validate after write by re-reading the whole
    file. The equivalent catalog session property is
    `parquet_optimized_writer_validation_percentage`. Validation can be turned
    off by setting this property to `0`.
  - `5`
* - `parquet.writer.page-size`
  - Maximum size of pages written by Parquet writer. The equivalent catalog 
    session property is `parquet_writer_page_size`.
  - `1 MB`
* - `parquet.writer.page-value-count`
  - Maximum values count of pages written by Parquet writer. The equivalent 
    catalog session property is `parquet_writer_page_value_count`.
  - `80000`
* - `parquet.writer.block-size`
  - Maximum size of row groups written by Parquet writer. The equivalent 
    catalog session property is `parquet_writer_block_size`.
  - `128 MB`
* - `parquet.writer.batch-size`
  - Maximum number of rows processed by the parquet writer in a batch.
    The equivalent catalog session property is `parquet_writer_batch_size`.
  - `10000`
* - `parquet.use-bloom-filter`
  - Whether bloom filters are used for predicate pushdown when reading Parquet
    files. Set this property to `false` to disable the usage of bloom filters by
    default. The equivalent catalog session property is
    `parquet_use_bloom_filter`.
  - `true`
* - `parquet.use-column-index`
  - Skip reading Parquet pages by using Parquet column indices. The equivalent
    catalog session property is `parquet_use_column_index`. Only supported by
    the Delta Lake and Hive connectors.
  - `true`
* - `parquet.ignore-statistics`
  - Ignore statistics from Parquet to allow querying files with corrupted or
    incorrect statistics. The equivalent catalog session property is
    `parquet_ignore_statistics`.
  - `false`
* - `parquet.max-read-block-row-count`
  - Sets the maximum number of rows read in a batch. The equivalent catalog
    session property is named `parquet_max_read_block_row_count` and supported
    by the Delta Lake, Hive, Iceberg and Hudi connectors.
  - `8192`
* - `parquet.small-file-threshold`
  - [Data size](prop-type-data-size) below which a Parquet file is read
    entirely. The equivalent catalog session property is named
    `parquet_small_file_threshold`.
  - `3MB`
* - `parquet.experimental.vectorized-decoding.enabled`
  - Enable using Java Vector API (SIMD) for faster decoding of parquet files.
    The equivalent catalog session property is
    `parquet_vectorized_decoding_enabled`.
  - `true`
* - `parquet.max-footer-read-size`
  - Sets the maximum allowed read size for Parquet file footers. Attempting to
    read a file with a footer larger than this value will result in an error.
    This prevents workers from going into full GC or crashing due to poorly
    configured Parquet writers.
  - `15MB`
* - `parquet.max-page-read-size`
  - Maximum allowed size of a parquet page during reads. Files with parquet pages
    larger than this will generate an exception on read.
  - `500MB`
:::

[](file-compression) is automatically performed and some details can be
configured.
