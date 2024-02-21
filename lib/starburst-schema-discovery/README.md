# Schema Discovery

Library, CLI and Trino System table to crawl Hive FileSystem directories and discover schema hierarchies.

## Versions

Use version `20221109-074309` with Trino versions older than 402. The latest version of Schema
Discovery requires Trino 402+.

## System Table

Integrates into Trino as a System Table. Calling convention:

```sql
SELECT sql, json, metadata_json, errors FROM catalog.schema_discovery.discovery WHERE uri = '...'
```

Additionally, `options` can be included in the WHERE clause (see CLI below for available options). `metadata_json` can be used to generate an update/diff from a previous version.

## CLI

Get the CLI uber JAR by going to the [Releases Page](https://github.com/starburstdata/schema-discovery/releases) and downloading the latest release JAR.

### Quick Start

Use `gimme-aws-creds` (or similar tool) to initialize your command line with credentials. Then, run the Schema Discovery CLI with the URI of the object store root to scan. E.g.

```
java -jar path/to/jar/schema-discovery-cli-VERSION.jar s3://starburst-demo/SalesDemo
```

### Details

```
java -jar path/to/jar/schema-discovery-cli-VERSION.jar
```

Call with `-h` to get help. Currently:

```
Usage: discover [-hV] [--partitions] [--buckets=<bucketQty>]
                [--catalog=<catalogName>] [--dialect=<dialect>]
                [--previous=<previousMetadata>] [-s=<schemaName>]
                [-t=<timeout>] [--format=<outputType>]...
                [-o=<String=String>]... [--rescan-uri=<uri>
                --rescan-type=<format> [--rescan-metadata=<metadata>]] URI
      URI                   URI to scan (e.g. s3://mys3/bucket)
      --buckets=<bucketQty> Bucket qty
                              Default: 10
      --catalog=<catalogName>
                            Catalog name to use for table creation
      --dialect=<dialect>   Dialect of hive connector. Trino or Galxy.
                              Default: TRINO
      --format=<outputType> Output format - Valid values: JSON, SQL, METADATA
                              Default: [SQL]
  -h, --help                Show this help message and exit.
  -o, --option=<String=String>
                            Discovery options - see below for details
      --partitions          Include partitions
                              Default: true
      --previous=<previousMetadata>
                            Previous metadata - output will be changes/diff
                              from previous
      --rescan-metadata=<metadata>
                            Metadata from initial scan. If provided, generated
                              output will contain initial scan combined with
                              this rescan
      --rescan-type=<format>
                            To rescan a single table - discovery type to use
                              (requires --rescan-uri) - Valid values: JSON,
                              CSV, ORC, PARQUET, ICEBERG, DELTA_LAKE, ERROR
      --rescan-uri=<uri>    To rescan a single table - URI to rescan (requires
                              --rescan-type)
  -s, --schema=<schemaName> Schema name
                              Default: discovered
  -t, --timeout=<timeout>   Schema Discovery timeout
                              Default: PT5M
  -V, --version             Print version information and exit.

=============== Options ===============

Note: table specific options can be set by prefixing with "<schema>.<table>."

===============
Option Defaults
===============
dateFormat = yyyy-MM-dd
positiveInf = Inf
generatedHeadersFormat = COL%d
nanValue = NaN
locale = US
lineSeparator =
nullValue =
complexHadoop = false
sampleLinesModulo = 3
maxSampleFilesPerTable = 5
skipFileExtensionsCheck = false
quote = "
includePatterns = **
delimiter = ,
escape = \
headers = true
ignoreTrailingWhiteSpace = false
discoveryMode = NORMAL
maxSampleLines = 10
excludePatterns = .*
sampleFilesPerTableModulo = 3
partitionProjection = false
negativeInf = -Inf
encoding = UTF-8
inferJsonStringDecimal = false
supportBuckets = false
ignoreLeadingWhiteSpace = false
maxSampleTables = 2147483647
hiveSkipHiddenFiles = false
timestampFormat = yyyy-MM-dd HH:mm:ss[.SSSSSSS]
comment = #

================
Standard Options
================
encoding                      Charset to use when reading files
locale                        Locale for date parsing
dateFormat                    Date format pattern
timestampFormat               Time format pattern
nanValue                      Value to use for not-a-number
positiveInf                   Value to use for positive infinity
negativeInf                   Value to use for negative infinity
includePatterns               HDFS include GLOB, split by |
excludePatterns               HDFS exclude GLOB, split by |
maxSampleLines                Max lines to sample from each file
sampleLinesModulo             Each SAMPLE_LINES_MODULO line is sampled. i.e. if 3, every 3rd line is sampled
sampleFilesPerTableModulo     Each SAMPLE_FILES_PER_TABLE_MODULO file is sampled. i.e. if 3, every 3rd file is sampled
maxSampleTables               Max Tables to discovery
maxSampleFilesPerTable        Max files per table to sample
supportBuckets                If "true" attempt to infer buckets
forceTableFormat              Optional = force the table format [JSON, CSV, ORC, PARQUET, ICEBERG, DELTA_LAKE, ERROR]
discoveryMode                 Discovery mode, NORMAL - default discovery mode, RECURSIVE_DIRECTORIES - ignores
                                directories in top level tables [RECURSIVE_DIRECTORIES, NORMAL]
partitionProjection           If "true" attempt to infer partition projection
skipFileExtensionsCheck       Whether to skip looking at file extension when discovering file format. Use in case of
                                mismatched file format / extension
inferJsonStringDecimal        Try to parse string json values into Trino's DECIMAL type
hiveSkipHiddenFiles           Whether to skip files/directories starting with _ or .

================
CSV/TEXT Options
================
headers                       If "true" treat first line as columns names
generatedHeadersFormat        Pattern to use if column names are auto-generated
delimiter                     Text file delimiter
quote                         Text file quote
escape                        Text file escape
comment                       Text file comment
nullValue                     Text file null value
ignoreLeadingWhiteSpace       If "true" ignore leading white space
ignoreTrailingWhiteSpace      If "true" ignore trailing white space
complexHadoop                 If "true" try to discover arrays, structs and maps
lineSeparator                 Text line separator
```

# Shallow discovery

Library, CLI and Trino System table to crawl Hive FileSystem directories and shallowly discover schema hierarchies.
Do not return columns, and do not crawl files in partitions, should be used as preliminary run for setting up
proper exclude/include patterns.

## System Table

Integrates into Trino as a System Table. Calling convention:

```sql
SELECT shallow_metadata_json FROM catalog.schema_discovery.shallow_discovery WHERE uri = '...'
```
