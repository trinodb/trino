# Object storage

Object storage systems are commonly used to create data lakes or data lake
houses. These systems provide methods to store objects in a structured manner
and means to access them, for example using an API over HTTP. The objects are
files in various format including ORC, Parquet and others. Object storage
systems are available as service from public cloud providers and others vendors,
or can be self-hosted using commercial as well as open source offerings.

## Object storage connectors

Trino accesses files directly on object storage and remote file system storage.
The following connectors use this direct approach to read and write data files.

* [](/connector/delta-lake)
* [](/connector/hive)
* [](/connector/hudi)
* [](/connector/iceberg)

The connectors all support a variety of protocols and formats used on these
object storage systems, and have separate requirements for metadata
availability.

(file-system-configuration)=
## Configuration

By default, no file system support is activated for your catalog. You must
select and configure one of the following properties to determine the support
for different file systems in the catalog. Each catalog can only use one file
system support.

:::{list-table} File system support properties
:widths: 35, 65
:header-rows: 1

* - Property
  - Description
* - `fs.native-azure.enabled`
  - Activate the [native implementation for Azure Storage
    support](/object-storage/file-system-azure). Defaults to `false`.
* - `fs.native-gcs.enabled`
  - Activate the [native implementation for Google Cloud Storage
    support](/object-storage/file-system-gcs). Defaults to `false`.
* - `fs.native-s3.enabled`
  - Activate the [native implementation for S3 storage
    support](/object-storage/file-system-s3). Defaults to `false`.
* - `fs.hadoop.enabled`
  - Activate [support for HDFS](/object-storage/file-system-hdfs) and [legacy
    support for other file systems](file-system-legacy) using the HDFS
    libraries. Defaults to `false`.
:::

(file-system-native)=
## Native file system support

Trino includes optimized implementations to access the following systems, and
compatible replacements:

* [](/object-storage/file-system-azure)
* [](/object-storage/file-system-gcs)
* [](/object-storage/file-system-s3)
* [](/object-storage/file-system-alluxio)

The native support is available in all four connectors, and must be activated
for use.

(file-system-legacy)=
## Legacy file system support

The default behavior uses legacy libraries that originate from the Hadoop
ecosystem. It should only be used for accessing the Hadoop Distributed File
System (HDFS):

- [](/object-storage/file-system-hdfs)

All four connectors can use the deprecated `hive.*` properties for access to
other object storage system as *legacy* support. These properties will be
removed in a future release. Additional documentation is available with the Hive
connector and relevant migration guides pages:

- [](/connector/hive)
- [Azure Storage migration from hive.azure.* properties](fs-legacy-azure-migration)
- [Google Cloud Storage migration from hive.gcs.* properties](fs-legacy-gcs-migration)
- [S3 migration from hive.s3.* properties](fs-legacy-s3-migration) 

(object-storage-other)=
## Other object storage support

Trino also provides the following additional support and features for object
storage:

* [](/object-storage/file-system-cache)
* [](/object-storage/file-system-alluxio)
* [](/object-storage/metastores)
* [](/object-storage/file-formats)

```{toctree}
:maxdepth: 1
:hidden:

/object-storage/file-system-azure
/object-storage/file-system-gcs
/object-storage/file-system-s3
/object-storage/file-system-hdfs
/object-storage/file-system-cache
/object-storage/file-system-alluxio
/object-storage/metastores
/object-storage/file-formats
```
