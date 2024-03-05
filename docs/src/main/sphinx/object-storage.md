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

Use the following properties to control the support for different file systems
in a catalog. Each catalog can only use one file system.

:::{list-table} File system support properties
:widths: 35, 65
:header-rows: 1

* - Property
  - Description
* - `fs.hadoop.enabled`
  - Activate the [legacy libraries and implementation based on the Hadoop](file-system-legacy)
    ecosystem. Defaults to `true`.
* - `fs.native-azure.enabled`
  - Activate the [native implementation for Azure Storage
    support](/object-storage/file-system-azure), and deactivate all [legacy
    support](file-system-legacy). Defaults to `false`.
* - `fs.native-gcs.enabled`
  - Activate the [native implementation for Google Cloud Storage
    support](/object-storage/file-system-gcs), and deactivate all [legacy
    support](file-system-legacy). Defaults to `false`.
* - `fs.native-s3.enabled`
  - Activate the [native implementation for S3 storage
    support](/object-storage/file-system-s3), and deactivate all [legacy
    support](file-system-legacy) . Defaults to `false`.

:::

(file-system-native)=
## Native file system support

Trino includes optimized implementations to access the following systems, and
compatible replacements:

* [](/object-storage/file-system-azure)
* [](/object-storage/file-system-gcs)
* [](/object-storage/file-system-s3)

The native support is available in all four connectors, but must be activated
for use.

(file-system-legacy)=
## Legacy file system support

The default behavior uses legacy libraries that originate from the Hadoop
ecosystem. All four connectors can use the related `hive.*` properties for
access to the different object storage system. Documentation is available with
the Hive connector and relevant dedicated pages:

- [](/connector/hive)
- [](/object-storage/legacy-azure)
- [](/object-storage/legacy-gcs)
- [](/object-storage/legacy-cos)
- [](/object-storage/legacy-s3)

(object-storage-other)=
## Other object storage support

Trino also provides the following additional support and features for object
storage:

* [](/object-storage/file-system-cache)
* [](/object-storage/metastores)
* [](/object-storage/file-formats)

```{toctree}
:maxdepth: 1
:hidden:

/object-storage/file-system-azure
/object-storage/file-system-gcs
/object-storage/file-system-s3
/object-storage/legacy-azure
/object-storage/legacy-cos
/object-storage/legacy-gcs
/object-storage/legacy-s3
/object-storage/file-system-cache
/object-storage/metastores
/object-storage/file-formats
```
