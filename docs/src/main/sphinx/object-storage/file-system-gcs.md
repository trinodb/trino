# Google Cloud Storage file system support

Trino includes a native implementation to access [Google Cloud Storage
(GCS)](https://cloud.google.com/storage/) with a catalog using the Delta Lake,
Hive, Hudi, or Iceberg connectors.

Enable the native implementation with `fs.native-gcs.enabled=true` in your
catalog properties file.

## General configuration

Use the following properties to configure general aspects of Google Cloud
Storage file system support:

:::{list-table}
:widths: 40, 60
:header-rows: 1

* - Property
  - Description
* - `fs.native-gcs.enabled`
  - Activate the native implementation for Google Cloud Storage support, and
    deactivate all [legacy support](file-system-legacy). Defaults to `false`.
    Must be set to `true` for all other properties be used.
* - `gcs.project-id`
  - Identifier for the project on Google Cloud Storage.
* - `gcs.client.max-retries`
  - Maximum number of RPC attempts. Defaults to 20.
* - `gcs.client.backoff-scale-factor`
  - Scale factor for RPC retry delays. Defaults to 3.
* - `gcs.client.max-retry-time`
  - Total time [duration](prop-type-duration) limit for RPC call retries.
    Defaults to `25s`.
* - `gcs.client.min-backoff-delay`
  - Minimum delay [duration](prop-type-duration) between RPC retries. Defaults
    to `10ms`.
* - `gcs.client.max-backoff-delay`
  - Maximum delay [duration](prop-type-duration) between RPC retries. Defaults
    to `2s`.
* - `gcs.read-block-size`
  - Minimum [data size](prop-type-data-size) for blocks read per RPC. Defaults
    to `2MiB`. See `com.google.cloud.BaseStorageReadChannel`.
* - `gcs.write-block-size`
  - Minimum [data size](prop-type-data-size) for blocks written per RPC. The
    Defaults to `16MiB`. See `com.google.cloud.BaseStorageWriteChannel`.
* - `gcs.page-size`
  - Maximum number of blobs to return per page. Defaults to 100.
* - `gcs.batch-size`
  - Number of blobs to delete per batch. Defaults to 100. [Recommended batch
    size](https://cloud.google.com/storage/docs/batch) is 100.
:::

## Authentication

Use one of the following properties to configure the authentication to Google
Cloud Storage:

:::{list-table}
:widths: 40, 60
:header-rows: 1

* - Property
  - Description
* - `gcs.use-access-token`
  - Flag to set usage of a client-provided OAuth 2.0 token to access Google
    Cloud Storage. Defaults to `false`.
* - `gcs.json-key`
  - Your Google Cloud service account key in JSON format. Not to be set together
    with `gcs.json-key-file-path`.
* - `gcs.json-key-file-path`
  - Path to the JSON file on each node that contains your Google Cloud Platform
    service account key. Not to be set together with `gcs.json-key`.
:::
