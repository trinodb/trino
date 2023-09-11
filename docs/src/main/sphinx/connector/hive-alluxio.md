# Hive connector with Alluxio

The {doc}`hive` can read and write tables stored in the [Alluxio Data Orchestration
System](https://www.alluxio.io/),
leveraging Alluxio's distributed block-level read/write caching functionality.
The tables must be created in the Hive metastore with the `alluxio://`
location prefix (see [Running Apache Hive with Alluxio](https://docs.alluxio.io/os/user/stable/en/compute/Hive.html)
for details and examples).

Trino queries will then transparently retrieve and cache files or objects from
a variety of disparate storage systems including HDFS and S3.

## Setting up Alluxio with Trino

For information on how to setup, configure, and use Alluxio, refer to [Alluxio's
documentation on using their platform with Trino](https://docs.alluxio.io/ee/user/stable/en/compute/Trino.html).
