# Atop connector

The Atop connector supports reading disk utilization statistics from the [Atop](https://www.atoptool.nl/)
(Advanced System and Process Monitor) Linux server performance analysis tool.

## Requirements

In order to use this connector, the host on which the Trino worker is running
needs to have the `atop` tool installed locally.

## Connector configuration

The connector can read disk utilization statistics on the Trino cluster.
Create a catalog properties file that specifies the Atop connector by
setting the `connector.name` to `atop`.

For example, create the file `etc/catalog/example.properties` with the
following connector properties as appropriate for your setup:

```text
connector.name=atop
atop.executable-path=/usr/bin/atop
```

## Configuration properties

```{eval-rst}
.. list-table::
  :widths: 42, 18, 5, 35
  :header-rows: 1

  * - Property name
    - Default value
    - Required
    - Description
  * - ``atop.concurrent-readers-per-node``
    - ``1``
    - Yes
    - The number of concurrent read operations allowed per node.
  * - ``atop.executable-path``
    - (none)
    - Yes
    - The file path on the local file system for the ``atop`` utility.
  * - ``atop.executable-read-timeout``
    - ``1ms``
    - Yes
    - The timeout when reading from the atop process.
  * - ``atop.max-history-days``
    - ``30``
    - Yes
    - The maximum number of days in the past to take into account for statistics.
  * - ``atop.security``
    - ``ALLOW_ALL``
    - Yes
    - The :doc:`access control </security/built-in-system-access-control>` for the connector.
  * - ``atop.time-zone``
    - System default
    - Yes
    - The time zone identifier in which the atop data is collected. Generally the timezone of the host.
      Sample time zone identifiers: ``Europe/Vienna``, ``+0100``, ``UTC``.
```

## Usage

The Atop connector provides a `default` schema.

The tables exposed by this connector can be retrieved by running `SHOW TABLES`:

```
SHOW TABLES FROM example.default;
```

```text
  Table
---------
 disks
 reboots
(2 rows)
```

The `disks` table offers disk utilization statistics recorded on the Trino node.

```{eval-rst}
.. list-table:: Disks columns
  :widths: 30, 30, 40
  :header-rows: 1

  * - Name
    - Type
    - Description
  * - ``host_ip``
    - ``VARCHAR``
    - Trino worker IP
  * - ``start_time``
    - ``TIMESTAMP(3) WITH TIME ZONE``
    - Interval start time for the statistics
  * - ``end_time``
    - ``TIMESTAMP(3) WITH TIME ZONE``
    - Interval end time for the statistics
  * - ``device_name``
    - ``VARCHAR``
    - Logical volume/hard disk name
  * - ``utilization_percent``
    - ``DOUBLE``
    - The percentage of time the unit was busy handling requests
  * - ``io_time``
    - ``INTERVAL DAY TO SECOND``
    - Time spent for I/O
  * - ``read_requests``
    - ``BIGINT``
    - Number of reads issued
  * - ``sectors_read``
    - ``BIGINT``
    - Number of sectors transferred for reads
  * - ``write_requests``
    - ``BIGINT``
    - Number of writes issued
  * - ``sectors_written``
    - ``BIGINT``
    - Number of sectors transferred for write
```

The `reboots` table offers information about the system reboots performed on the Trino node.

```{eval-rst}
.. list-table:: Reboots columns
  :widths: 30, 30, 40
  :header-rows: 1

  * - Name
    - Type
    - Description
  * - ``host_ip``
    - ``VARCHAR``
    - Trino worker IP
  * - ``power_on_time``
    - ``TIMESTAMP(3) WITH TIME ZONE``
    - The boot/reboot timestamp

```

## SQL support

The connector provides {ref}`globally available <sql-globally-available>` and
{ref}`read operation <sql-read-operations>` statements to access system and process monitor
information on your Trino nodes.
