====================
Black Hole Connector
====================

Primarily Black Hole connector is designed for high performance testing of
other components. It works like the ``/dev/null`` device on Unix-like
operating systems for data writing and like ``/dev/null`` or ``/dev/zero``
for data reading. However, it also has some other features that allow testing Presto
in a more controlled manner. Metadata for any tables created via this connector
is kept in memory on the coordinator and discarded when Presto restarts.
Created tables are by default always empty, and any data written to them
is ignored and data reads return no rows.

During table creation, a desired rows number can be specified.
In such cases, writes behave in the same way, but reads
always return the specified number of some constant rows.
You shouldn't rely on the content of such rows.

.. warning::

    This connector does not work properly with multiple coordinators,
    since each coordinator has different metadata.

Configuration
-------------

To configure the Black Hole connector, create a catalog properties file
``etc/catalog/blackhole.properties`` with the following contents:

.. code-block:: none

    connector.name=blackhole

Examples
--------

Create a table using the blackhole connector::

    CREATE TABLE blackhole.test.nation AS
    SELECT * from tpch.tiny.nation;

Insert data into a table in the blackhole connector::

    INSERT INTO blackhole.test.nation
    SELECT * FROM tpch.tiny.nation;

Select from the blackhole connector::

    SELECT count(*) FROM blackhole.test.nation;

The above query always returns zero.

Create a table with a constant number of rows (500 * 1000 * 2000)::

    CREATE TABLE blackhole.test.nation (
      nationkey bigint,
      name varchar
    )
    WITH (
      split_count = 500,
      pages_per_split = 1000,
      rows_per_page = 2000
    );

Now query it::

    SELECT count(*) FROM blackhole.test.nation;

The above query returns 1,000,000,000.

Length of variable length columns can be controlled using the ``field_length``
table property (default value is equal to 16)::

    CREATE TABLE blackhole.test.nation (
      nationkey bigint,
      name varchar
    )
    WITH (
      split_count = 500,
      pages_per_split = 1000,
      rows_per_page = 2000,
      field_length = 100
    );

The consuming and producing rate can be slowed down
using the ``page_processing_delay`` table property.
Setting this property to ``5s`` leads to a 5 second
delay before consuming or producing a new page::

    CREATE TABLE blackhole.test.delay (
      dummy bigint
    )
    WITH (
      split_count = 1,
      pages_per_split = 1,
      rows_per_page = 1,
      page_processing_delay = '5s'
    );

