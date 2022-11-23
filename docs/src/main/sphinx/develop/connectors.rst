==========
Connectors
==========

Connectors are the source of all data for queries in Trino. Even if your data
source doesn't have underlying tables backing it, as long as you adapt your data
source to the API expected by Trino, you can write queries against this data.

ConnectorFactory
----------------

Instances of your connector are created by a ``ConnectorFactory`` instance which
is created when Trino calls ``getConnectorFactory()`` on the plugin. The
connector factory is a simple interface responsible for providing the connector
name and creating an instance of a ``Connector`` object. A basic connector
implementation that only supports reading, but not writing data, should return
instances of the following services:

* :ref:`connector-metadata`
* :ref:`connector-split-manager`
* :ref:`connector-record-set-provider` or :ref:`connector-page-source-provider`

Configuration
^^^^^^^^^^^^^

The ``create()`` method of the connector factory receives a ``config`` map,
containing all properties from the catalog properties file. It can be used
to configure the connector, but because all the values are strings, they
might require additional processing if they represent other data types.
It also doesn't validate if all the provided properties are known. This
can lead to the connector behaving differently than expected when a
connector ignores a property due to the user making a mistake in
typing the name of the property.

To make the configuration more robust, define a Configuration class. This
class describes all the available properties, their types, and additional
validation rules.


.. code-block:: java

  import io.airlift.configuration.Config;
  import io.airlift.configuration.ConfigDescription;
  import io.airlift.configuration.ConfigSecuritySensitive;
  import io.airlift.units.Duration;
  import io.airlift.units.MaxDuration;
  import io.airlift.units.MinDuration;

  import javax.validation.constraints.NotNull;

  public class ExampleConfig
  {
      private String secret;
      private Duration timeout = Duration.succinctDuration(10, TimeUnit.SECONDS);

      public String getSecret()
      {
          return secret;
      }

      @Config("secret")
      @ConfigDescription("Secret required to access the data source")
      @ConfigSecuritySensitive
      public ExampleConfig setSecret(String secret)
      {
          this.secret = secret;
          return this;
      }

      @NotNull
      @MaxDuration("10m")
      @MinDuration("1ms")
      public Duration getTimeout()
      {
          return timeout;
      }

      @Config("timeout")
      public ExampleConfig setTimeout(Duration timeout)
      {
          this.timeout = timeout;
          return this;
      }
  }

The preceding example defines two configuration properties and makes
the connector more robust by:

* defining all supported properties, which allows detecting spelling mistakes
  in the configuration on server startup
* defining a default timeout value, to prevent connections getting stuck
  indefinitely
* preventing invalid timeout values, like 0 ms, that would make
  all requests fail
* parsing timeout values in different units, detecting invalid values
* preventing logging the secret value in plain text

The configuration class needs to be bound in a Guice module:

.. code-block:: java

  import com.google.inject.Binder;
  import com.google.inject.Module;

  import static io.airlift.configuration.ConfigBinder.configBinder;

  public class ExampleModule
          implements Module
  {
      public ExampleModule()
      {
      }

      @Override
      public void configure(Binder binder)
      {
          configBinder(binder).bindConfig(ExampleConfig.class);
      }
  }


And then the module needs to be initialized in the connector factory, when
creating a new instance of the connector:

.. code-block:: java

  @Override
  public Connector create(String connectorName, Map<String, String> config, ConnectorContext context)
  {
      requireNonNull(config, "config is null");
      Bootstrap app = new Bootstrap(new ExampleModule());
      Injector injector = app
              .doNotInitializeLogging()
              .setRequiredConfigurationProperties(config)
              .initialize();

      return injector.getInstance(ExampleConnector.class);
  }

.. note::

  Environment variables in the catalog properties file
  (ex. ``secret=${ENV:SECRET}``) are resolved only when using
  the ``io.airlift.bootstrap.Bootstrap`` class to initialize the module.
  See :doc:`/security/secrets` for more information.

If you end up needing to define multiple catalogs using the same connector
just to change one property, consider adding support for schema and/or
table properties. That would allow a more fine-grained configuration.
If a connector doesn't support managing the schema, query predicates for
selected columns could be used as a way of passing the required configuration
at run time.

For example, when building a connector to read commits from a Git repository,
the repository URL could be a configuration property. But this would result
in a catalog being able to return data only from a single repository.
Alternatively, it can be a column, where every select query would require
a predicate for it:

.. code-block:: sql

  SELECT *
  FROM git.default.commits
  WHERE url = 'https://github.com/trinodb/trino.git'


.. _connector-metadata:

ConnectorMetadata
-----------------

The connector metadata interface allows Trino to get a lists of schemas,
tables, columns, and other metadata about a particular data source.

A basic read-only connector should implement the following methods:

* ``listSchemaNames``
* ``listTables``
* ``streamTableColumns``
* ``getTableHandle``
* ``getTableMetadata``
* ``getColumnHandles``
* ``getColumnMetadata``

If you are interested in seeing strategies for implementing more methods,
look at the :doc:`example-http` and the Cassandra connector. If your underlying
data source supports schemas, tables, and columns, this interface should be
straightforward to implement. If you are attempting to adapt something that
isn't a relational database, as the Example HTTP connector does, you may
need to get creative about how you map your data source to Trino's schema,
table, and column concepts.

The connector metadata interface allows to also implement other connector
features, like:

* Schema management, which is creating, altering and dropping schemas, tables,
  table columns, views, and materialized views.
* Support for table and column comments, and properties.
* Schema, table and view authorization.
* Executing :doc:`table-functions`.
* Providing table statistics used by the Cost Based Optimizer (CBO)
  and collecting statistics during writes and when analyzing selected tables.
* Data modification, which is:

  * inserting, updating, and deleting rows in tables,
  * refreshing materialized views,
  * truncating whole tables,
  * and creating tables from query results.

* Role and grant management.
* Pushing down:

  * :ref:`Limit and Top N - limit with sort items <connector-limit-pushdown>`
  * Predicates
  * Projections
  * Sampling
  * Aggregations
  * Joins
  * Table function invocation

Note that data modification also requires implementing
a :ref:`connector-page-sink-provider`.

When Trino receives a ``SELECT`` query, it parses it into an Intermediate
Representation (IR). Then, during optimization, it checks if connectors
can handle operations related to SQL clauses by calling one of the following
methods of the ``ConnectorMetadata`` service:

* ``applyLimit``
* ``applyTopN``
* ``applyFilter``
* ``applyProjection``
* ``applySample``
* ``applyAggregation``
* ``applyJoin``
* ``applyTableFunction``
* ``applyTableScanRedirect``

Connectors can indicate that they don't support a particular pushdown or that
the action had no effect by returning ``Optional.empty()``. Connectors should
expect these methods to be called multiple times during the optimization of
a given query.

.. warning::

  It's critical for connectors to return ``Optional.empty()`` if calling
  this method has no effect for that invocation, even if the connector generally
  supports a particular pushdown. Doing otherwise can cause the optimizer
  to loop indefinitely.

Otherwise, these methods return a result object containing a new table handle.

The new table handle represents the virtual table derived from applying the
operation (filter, project, limit, etc.) to the table produced by the table
scan node.

The returned table handle is later passed to other services that the connector
implements, like the ``ConnectorRecordSetProvider`` or
``ConnectorPageSourceProvider``.

.. _connector-limit-pushdown:

Limit and top-N pushdown
^^^^^^^^^^^^^^^^^^^^^^^^

When executing a ``SELECT`` query with ``LIMIT`` or ``ORDER BY`` clauses,
the query plan may contain a ``Sort`` or ``Limit`` operations.

When the plan contains a ``Sort`` and ``Limit`` operations, the engine
tries to push down the limit into the connector by calling the ``applyTopN``
method of the connector metadata service. If there's no ``Sort`` operation, but
only a ``Limit``, the ``applyLimit`` method is called, and the connector can
return results in an arbitrary order.

If the connector could benefit from the information passed to these methods but
can't guarantee that it's be able to produce fewer rows than the provided
limit, it should return a non-empty result containing a new handle for the
derived table and the ``limitGuaranteed`` (in ``LimitApplicationResult``) or
``topNGuaranteed`` (in ``TopNApplicationResult``) flag set to false.

If the connector can guarantee to produce fewer rows than the provided
limit, it should return a non-empty result with the "limit guaranteed" or
"topN guaranteed" flag set to true.

.. note::

  The ``applyTopN`` is the only method that receives sort items from the
  ``Sort`` operation.

In an SQL query, the ``ORDER BY`` section can include any column with any order.
But the data source for the connector might only support limited combinations.
Plugin authors have to decide if the connector should ignore the pushdown,
return all the data and let the engine sort it, or throw an exception
to inform the user that particular order isn't supported, if fetching all
the data would be too expensive or time consuming. When throwing
an exception, use the ``TrinoException`` class with the ``INVALID_ORDER_BY``
error code and an actionable message, to let users know how to write a valid
query.

.. _connector-split-manager:

ConnectorSplitManager
---------------------

The split manager partitions the data for a table into the individual chunks
that Trino distributes to workers for processing. For example, the Hive
connector lists the files for each Hive partition and creates one or more
splits per file. For data sources that don't have partitioned data, a good
strategy here is to simply return a single split for the entire table. This is
the strategy employed by the Example HTTP connector.

.. _connector-record-set-provider:

ConnectorRecordSetProvider
--------------------------

Given a split, a table handle, and a list of columns, the record set provider
is responsible for delivering data to the Trino execution engine.

The table and column handles represent a virtual table. They're created by the
connector's metadata service, called by Trino during query planning and
optimization. Such a virtual table doesn't have to map directly to a single
collection in the connector's data source. If the connector supports pushdowns,
there can be multiple virtual tables derived from others, presenting a different
view of the underlying data.

The provider creates a ``RecordSet``, which in turn creates a ``RecordCursor``
that's used by Trino to read the column values for each row.

The provided record set must only include requested columns in the order
matching the list of column handles passed to the
``ConnectorRecordSetProvider.getRecordSet()`` method. The record set must return
all the rows contained in the "virtual table" represented by the TableHandle
associated with the TableScan operation.

For simple connectors, where performance isn't critical, the record set
provider can return an instance of ``InMemoryRecordSet``. The in-memory record
set can be built using lists of values for every row, which can be simpler than
implementing a ``RecordCursor``.

A ``RecordCursor`` implementation needs to keep track of the current record.
It return values for columns by a numerical position, in the data type matching
the column definition in the table. When the engine is done reading the current
record it calls ``advanceNextPosition`` on the cursor.

Type mapping
^^^^^^^^^^^^

The built-in SQL data types use different Java types as carrier types.

.. list-table:: SQL type to carrier type mapping
  :widths: 45, 55
  :header-rows: 1

  * - SQL type
    - Java type
  * - ``BOOLEAN``
    - ``boolean``
  * - ``TINYINT``
    - ``long``
  * - ``SMALLINT``
    - ``long``
  * - ``INTEGER``
    - ``long``
  * - ``BIGINT``
    - ``long``
  * - ``REAL``
    - ``double``
  * - ``DOUBLE``
    - ``double``
  * - ``DECIMAL``
    - ``long`` for precision up to 19, inclusive;
      ``Int128`` for precision greater than 19
  * - ``VARCHAR``
    - ``Slice``
  * - ``CHAR``
    - ``Slice``
  * - ``VARBINARY``
    - ``Slice``
  * - ``JSON``
    - ``Slice``
  * - ``DATE``
    - ``long``
  * - ``TIME(P)``
    - ``long``
  * - ``TIME WITH TIME ZONE``
    - ``long`` for precision up to 9;
      ``LongTimeWithTimeZone`` for precision greater than 9
  * - ``TIMESTAMP(P)``
    - ``long`` for precision up to 6;
      ``LongTimestamp`` for precision greater than 6
  * - ``TIMESTAMP(P) WITH TIME ZONE``
    - ``long`` for precision up to 3;
      ``LongTimestampWithTimeZone`` for precision greater than 3
  * - ``INTERVAL YEAR TO MONTH``
    - ``long``
  * - ``INTERVAL DAY TO SECOND``
    - ``long``
  * - ``ARRAY``
    - ``Block``
  * - ``MAP``
    - ``Block``
  * - ``ROW``
    - ``Block``
  * - ``IPADDRESS``
    - ``Slice``
  * - ``UUID``
    - ``Slice``
  * - ``HyperLogLog``
    - ``Slice``
  * - ``P4HyperLogLog``
    - ``Slice``
  * - ``SetDigest``
    - ``Slice``
  * - ``QDigest``
    - ``Slice``
  * - ``TDigest``
    - ``TDigest``

The ``RecordCursor.getType(int field)`` method returns the SQL type for a field
and the field value is returned by one of the following methods, matching
the carrier type:

* ``getBoolean(int field)``
* ``getLong(int field)``
* ``getDouble(int field)``
* ``getSlice(int field)``
* ``getObject(int field)``

Values for the ``timestamp(p) with time zone`` and ``time(p) with time zone``
types of regular precision can be converted into ``long`` using static methods
from the ``io.trino.spi.type.DateTimeEncoding`` class, like ``pack()`` or
``packDateTimeWithZone()``.

UTF-8 encoded strings can be converted to Slices using
the ``Slices.utf8Slice()`` static method.

.. note::

  The ``Slice`` class is provided by the ``io.airlift:slice`` package.

``Int128`` objects can be created using the ``Int128.valueOf()`` method.

The following example creates a block for an ``array(varchar)``  column:

.. code-block:: java

    private Block encodeArray(List<String> names)
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, names.size());
        for (String name : names) {
            if (name == null) {
                builder.appendNull();
            }
            else {
                VARCHAR.writeString(builder, name);
            }
        }
        return builder.build();
    }

The following example creates a block for a ``map(varchar, varchar)`` column:

.. code-block:: java

    private Block encodeMap(Map<String, ?> map)
    {
        MapType mapType = typeManager.getType(TypeSignature.mapType(
                                VARCHAR.getTypeSignature(),
                                VARCHAR.getTypeSignature()));
        BlockBuilder values = mapType.createBlockBuilder(null, map != null ? map.size() : 0);
        if (map == null) {
            values.appendNull();
            return values.build().getObject(0, Block.class);
        }
        BlockBuilder builder = values.beginBlockEntry();
        for (Map.Entry<String, ?> entry : map.entrySet()) {
            VARCHAR.writeString(builder, entry.getKey());
            Object value = entry.getValue();
            if (value == null) {
                builder.appendNull();
            }
            else {
                VARCHAR.writeString(builder, value.toString());
            }
        }
        values.closeEntry();
        return values.build().getObject(0, Block.class);
    }

.. _connector-page-source-provider:

ConnectorPageSourceProvider
---------------------------

Given a split, a table handle, and a list of columns, the page source provider
is responsible for delivering data to the Trino execution engine. It creates
a ``ConnectorPageSource``, which in turn creates ``Page`` objects that are used
by Trino to read the column values.

If not implemented, a default ``RecordPageSourceProvider`` is used.
Given a record set provider, it returns an instance of ``RecordPageSource``
that builds ``Page`` objects from records in a record set.

A connector should implement a page source provider instead of a record set
provider when it's possible to create pages directly. The conversion of
individual records from a record set provider into pages adds overheads during
query execution.

To add support for updating and/or deleting rows in a connector, it needs
to implement a ``ConnectorPageSourceProvider`` that returns
an ``UpdatablePageSource``. See :doc:`delete-and-update` for more.

.. _connector-page-sink-provider:

ConnectorPageSinkProvider
-------------------------

Given an insert table handle, the page sink provider is responsible for
consuming data from the Trino execution engine.
It creates a ``ConnectorPageSink``, which in turn accepts ``Page`` objects
that contains the column values.

Example that shows how to iterate over the page to access single values:

.. code-block:: java

  @Override
  public CompletableFuture<?> appendPage(Page page)
  {
      for (int channel = 0; channel < page.getChannelCount(); channel++) {
          Block block = page.getBlock(channel);
          for (int position = 0; position < page.getPositionCount(); position++) {
              if (block.isNull(position)) {
                  // or handle this differently
                  continue;
              }

              // channel should match the column number in the table
              // use it to determine the expected column type
              String value = VARCHAR.getSlice(block, position).toStringUtf8();
              // TODO do something with the value
          }
      }
      return NOT_BLOCKED;
  }
