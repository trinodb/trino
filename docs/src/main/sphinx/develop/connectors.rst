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

.. _connector-metadata:

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



ConnectorMetadata
^^^^^^^^^^^^^^^^^

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

  * Limit
  * Predicates
  * Projections
  * Sampling
  * Aggregations
  * Joins
  * Top N - limit with sort items
  * Table function invocation

Note that data modification also requires implementing
a :ref:`connector-page-sink-provider`.

.. _connector-split-manager:

ConnectorSplitManager
^^^^^^^^^^^^^^^^^^^^^

The split manager partitions the data for a table into the individual chunks
that Trino distributes to workers for processing. For example, the Hive
connector lists the files for each Hive partition and creates one or more
splits per file. For data sources that don't have partitioned data, a good
strategy here is to simply return a single split for the entire table. This is
the strategy employed by the Example HTTP connector.

.. _connector-record-set-provider:

ConnectorRecordSetProvider
^^^^^^^^^^^^^^^^^^^^^^^^^^

Given a split and a list of columns, the record set provider is
responsible for delivering data to the Trino execution engine.
It creates a ``RecordSet``, which in turn creates a ``RecordCursor``
that's used by Trino to read the column values for each row.

.. _connector-page-source-provider:

ConnectorPageSourceProvider
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Given a split and a list of columns, the page source provider is
responsible for delivering data to the Trino execution engine.
It creates a ``ConnectorPageSource``, which in turn creates ``Page`` objects
that are used by Trino to read the column values.

If not implemented, a default ``RecordPageSourceProvider`` is used.
Given a record set provider, it returns an instance of ``RecordPageSource``
that builds ``Page`` objects from records in a record set.

A connector should implement a page source provider instead of a record set
provider when it's possible to create pages directly. The conversion of individual
records from a record set provider into pages adds overheads during query execution.

To add support for updating and/or deleting rows in a connector, it needs
to implement a ``ConnectorPageSourceProvider`` that returns
an ``UpdatablePageSource``. See :doc:`delete-and-update` for more.

.. _connector-page-sink-provider:

ConnectorPageSinkProvider
^^^^^^^^^^^^^^^^^^^^^^^^^

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

