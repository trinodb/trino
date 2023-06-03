# Connectors

Connectors are the source of all data for queries in Trino. Even if your data
source doesn't have underlying tables backing it, as long as you adapt your data
source to the API expected by Trino, you can write queries against this data.

## ConnectorFactory

Instances of your connector are created by a `ConnectorFactory` instance which
is created when Trino calls `getConnectorFactory()` on the plugin. The
connector factory is a simple interface responsible for providing the connector
name and creating an instance of a `Connector` object. A basic connector
implementation that only supports reading, but not writing data, should return
instances of the following services:

- {ref}`connector-metadata`
- {ref}`connector-split-manager`
- {ref}`connector-record-set-provider` or {ref}`connector-page-source-provider`

### Configuration

The `create()` method of the connector factory receives a `config` map,
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

```java
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
```

The preceding example defines two configuration properties and makes
the connector more robust by:

- defining all supported properties, which allows detecting spelling mistakes
  in the configuration on server startup
- defining a default timeout value, to prevent connections getting stuck
  indefinitely
- preventing invalid timeout values, like 0 ms, that would make
  all requests fail
- parsing timeout values in different units, detecting invalid values
- preventing logging the secret value in plain text

The configuration class needs to be bound in a Guice module:

```java
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
```

And then the module needs to be initialized in the connector factory, when
creating a new instance of the connector:

```java
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
```

:::{note}
Environment variables in the catalog properties file
(ex. `secret=${ENV:SECRET}`) are resolved only when using
the `io.airlift.bootstrap.Bootstrap` class to initialize the module.
See {doc}`/security/secrets` for more information.
:::

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

```sql
SELECT *
FROM git.default.commits
WHERE url = 'https://github.com/trinodb/trino.git'
```

(connector-metadata)=

## ConnectorMetadata

The connector metadata interface allows Trino to get a lists of schemas,
tables, columns, and other metadata about a particular data source.

A basic read-only connector should implement the following methods:

- `listSchemaNames`
- `listTables`
- `streamTableColumns`
- `getTableHandle`
- `getTableMetadata`
- `getColumnHandles`
- `getColumnMetadata`

If you are interested in seeing strategies for implementing more methods,
look at the {doc}`example-http` and the Cassandra connector. If your underlying
data source supports schemas, tables, and columns, this interface should be
straightforward to implement. If you are attempting to adapt something that
isn't a relational database, as the Example HTTP connector does, you may
need to get creative about how you map your data source to Trino's schema,
table, and column concepts.

The connector metadata interface allows to also implement other connector
features, like:

- Schema management, which is creating, altering and dropping schemas, tables,
  table columns, views, and materialized views.

- Support for table and column comments, and properties.

- Schema, table and view authorization.

- Executing {doc}`table-functions`.

- Providing table statistics used by the Cost Based Optimizer (CBO)
  and collecting statistics during writes and when analyzing selected tables.

- Data modification, which is:

  - inserting, updating, and deleting rows in tables,
  - refreshing materialized views,
  - truncating whole tables,
  - and creating tables from query results.

- Role and grant management.

- Pushing down:

  - {ref}`Limit and Top N - limit with sort items <connector-limit-pushdown>`
  - {ref}`Predicates <dev-predicate-pushdown>`
  - Projections
  - Sampling
  - Aggregations
  - Joins
  - Table function invocation

Note that data modification also requires implementing
a {ref}`connector-page-sink-provider`.

When Trino receives a `SELECT` query, it parses it into an Intermediate
Representation (IR). Then, during optimization, it checks if connectors
can handle operations related to SQL clauses by calling one of the following
methods of the `ConnectorMetadata` service:

- `applyLimit`
- `applyTopN`
- `applyFilter`
- `applyProjection`
- `applySample`
- `applyAggregation`
- `applyJoin`
- `applyTableFunction`
- `applyTableScanRedirect`

Connectors can indicate that they don't support a particular pushdown or that
the action had no effect by returning `Optional.empty()`. Connectors should
expect these methods to be called multiple times during the optimization of
a given query.

:::{warning}
It's critical for connectors to return `Optional.empty()` if calling
this method has no effect for that invocation, even if the connector generally
supports a particular pushdown. Doing otherwise can cause the optimizer
to loop indefinitely.
:::

Otherwise, these methods return a result object containing a new table handle.
The new table handle represents the virtual table derived from applying the
operation (filter, project, limit, etc.) to the table produced by the table
scan node. Once the query actually runs, `ConnectorRecordSetProvider` or
`ConnectorPageSourceProvider` can use whatever optimizations were pushed down to
`ConnectorTableHandle`.

The returned table handle is later passed to other services that the connector
implements, like the `ConnectorRecordSetProvider` or
`ConnectorPageSourceProvider`.

(connector-limit-pushdown)=

### Limit and top-N pushdown

When executing a `SELECT` query with `LIMIT` or `ORDER BY` clauses,
the query plan may contain a `Sort` or `Limit` operations.

When the plan contains a `Sort` and `Limit` operations, the engine
tries to push down the limit into the connector by calling the `applyTopN`
method of the connector metadata service. If there's no `Sort` operation, but
only a `Limit`, the `applyLimit` method is called, and the connector can
return results in an arbitrary order.

If the connector could benefit from the information passed to these methods but
can't guarantee that it's be able to produce fewer rows than the provided
limit, it should return a non-empty result containing a new handle for the
derived table and the `limitGuaranteed` (in `LimitApplicationResult`) or
`topNGuaranteed` (in `TopNApplicationResult`) flag set to false.

If the connector can guarantee to produce fewer rows than the provided
limit, it should return a non-empty result with the "limit guaranteed" or
"topN guaranteed" flag set to true.

:::{note}
The `applyTopN` is the only method that receives sort items from the
`Sort` operation.
:::

In a query, the `ORDER BY` section can include any column with any order.
But the data source for the connector might only support limited combinations.
Plugin authors have to decide if the connector should ignore the pushdown,
return all the data and let the engine sort it, or throw an exception
to inform the user that particular order isn't supported, if fetching all
the data would be too expensive or time consuming. When throwing
an exception, use the `TrinoException` class with the `INVALID_ORDER_BY`
error code and an actionable message, to let users know how to write a valid
query.

(dev-predicate-pushdown)=

### Predicate pushdown

When executing a query with a `WHERE` clause, the query plan can
contain a `ScanFilterProject` plan node/node with a predicate constraint.

A predicate constraint is a description of the constraint imposed on the
results of the stage/fragment as expressed in the `WHERE` clause. For example,
`WHERE x > 5 AND y = 3` translates into a constraint where the
`summary` field means the `x` column's domain must be greater than
`5` and the `y` column domain equals `3`.

When the query plan contains a `ScanFilterProject` operation, Trino
tries to optimize the query by pushing down the predicate constraint
into the connector by calling the `applyFilter` method of the
connector metadata service. This method receives a table handle with
all optimizations applied thus far, and returns either
`Optional.empty()` or a response with a new table handle derived from
the old one.

The query optimizer may call `applyFilter` for a single query multiple times,
as it searches for an optimal query plan. Connectors must
return `Optional.empty()` from `applyFilter` if they cannot apply the
constraint for this invocation, even if they support `ScanFilterProject`
pushdown in general. Connectors must also return `Optional.empty()` if the
constraint has already been applied.

A constraint contains the following elements:

- A `TupleDomain` defining the mapping between columns and their domains.
  A `Domain` is either a list of possible values, or a list of ranges, and
  also contains information about nullability.
- Expression for pushing down function calls.
- Map of assignments from variables in the expression to columns.
- (optional) Predicate which tests a map of columns and their values;
  it cannot be held on to after the `applyFilter` call returns.
- (optional) Set of columns the predicate depends on; must be present
  if predicate is present.

If both a predicate and a summary are available, the predicate is guaranteed to
be more strict in filtering of values, and can provide a significant boost to
query performance if used.

However it is not possible to store a predicate in the table handle and use
it later, as the predicate cannot be held on to after the `applyFilter`
call returns. It is used for filtering of entire partitions, and is not pushed
down. The summary can be pushed down instead by storing it in the table handle.

This overlap between the predicate and summary is due to historical reasons,
as simple comparison pushdown was implemented first via summary, and more
complex filters such as `LIKE` which required more expressive predicates
were added later.

If a constraint can only be partially pushed down, for example when a connector
for a database that does not support range matching is used in a query with
`WHERE x = 2 AND y > 5`, the `y` column constraint must be
returned in the `ConstraintApplicationResult` from `applyFilter`.
In this case the `y > 5` condition is applied in Trino,
and not pushed down.

The following is a simple example which only looks at `TupleDomain`:

```java
@Override
public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
        ConnectorSession session,
        ConnectorTableHandle tableHandle,
        Constraint constraint)
{
    ExampleTableHandle handle = (ExampleTableHandle) tableHandle;

    TupleDomain<ColumnHandle> oldDomain = handle.getConstraint();
    TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(constraint.getSummary());
    if (oldDomain.equals(newDomain)) {
        // Nothing has changed, return empty Option
        return Optional.empty();
    }

    handle = new ExampleTableHandle(newDomain);
    return Optional.of(new ConstraintApplicationResult<>(handle, TupleDomain.all(), false));
}
```

The `TupleDomain` from the constraint is intersected with the `TupleDomain`
already applied to the `TableHandle` to form `newDomain`.
If filtering has not changed, an `Optional.empty()` result is returned to
notify the planner that this optimization path has reached its end.

In this example, the connector pushes down the `TupleDomain`
with all Trino data types supported with same semantics in the
data source. As a result, no filters are needed in Trino,
and the `ConstraintApplicationResult` sets `remainingFilter` to
`TupleDomain.all()`.

This pushdown implementation is quite similar to many Trino connectors,
including  `MongoMetadata`, `BigQueryMetadata`, `KafkaMetadata`.

The following, more complex example shows data types from Trino that are
not available directly in the underlying data source, and must be mapped:

```java
@Override
public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
        ConnectorSession session,
        ConnectorTableHandle table,
        Constraint constraint)
{
    JdbcTableHandle handle = (JdbcTableHandle) table;

    TupleDomain<ColumnHandle> oldDomain = handle.getConstraint();
    TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(constraint.getSummary());
    TupleDomain<ColumnHandle> remainingFilter;
    if (newDomain.isNone()) {
        newConstraintExpressions = ImmutableList.of();
        remainingFilter = TupleDomain.all();
        remainingExpression = Optional.of(Constant.TRUE);
    }
    else {
        // We need to decide which columns to push down.
        // Since this is a base class for many JDBC-based connectors, each
        // having different Trino type mappings and comparison semantics
        // it needs to be flexible.

        Map<ColumnHandle, Domain> domains = newDomain.getDomains().orElseThrow();
        List<JdbcColumnHandle> columnHandles = domains.keySet().stream()
                .map(JdbcColumnHandle.class::cast)
                .collect(toImmutableList());

        // Get information about how to push down every column based on its
        // JDBC data type
        List<ColumnMapping> columnMappings = jdbcClient.toColumnMappings(
                session,
                columnHandles.stream()
                        .map(JdbcColumnHandle::getJdbcTypeHandle)
                        .collect(toImmutableList()));

        // Calculate the domains which can be safely pushed down (supported)
        // and those which need to be filtered in Trino (unsupported)
        Map<ColumnHandle, Domain> supported = new HashMap<>();
        Map<ColumnHandle, Domain> unsupported = new HashMap<>();
        for (int i = 0; i < columnHandles.size(); i++) {
            JdbcColumnHandle column = columnHandles.get(i);
            DomainPushdownResult pushdownResult =
                columnMappings.get(i).getPredicatePushdownController().apply(
                    session,
                    domains.get(column));
            supported.put(column, pushdownResult.getPushedDown());
            unsupported.put(column, pushdownResult.getRemainingFilter());
        }

        newDomain = TupleDomain.withColumnDomains(supported);
        remainingFilter = TupleDomain.withColumnDomains(unsupported);
    }

    // Return empty Optional if nothing changed in filtering
    if (oldDomain.equals(newDomain)) {
        return Optional.empty();
    }

    handle = new JdbcTableHandle(
            handle.getRelationHandle(),
            newDomain,
            ...);

    return Optional.of(
            new ConstraintApplicationResult<>(
                handle,
                remainingFilter));
}
```

This example illustrates implementing a base class for many JDBC connectors
while handling the specific requirements of multiple JDBC-compliant data sources.
It ensures that if a constraint gets pushed down, it works exactly the same in
the underlying data source, and produces the same results as it would in Trino.
For example, in databases where string comparisons are case-insensitive,
pushdown does not work, as string comparison operations in Trino are
case-sensitive.

The `PredicatePushdownController` interface determines if a column domain can
be pushed down in JDBC-compliant data sources. In the preceding example, it is
called from a `JdbcClient` implementation specific to that database.
In non-JDBC-compliant data sources, type-based push downs are implemented
directly, without going through the `PredicatePushdownController` interface.

The following example adds expression pushdown enabled by a session flag:

```java
@Override
public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
        ConnectorSession session,
        ConnectorTableHandle table,
        Constraint constraint)
{
    JdbcTableHandle handle = (JdbcTableHandle) table;

    TupleDomain<ColumnHandle> oldDomain = handle.getConstraint();
    TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(constraint.getSummary());
    List<String> newConstraintExpressions;
    TupleDomain<ColumnHandle> remainingFilter;
    Optional<ConnectorExpression> remainingExpression;
    if (newDomain.isNone()) {
        newConstraintExpressions = ImmutableList.of();
        remainingFilter = TupleDomain.all();
        remainingExpression = Optional.of(Constant.TRUE);
    }
    else {
        // We need to decide which columns to push down.
        // Since this is a base class for many JDBC-based connectors, each
        // having different Trino type mappings and comparison semantics
        // it needs to be flexible.

        Map<ColumnHandle, Domain> domains = newDomain.getDomains().orElseThrow();
        List<JdbcColumnHandle> columnHandles = domains.keySet().stream()
                .map(JdbcColumnHandle.class::cast)
                .collect(toImmutableList());

        // Get information about how to push down every column based on its
        // JDBC data type
        List<ColumnMapping> columnMappings = jdbcClient.toColumnMappings(
                session,
                columnHandles.stream()
                        .map(JdbcColumnHandle::getJdbcTypeHandle)
                        .collect(toImmutableList()));

        // Calculate the domains which can be safely pushed down (supported)
        // and those which need to be filtered in Trino (unsupported)
        Map<ColumnHandle, Domain> supported = new HashMap<>();
        Map<ColumnHandle, Domain> unsupported = new HashMap<>();
        for (int i = 0; i < columnHandles.size(); i++) {
            JdbcColumnHandle column = columnHandles.get(i);
            DomainPushdownResult pushdownResult =
                columnMappings.get(i).getPredicatePushdownController().apply(
                    session,
                    domains.get(column));
            supported.put(column, pushdownResult.getPushedDown());
            unsupported.put(column, pushdownResult.getRemainingFilter());
        }

        newDomain = TupleDomain.withColumnDomains(supported);
        remainingFilter = TupleDomain.withColumnDomains(unsupported);

        // Do we want to handle expression pushdown?
        if (isComplexExpressionPushdown(session)) {
            List<String> newExpressions = new ArrayList<>();
            List<ConnectorExpression> remainingExpressions = new ArrayList<>();
            // Each expression can be broken down into a list of conjuncts
            // joined with AND. We handle each conjunct separately.
            for (ConnectorExpression expression : extractConjuncts(constraint.getExpression())) {
                // Try to convert the conjunct into something which is
                // understood by the underlying JDBC data source
                Optional<String> converted = jdbcClient.convertPredicate(
                    session,
                    expression,
                    constraint.getAssignments());
                if (converted.isPresent()) {
                    newExpressions.add(converted.get());
                }
                else {
                    remainingExpressions.add(expression);
                }
            }
            // Calculate which parts of the expression can be pushed down
            // and which need to be calculated in Trino engine
            newConstraintExpressions = ImmutableSet.<String>builder()
                    .addAll(handle.getConstraintExpressions())
                    .addAll(newExpressions)
                    .build().asList();
            remainingExpression = Optional.of(and(remainingExpressions));
        }
        else {
            newConstraintExpressions = ImmutableList.of();
            remainingExpression = Optional.empty();
        }
    }

    // Return empty Optional if nothing changed in filtering
    if (oldDomain.equals(newDomain) &&
            handle.getConstraintExpressions().equals(newConstraintExpressions)) {
        return Optional.empty();
    }

    handle = new JdbcTableHandle(
            handle.getRelationHandle(),
            newDomain,
            newConstraintExpressions,
            ...);

    return Optional.of(
            remainingExpression.isPresent()
                    ? new ConstraintApplicationResult<>(
                        handle,
                        remainingFilter,
                        remainingExpression.get())
                    : new ConstraintApplicationResult<>(
                        handle,
                        remainingFilter));
}
```

`ConnectorExpression` is split similarly to `TupleDomain`.
Each expression can be broken down into independent *conjuncts*. Conjuncts are
smaller expressions which, if joined together using an `AND` operator, are
equivalent to the original expression. Every conjunct can be handled
individually. Each one is converted using connector-specific rules, as defined
by the `JdbcClient` implementation, to be more flexible. Unconverted
conjuncts are returned as `remainingExpression` and are evaluated by
the Trino engine.

(connector-split-manager)=

## ConnectorSplitManager

The split manager partitions the data for a table into the individual chunks
that Trino distributes to workers for processing. For example, the Hive
connector lists the files for each Hive partition and creates one or more
splits per file. For data sources that don't have partitioned data, a good
strategy here is to simply return a single split for the entire table. This is
the strategy employed by the Example HTTP connector.

(connector-record-set-provider)=

## ConnectorRecordSetProvider

Given a split, a table handle, and a list of columns, the record set provider
is responsible for delivering data to the Trino execution engine.

The table and column handles represent a virtual table. They're created by the
connector's metadata service, called by Trino during query planning and
optimization. Such a virtual table doesn't have to map directly to a single
collection in the connector's data source. If the connector supports pushdowns,
there can be multiple virtual tables derived from others, presenting a different
view of the underlying data.

The provider creates a `RecordSet`, which in turn creates a `RecordCursor`
that's used by Trino to read the column values for each row.

The provided record set must only include requested columns in the order
matching the list of column handles passed to the
`ConnectorRecordSetProvider.getRecordSet()` method. The record set must return
all the rows contained in the "virtual table" represented by the TableHandle
associated with the TableScan operation.

For simple connectors, where performance isn't critical, the record set
provider can return an instance of `InMemoryRecordSet`. The in-memory record
set can be built using lists of values for every row, which can be simpler than
implementing a `RecordCursor`.

A `RecordCursor` implementation needs to keep track of the current record.
It return values for columns by a numerical position, in the data type matching
the column definition in the table. When the engine is done reading the current
record it calls `advanceNextPosition` on the cursor.

### Type mapping

The built-in SQL data types use different Java types as carrier types.

```{eval-rst}
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
```

The `RecordCursor.getType(int field)` method returns the SQL type for a field
and the field value is returned by one of the following methods, matching
the carrier type:

- `getBoolean(int field)`
- `getLong(int field)`
- `getDouble(int field)`
- `getSlice(int field)`
- `getObject(int field)`

Values for the `timestamp(p) with time zone` and `time(p) with time zone`
types of regular precision can be converted into `long` using static methods
from the `io.trino.spi.type.DateTimeEncoding` class, like `pack()` or
`packDateTimeWithZone()`.

UTF-8 encoded strings can be converted to Slices using
the `Slices.utf8Slice()` static method.

:::{note}
The `Slice` class is provided by the `io.airlift:slice` package.
:::

`Int128` objects can be created using the `Int128.valueOf()` method.

The following example creates a block for an `array(varchar)`  column:

```java
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
```

The following example creates a block for a `map(varchar, varchar)` column:

```java
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
```

(connector-page-source-provider)=

## ConnectorPageSourceProvider

Given a split, a table handle, and a list of columns, the page source provider
is responsible for delivering data to the Trino execution engine. It creates
a `ConnectorPageSource`, which in turn creates `Page` objects that are used
by Trino to read the column values.

If not implemented, a default `RecordPageSourceProvider` is used.
Given a record set provider, it returns an instance of `RecordPageSource`
that builds `Page` objects from records in a record set.

A connector should implement a page source provider instead of a record set
provider when it's possible to create pages directly. The conversion of
individual records from a record set provider into pages adds overheads during
query execution.

(connector-page-sink-provider)=

## ConnectorPageSinkProvider

Given an insert table handle, the page sink provider is responsible for
consuming data from the Trino execution engine.
It creates a `ConnectorPageSink`, which in turn accepts `Page` objects
that contains the column values.

Example that shows how to iterate over the page to access single values:

```java
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
```
