/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.plugin.phoenix;

import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ColumnMapping;
import io.prestosql.plugin.jdbc.DriverConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcOutputTableHandle;
import io.prestosql.plugin.jdbc.JdbcSplit;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.plugin.jdbc.WriteMapping;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.Type;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.iterate.ConcatResultIterator;
import org.apache.phoenix.iterate.LookAheadResultIterator;
import org.apache.phoenix.iterate.MapReduceParallelScanGrouper;
import org.apache.phoenix.iterate.PeekingResultIterator;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.iterate.RoundRobinResultIterator;
import org.apache.phoenix.iterate.SequenceResultIterator;
import org.apache.phoenix.iterate.TableResultIterator;
import org.apache.phoenix.jdbc.DelegatePreparedStatement;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver.ConnectionInfo;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.mapreduce.PhoenixInputSplit;
import org.apache.phoenix.monitoring.ScanMetricsHolder;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.HBaseFactoryProvider;

import javax.inject.Inject;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.timeWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varcharColumnMapping;
import static io.prestosql.plugin.phoenix.MetadataUtil.toPhoenixSchemaName;
import static io.prestosql.plugin.phoenix.PhoenixErrorCode.PHOENIX_QUERY_ERROR;
import static io.prestosql.plugin.phoenix.PhoenixMetadata.DEFAULT_SCHEMA;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.sql.Types.LONGNVARCHAR;
import static java.sql.Types.LONGVARCHAR;
import static java.sql.Types.NVARCHAR;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TIMESTAMP_WITH_TIMEZONE;
import static java.sql.Types.TIME_WITH_TIMEZONE;
import static java.sql.Types.VARCHAR;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.SKIP_REGION_BOUNDARY_CHECK;
import static org.apache.phoenix.query.QueryConstants.NULL_SCHEMA_NAME;
import static org.apache.phoenix.util.SchemaUtil.ESCAPE_CHARACTER;

public class PhoenixClient
        extends BaseJdbcClient
{
    private final Configuration configuration;

    @Inject
    public PhoenixClient(PhoenixConfig config)
            throws SQLException
    {
        super(
                new BaseJdbcConfig(),
                ESCAPE_CHARACTER,
                new DriverConnectionFactory(
                        DriverManager.getDriver(config.getConnectionUrl()),
                        config.getConnectionUrl(),
                        Optional.empty(),
                        Optional.empty(),
                        config.getConnectionProperties()));
        requireNonNull(config, "config is null");
        configuration = new Configuration(false);
        ConnectionInfo connectionInfo = PhoenixEmbeddedDriver.ConnectionInfo.create(config.getConnectionUrl());
        connectionInfo.asProps()
                .forEach(prop -> configuration.set(prop.getKey(), prop.getValue()));
        config.getConnectionProperties()
                .forEach((k, v) -> configuration.set((String) k, (String) v));
    }

    public PhoenixConnection getConnection(JdbcIdentity identity)
            throws SQLException
    {
        Connection connection = connectionFactory.openConnection(identity);
        try {
            return connection.unwrap(PhoenixConnection.class);
        }
        catch (Exception e) {
            connection.close();
            throw new PrestoException(PHOENIX_QUERY_ERROR, "Couldn't open Phoenix connection", e);
        }
    }

    public org.apache.hadoop.hbase.client.Connection getHConnection()
            throws IOException
    {
        return HBaseFactoryProvider.getHConnectionFactory().createConnection(configuration);
    }

    public void execute(ConnectorSession session, String statement)
    {
        try (Connection connection = connectionFactory.openConnection(JdbcIdentity.from(session))) {
            execute(connection, statement);
        }
        catch (SQLException e) {
            throw new PrestoException(PHOENIX_QUERY_ERROR, "Error while executing statement", e);
        }
    }

    @Override
    protected Collection<String> listSchemas(Connection connection)
    {
        try (ResultSet resultSet = connection.getMetaData().getSchemas()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            schemaNames.add(DEFAULT_SCHEMA);
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_SCHEM");
                // skip internal schemas
                if (schemaName != null && !schemaName.equalsIgnoreCase("information_schema")) {
                    schemaNames.add(schemaName);
                }
            }
            return schemaNames.build();
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public PreparedStatement buildSql(ConnectorSession session, Connection connection, JdbcSplit split, JdbcTableHandle table, List<JdbcColumnHandle> columnHandles)
            throws SQLException
    {
        PreparedStatement query = super.buildSql(session, connection, split, table, columnHandles);
        QueryPlan queryPlan = getQueryPlan((PhoenixPreparedStatement) query);
        ResultSet resultSet = getResultSet(((PhoenixSplit) split).getPhoenixInputSplit(), queryPlan);
        return new DelegatePreparedStatement(query)
        {
            @Override
            public ResultSet executeQuery()
                    throws SQLException
            {
                return resultSet;
            }
        };
    }

    @Override
    public String buildInsertSql(JdbcOutputTableHandle handle)
    {
        PhoenixOutputTableHandle outputHandle = (PhoenixOutputTableHandle) handle;
        String params = join(",", nCopies(handle.getColumnNames().size(), "?"));
        if (outputHandle.hasUUIDRowkey()) {
            String nextId = format(
                    "NEXT VALUE FOR %s, ",
                    quoted(null, handle.getSchemaName(), handle.getTableName() + "_sequence"));
            params = nextId + params;
        }
        return format(
                "UPSERT INTO %s VALUES (%s)",
                quoted(null, handle.getSchemaName(), handle.getTableName()),
                params);
    }

    @Override
    protected ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        return super.getTables(connection, toPhoenixSchemaName(schemaName), tableName);
    }

    protected static ResultSet getColumns(JdbcTableHandle tableHandle, DatabaseMetaData metadata)
            throws SQLException
    {
        Optional<String> escape = Optional.ofNullable(metadata.getSearchStringEscape());
        return metadata.getColumns(
                tableHandle.getCatalogName(),
                // passing null for schema returns tables in all schemas, so we need to specify the default empty schema name
                escapeNamePattern(Optional.ofNullable(tableHandle.getSchemaName()), escape).orElse(NULL_SCHEMA_NAME),
                escapeNamePattern(Optional.ofNullable(tableHandle.getTableName()), escape).orElse(null),
                null);
    }

    @Override
    public Optional<ColumnMapping> toPrestoType(ConnectorSession session, JdbcTypeHandle typeHandle)
    {
        switch (typeHandle.getJdbcType()) {
            case VARCHAR:
            case NVARCHAR:
            case LONGVARCHAR:
            case LONGNVARCHAR:
                if (typeHandle.getColumnSize() == 0) {
                    return Optional.of(varcharColumnMapping(createUnboundedVarcharType()));
                }
                return super.toPrestoType(session, typeHandle);
            // TODO add support for TIMESTAMP after Phoenix adds support for LocalDateTime
            case TIMESTAMP:
            case TIME_WITH_TIMEZONE:
            case TIMESTAMP_WITH_TIMEZONE:
                return Optional.empty();
        }
        return super.toPrestoType(session, typeHandle);
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (DOUBLE.equals(type)) {
            return WriteMapping.doubleMapping("double", doubleWriteFunction());
        }
        if (REAL.equals(type)) {
            return WriteMapping.longMapping("float", realWriteFunction());
        }
        if (TIME.equals(type)) {
            return WriteMapping.longMapping("time", timeWriteFunction());
        }
        // Phoenix doesn't support _WITH_TIME_ZONE
        if (TIME_WITH_TIME_ZONE.equals(type) || TIMESTAMP_WITH_TIME_ZONE.equals(type)) {
            throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
        }
        return super.toWriteMapping(session, type);
    }

    public List<JdbcColumnHandle> getNonRowkeyColumns(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        return getColumns(session, tableHandle).stream().filter(handle -> !PhoenixMetadata.ROWKEY.equalsIgnoreCase(handle.getColumnName())).collect(Collectors.toList());
    }

    public Optional<String> getCatalogName()
    {
        // catalogName in Phoenix is used for tenantId, but
        // tenant-specific connections not currently supported)
        return Optional.of("");
    }

    public QueryPlan getQueryPlan(PhoenixPreparedStatement inputQuery)
    {
        try {
            // Optimize the query plan so that we potentially use secondary indexes
            QueryPlan queryPlan = inputQuery.optimizeQuery();
            // Initialize the query plan so it sets up the parallel scans
            queryPlan.iterator(MapReduceParallelScanGrouper.getInstance());
            return queryPlan;
        }
        catch (SQLException e) {
            throw new PrestoException(PHOENIX_QUERY_ERROR, "Failed to get the Phoenix query plan", e);
        }
    }

    private ResultSet getResultSet(InputSplit split, QueryPlan queryPlan)
    {
        List<Scan> scans = ((PhoenixInputSplit) split).getScans();
        try {
            List<PeekingResultIterator> iterators = new ArrayList<>(scans.size());
            StatementContext context = queryPlan.getContext();
            // Clear the table region boundary cache to make sure long running jobs stay up to date
            byte[] tableNameBytes = queryPlan.getTableRef().getTable().getPhysicalName().getBytes();
            ConnectionQueryServices services = context.getConnection().getQueryServices();
            services.clearTableRegionCache(tableNameBytes);

            long renewScannerLeaseThreshold = context.getConnection().getQueryServices().getRenewLeaseThresholdMilliSeconds();
            for (Scan scan : scans) {
                // For MR, skip the region boundary check exception if we encounter a split. ref: PHOENIX-2599
                scan.setAttribute(SKIP_REGION_BOUNDARY_CHECK, Bytes.toBytes(true));

                PeekingResultIterator peekingResultIterator;
                ScanMetricsHolder scanMetricsHolder =
                        ScanMetricsHolder.getInstance(
                                context.getReadMetricsQueue(),
                                queryPlan.getTableRef().getTable().getPhysicalName().getString(), scan,
                                context.getConnection().getLogLevel());

                TableResultIterator tableResultIterator =
                        new TableResultIterator(
                                context.getConnection().getMutationState(), scan,
                                scanMetricsHolder, renewScannerLeaseThreshold, queryPlan,
                                MapReduceParallelScanGrouper.getInstance());
                peekingResultIterator = LookAheadResultIterator.wrap(tableResultIterator);
                iterators.add(peekingResultIterator);
            }
            ResultIterator iterator = queryPlan.useRoundRobinIterator() ? RoundRobinResultIterator.newIterator(iterators, queryPlan) : ConcatResultIterator.newIterator(iterators);
            if (context.getSequenceManager().getSequenceCount() > 0) {
                iterator = new SequenceResultIterator(iterator, context.getSequenceManager());
            }
            // Clone the row projector as it's not thread safe and would be used simultaneously by
            // multiple threads otherwise.
            return new PhoenixResultSet(iterator, queryPlan.getProjector().cloneIfNecessary(), context);
        }
        catch (SQLException e) {
            throw new PrestoException(PHOENIX_QUERY_ERROR, "Error while setting up Phoenix ResultSet", e);
        }
    }
}
