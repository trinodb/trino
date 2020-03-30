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
import io.prestosql.plugin.jdbc.BlockReadFunction;
import io.prestosql.plugin.jdbc.BlockWriteFunction;
import io.prestosql.plugin.jdbc.ColumnMapping;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcOutputTableHandle;
import io.prestosql.plugin.jdbc.JdbcSplit;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.plugin.jdbc.QueryBuilder;
import io.prestosql.plugin.jdbc.WriteMapping;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.Type;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
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
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.mapreduce.PhoenixInputSplit;
import org.apache.phoenix.monitoring.ScanMetricsHolder;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.HBaseFactoryProvider;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.types.PDataType;

import javax.inject.Inject;

import java.io.IOException;
import java.sql.Array;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.realColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.timeWriteFunctionUsingSqlTime;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varcharColumnMapping;
import static io.prestosql.plugin.jdbc.TypeHandlingJdbcPropertiesProvider.getUnsupportedTypeHandling;
import static io.prestosql.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.prestosql.plugin.phoenix.MetadataUtil.toPhoenixSchemaName;
import static io.prestosql.plugin.phoenix.PhoenixClientModule.getConnectionProperties;
import static io.prestosql.plugin.phoenix.PhoenixErrorCode.PHOENIX_METADATA_ERROR;
import static io.prestosql.plugin.phoenix.PhoenixErrorCode.PHOENIX_QUERY_ERROR;
import static io.prestosql.plugin.phoenix.PhoenixMetadata.DEFAULT_SCHEMA;
import static io.prestosql.plugin.phoenix.TypeUtils.getArrayElementPhoenixTypeName;
import static io.prestosql.plugin.phoenix.TypeUtils.getJdbcObjectArray;
import static io.prestosql.plugin.phoenix.TypeUtils.jdbcObjectArrayToBlock;
import static io.prestosql.plugin.phoenix.TypeUtils.toBoxedArray;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.sql.Types.ARRAY;
import static java.sql.Types.FLOAT;
import static java.sql.Types.LONGNVARCHAR;
import static java.sql.Types.LONGVARCHAR;
import static java.sql.Types.NVARCHAR;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TIMESTAMP_WITH_TIMEZONE;
import static java.sql.Types.TIME_WITH_TIMEZONE;
import static java.sql.Types.VARCHAR;
import static java.util.Collections.nCopies;
import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.SKIP_REGION_BOUNDARY_CHECK;
import static org.apache.phoenix.util.SchemaUtil.ESCAPE_CHARACTER;

public class PhoenixClient
        extends BaseJdbcClient
{
    private final Configuration configuration;

    @Inject
    public PhoenixClient(PhoenixConfig config, ConnectionFactory connectionFactory)
            throws SQLException
    {
        super(
                ESCAPE_CHARACTER,
                connectionFactory,
                ImmutableSet.of(),
                config.isCaseInsensitiveNameMatching(),
                config.getCaseInsensitiveNameMatchingCacheTtl());
        this.configuration = new Configuration(false);
        getConnectionProperties(config).forEach((k, v) -> configuration.set((String) k, (String) v));
    }

    public PhoenixConnection getConnection(JdbcIdentity identity)
            throws SQLException
    {
        return connectionFactory.openConnection(identity).unwrap(PhoenixConnection.class);
    }

    public org.apache.hadoop.hbase.client.Connection getHConnection()
            throws IOException
    {
        return HBaseFactoryProvider.getHConnectionFactory().createConnection(configuration);
    }

    public void execute(ConnectorSession session, String statement)
    {
        execute(JdbcIdentity.from(session), statement);
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
            throw new PrestoException(PHOENIX_METADATA_ERROR, e);
        }
    }

    @Override
    public PreparedStatement buildSql(ConnectorSession session, Connection connection, JdbcSplit split, JdbcTableHandle table, List<JdbcColumnHandle> columnHandles)
            throws SQLException
    {
        PhoenixSplit phoenixSplit = (PhoenixSplit) split;
        PreparedStatement query = new QueryBuilder(identifierQuote).buildSql(
                this,
                session,
                connection,
                table.getCatalogName(),
                table.getSchemaName(),
                table.getTableName(),
                columnHandles,
                phoenixSplit.getConstraint(),
                split.getAdditionalPredicate(),
                tryApplyLimit(table.getLimit()));
        QueryPlan queryPlan = getQueryPlan((PhoenixPreparedStatement) query);
        ResultSet resultSet = getResultSet(phoenixSplit.getPhoenixInputSplit(), queryPlan);
        return new DelegatePreparedStatement(query)
        {
            @Override
            public ResultSet executeQuery()
            {
                return resultSet;
            }
        };
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> sql + " LIMIT " + limit);
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

    @Override
    protected String getTableSchemaName(ResultSet resultSet)
            throws SQLException
    {
        return firstNonNull(resultSet.getString("TABLE_SCHEM"), DEFAULT_SCHEMA);
    }

    @Override
    public Optional<ColumnMapping> toPrestoType(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        switch (typeHandle.getJdbcType()) {
            case VARCHAR:
            case NVARCHAR:
            case LONGVARCHAR:
            case LONGNVARCHAR:
                if (typeHandle.getColumnSize() == 0) {
                    return Optional.of(varcharColumnMapping(createUnboundedVarcharType()));
                }
                break;
            // TODO add support for TIMESTAMP after Phoenix adds support for LocalDateTime
            case TIMESTAMP:
            case TIME_WITH_TIMEZONE:
            case TIMESTAMP_WITH_TIMEZONE:
                if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
                    return mapToUnboundedVarchar(typeHandle);
                }
                return Optional.empty();
            case FLOAT:
                return Optional.of(realColumnMapping());
            case ARRAY:
                JdbcTypeHandle elementTypeHandle = getArrayElementTypeHandle(typeHandle);
                if (elementTypeHandle.getJdbcType() == Types.VARBINARY) {
                    return Optional.empty();
                }
                return toPrestoType(session, connection, elementTypeHandle)
                        .map(elementMapping -> {
                            ArrayType prestoArrayType = new ArrayType(elementMapping.getType());
                            String jdbcTypeName = elementTypeHandle.getJdbcTypeName()
                                    .orElseThrow(() -> new PrestoException(
                                            PHOENIX_METADATA_ERROR,
                                            "Type name is missing for jdbc type: " + JDBCType.valueOf(elementTypeHandle.getJdbcType())));
                            return arrayColumnMapping(session, prestoArrayType, jdbcTypeName);
                        });
        }
        return super.toPrestoType(session, connection, typeHandle);
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
            return WriteMapping.longMapping("time", timeWriteFunctionUsingSqlTime(session));
        }
        // Phoenix doesn't support _WITH_TIME_ZONE
        if (TIME_WITH_TIME_ZONE.equals(type) || TIMESTAMP_WITH_TIME_ZONE.equals(type)) {
            throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
        }
        if (type instanceof ArrayType) {
            Type elementType = ((ArrayType) type).getElementType();
            String elementDataType = toWriteMapping(session, elementType).getDataType().toUpperCase();
            String elementWriteName = getArrayElementPhoenixTypeName(session, this, elementType);
            return WriteMapping.blockMapping(elementDataType + " ARRAY", arrayWriteFunction(session, elementType, elementWriteName));
        }
        return super.toWriteMapping(session, type);
    }

    private static ColumnMapping arrayColumnMapping(ConnectorSession session, ArrayType arrayType, String elementJdbcTypeName)
    {
        return ColumnMapping.blockMapping(
                arrayType,
                arrayReadFunction(session, arrayType.getElementType()),
                arrayWriteFunction(session, arrayType.getElementType(), elementJdbcTypeName));
    }

    private static BlockReadFunction arrayReadFunction(ConnectorSession session, Type elementType)
    {
        return (resultSet, columnIndex) -> {
            Object[] objectArray = toBoxedArray(resultSet.getArray(columnIndex).getArray());
            return jdbcObjectArrayToBlock(session, elementType, objectArray);
        };
    }

    private static BlockWriteFunction arrayWriteFunction(ConnectorSession session, Type elementType, String elementJdbcTypeName)
    {
        return (statement, index, block) -> {
            Array jdbcArray = statement.getConnection().createArrayOf(elementJdbcTypeName, getJdbcObjectArray(session, elementType, block));
            statement.setArray(index, jdbcArray);
        };
    }

    private JdbcTypeHandle getArrayElementTypeHandle(JdbcTypeHandle arrayTypeHandle)
    {
        String arrayTypeName = arrayTypeHandle.getJdbcTypeName()
                .orElseThrow(() -> new PrestoException(PHOENIX_METADATA_ERROR, "Type name is missing for jdbc type: " + JDBCType.valueOf(arrayTypeHandle.getJdbcType())));
        checkArgument(arrayTypeName.endsWith(" ARRAY"), "array type must end with ' ARRAY'");
        arrayTypeName = arrayTypeName.substring(0, arrayTypeName.length() - " ARRAY".length());
        return new JdbcTypeHandle(
                PDataType.fromSqlTypeName(arrayTypeName).getSqlType(),
                Optional.of(arrayTypeName),
                arrayTypeHandle.getColumnSize(),
                arrayTypeHandle.getDecimalDigits(),
                arrayTypeHandle.getArrayDimensions());
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

    private static ResultSet getResultSet(PhoenixInputSplit split, QueryPlan queryPlan)
    {
        List<Scan> scans = split.getScans();
        try {
            List<PeekingResultIterator> iterators = new ArrayList<>(scans.size());
            StatementContext context = queryPlan.getContext();
            // Clear the table region boundary cache to make sure long running jobs stay up to date
            PName physicalTableName = queryPlan.getTableRef().getTable().getPhysicalName();
            PhoenixConnection phoenixConnection = context.getConnection();
            ConnectionQueryServices services = phoenixConnection.getQueryServices();
            services.clearTableRegionCache(physicalTableName.getBytes());

            for (Scan scan : scans) {
                scan = new Scan(scan);
                // For MR, skip the region boundary check exception if we encounter a split. ref: PHOENIX-2599
                scan.setAttribute(SKIP_REGION_BOUNDARY_CHECK, Bytes.toBytes(true));

                ScanMetricsHolder scanMetricsHolder = ScanMetricsHolder.getInstance(
                        context.getReadMetricsQueue(),
                        physicalTableName.getString(),
                        scan,
                        phoenixConnection.getLogLevel());

                TableResultIterator tableResultIterator = new TableResultIterator(
                        phoenixConnection.getMutationState(),
                        scan,
                        scanMetricsHolder,
                        services.getRenewLeaseThresholdMilliSeconds(),
                        queryPlan,
                        MapReduceParallelScanGrouper.getInstance());
                iterators.add(LookAheadResultIterator.wrap(tableResultIterator));
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
        catch (IOException e) {
            throw new PrestoException(PhoenixErrorCode.PHOENIX_INTERNAL_ERROR, "Error while copying scan", e);
        }
    }
}
