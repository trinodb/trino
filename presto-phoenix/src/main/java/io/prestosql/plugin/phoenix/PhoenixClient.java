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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.ColumnMapping;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcOutputTableHandle;
import io.prestosql.plugin.jdbc.JdbcSplit;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.plugin.jdbc.ObjectReadFunction;
import io.prestosql.plugin.jdbc.ObjectWriteFunction;
import io.prestosql.plugin.jdbc.QueryBuilder;
import io.prestosql.plugin.jdbc.WriteMapping;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.SchemaNotFoundException;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.Type;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.exception.SQLExceptionCode;
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
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableProperty;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.SchemaUtil;

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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.BiFunction;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.realColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.timeWriteFunctionUsingSqlTime;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varcharColumnMapping;
import static io.prestosql.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.prestosql.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.prestosql.plugin.phoenix.MetadataUtil.getEscapedTableName;
import static io.prestosql.plugin.phoenix.MetadataUtil.toPhoenixSchemaName;
import static io.prestosql.plugin.phoenix.PhoenixClientModule.getConnectionProperties;
import static io.prestosql.plugin.phoenix.PhoenixColumnProperties.isPrimaryKey;
import static io.prestosql.plugin.phoenix.PhoenixErrorCode.PHOENIX_METADATA_ERROR;
import static io.prestosql.plugin.phoenix.PhoenixErrorCode.PHOENIX_QUERY_ERROR;
import static io.prestosql.plugin.phoenix.PhoenixMetadata.DEFAULT_SCHEMA;
import static io.prestosql.plugin.phoenix.TypeUtils.getArrayElementPhoenixTypeName;
import static io.prestosql.plugin.phoenix.TypeUtils.getJdbcObjectArray;
import static io.prestosql.plugin.phoenix.TypeUtils.jdbcObjectArrayToBlock;
import static io.prestosql.plugin.phoenix.TypeUtils.toBoxedArray;
import static io.prestosql.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
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
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;
import static org.apache.hadoop.hbase.HConstants.FOREVER;
import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.SKIP_REGION_BOUNDARY_CHECK;
import static org.apache.phoenix.util.PhoenixRuntime.getTable;
import static org.apache.phoenix.util.SchemaUtil.ESCAPE_CHARACTER;
import static org.apache.phoenix.util.SchemaUtil.getEscapedArgument;

public class PhoenixClient
        extends BaseJdbcClient
{
    private static final String ROWKEY = "ROWKEY";

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
                String schemaName = getTableSchemaName(resultSet);
                // skip internal schemas
                if (filterSchema(schemaName)) {
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
        PreparedStatement query = new QueryBuilder(this).buildSql(
                session,
                connection,
                table.getRemoteTableName(),
                table.getGroupingSets(),
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
        String columns = handle.getColumnNames().stream()
                .map(SchemaUtil::getEscapedArgument)
                .collect(joining(","));
        if (outputHandle.rowkeyColumn().isPresent()) {
            String nextId = format(
                    "NEXT VALUE FOR %s, ",
                    quoted(null, handle.getSchemaName(), handle.getTableName() + "_sequence"));
            params = nextId + params;
            columns = outputHandle.rowkeyColumn().get() + ", " + columns;
        }
        return format(
                "UPSERT INTO %s (%s) VALUES (%s)",
                quoted(null, handle.getSchemaName(), handle.getTableName()),
                columns,
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
            return WriteMapping.longMapping("time", timeWriteFunctionUsingSqlTime());
        }
        // Phoenix doesn't support _WITH_TIME_ZONE
        if (TIME_WITH_TIME_ZONE.equals(type) || TIMESTAMP_TZ_MILLIS.equals(type)) {
            throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
        }
        if (type instanceof ArrayType) {
            Type elementType = ((ArrayType) type).getElementType();
            String elementDataType = toWriteMapping(session, elementType).getDataType().toUpperCase();
            String elementWriteName = getArrayElementPhoenixTypeName(session, this, elementType);
            return WriteMapping.objectMapping(elementDataType + " ARRAY", arrayWriteFunction(session, elementType, elementWriteName));
        }
        return super.toWriteMapping(session, type);
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
        return false;
    }

    @Override
    public JdbcOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        Optional<String> schema = Optional.of(schemaTableName.getSchemaName());
        String table = schemaTableName.getTableName();

        if (!getSchemaNames(JdbcIdentity.from(session)).contains(schema.orElse(null))) {
            throw new SchemaNotFoundException(schema.orElse(null));
        }

        try (Connection connection = connectionFactory.openConnection(JdbcIdentity.from(session))) {
            boolean uppercase = connection.getMetaData().storesUpperCaseIdentifiers();
            if (uppercase) {
                schema = schema.map(schemaName -> schemaName.toUpperCase(ENGLISH));
                table = table.toUpperCase(ENGLISH);
            }
            schema = toPhoenixSchemaName(schema);
            LinkedList<ColumnMetadata> tableColumns = new LinkedList<>(tableMetadata.getColumns());
            Map<String, Object> tableProperties = tableMetadata.getProperties();
            Optional<Boolean> immutableRows = PhoenixTableProperties.getImmutableRows(tableProperties);
            String immutable = immutableRows.isPresent() && immutableRows.get() ? "IMMUTABLE" : "";

            ImmutableList.Builder<String> columnNames = ImmutableList.builder();
            ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
            ImmutableList.Builder<String> columnList = ImmutableList.builder();
            Set<ColumnMetadata> rowkeyColumns = tableColumns.stream().filter(col -> isPrimaryKey(col, tableProperties)).collect(toSet());
            ImmutableList.Builder<String> pkNames = ImmutableList.builder();
            Optional<String> rowkeyColumn = Optional.empty();
            if (rowkeyColumns.isEmpty()) {
                // Add a rowkey when not specified in DDL
                columnList.add(ROWKEY + " bigint not null");
                pkNames.add(ROWKEY);
                execute(session, format("CREATE SEQUENCE %s", getEscapedTableName(schema, table + "_sequence")));
                rowkeyColumn = Optional.of(ROWKEY);
            }
            for (ColumnMetadata column : tableColumns) {
                String columnName = column.getName();
                if (uppercase) {
                    columnName = columnName.toUpperCase(ENGLISH);
                }
                columnNames.add(columnName);
                columnTypes.add(column.getType());
                String typeStatement = toWriteMapping(session, column.getType()).getDataType();
                if (rowkeyColumns.contains(column)) {
                    typeStatement += " not null";
                    pkNames.add(columnName);
                }
                columnList.add(format("%s %s", getEscapedArgument(columnName), typeStatement));
            }

            ImmutableList.Builder<String> tableOptions = ImmutableList.builder();
            PhoenixTableProperties.getSaltBuckets(tableProperties).ifPresent(value -> tableOptions.add(TableProperty.SALT_BUCKETS + "=" + value));
            PhoenixTableProperties.getSplitOn(tableProperties).ifPresent(value -> tableOptions.add("SPLIT ON (" + value.replace('"', '\'') + ")"));
            PhoenixTableProperties.getDisableWal(tableProperties).ifPresent(value -> tableOptions.add(TableProperty.DISABLE_WAL + "=" + value));
            PhoenixTableProperties.getDefaultColumnFamily(tableProperties).ifPresent(value -> tableOptions.add(TableProperty.DEFAULT_COLUMN_FAMILY + "=" + value));
            PhoenixTableProperties.getBloomfilter(tableProperties).ifPresent(value -> tableOptions.add(HColumnDescriptor.BLOOMFILTER + "='" + value + "'"));
            PhoenixTableProperties.getVersions(tableProperties).ifPresent(value -> tableOptions.add(HConstants.VERSIONS + "=" + value));
            PhoenixTableProperties.getMinVersions(tableProperties).ifPresent(value -> tableOptions.add(HColumnDescriptor.MIN_VERSIONS + "=" + value));
            PhoenixTableProperties.getCompression(tableProperties).ifPresent(value -> tableOptions.add(HColumnDescriptor.COMPRESSION + "='" + value + "'"));
            PhoenixTableProperties.getTimeToLive(tableProperties).ifPresent(value -> tableOptions.add(HColumnDescriptor.TTL + "=" + value));
            PhoenixTableProperties.getDataBlockEncoding(tableProperties).ifPresent(value -> tableOptions.add(HColumnDescriptor.DATA_BLOCK_ENCODING + "='" + value + "'"));

            String sql = format(
                    "CREATE %s TABLE %s (%s , CONSTRAINT PK PRIMARY KEY (%s)) %s",
                    immutable,
                    getEscapedTableName(schema, table),
                    join(", ", columnList.build()),
                    join(", ", pkNames.build()),
                    join(", ", tableOptions.build()));

            execute(session, sql);

            return new PhoenixOutputTableHandle(
                    schema,
                    table,
                    columnNames.build(),
                    columnTypes.build(),
                    Optional.empty(),
                    rowkeyColumn);
        }
        catch (SQLException e) {
            if (e.getErrorCode() == SQLExceptionCode.TABLE_ALREADY_EXIST.getErrorCode()) {
                throw new PrestoException(ALREADY_EXISTS, "Phoenix table already exists", e);
            }
            throw new PrestoException(PHOENIX_METADATA_ERROR, "Error creating Phoenix table", e);
        }
    }

    @Override
    public Map<String, Object> getTableProperties(JdbcIdentity identity, JdbcTableHandle handle)
    {
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();

        try (PhoenixConnection connection = (PhoenixConnection) connectionFactory.openConnection(identity);
                HBaseAdmin admin = connection.getQueryServices().getAdmin()) {
            String schemaName = toPhoenixSchemaName(Optional.ofNullable(handle.getSchemaName())).orElse(null);
            PTable table = getTable(connection, SchemaUtil.getTableName(schemaName, handle.getTableName()));

            boolean salted = table.getBucketNum() != null;
            StringJoiner joiner = new StringJoiner(",");
            List<PColumn> pkColumns = table.getPKColumns();
            for (PColumn pkColumn : pkColumns.subList(salted ? 1 : 0, pkColumns.size())) {
                joiner.add(pkColumn.getName().getString());
            }
            properties.put(PhoenixTableProperties.ROWKEYS, joiner.toString());

            if (table.getBucketNum() != null) {
                properties.put(PhoenixTableProperties.SALT_BUCKETS, table.getBucketNum());
            }
            if (table.isWALDisabled()) {
                properties.put(PhoenixTableProperties.DISABLE_WAL, table.isWALDisabled());
            }
            if (table.isImmutableRows()) {
                properties.put(PhoenixTableProperties.IMMUTABLE_ROWS, table.isImmutableRows());
            }

            String defaultFamilyName = QueryConstants.DEFAULT_COLUMN_FAMILY;
            if (table.getDefaultFamilyName() != null) {
                defaultFamilyName = table.getDefaultFamilyName().getString();
                properties.put(PhoenixTableProperties.DEFAULT_COLUMN_FAMILY, defaultFamilyName);
            }

            HTableDescriptor tableDesc = admin.getTableDescriptor(table.getPhysicalName().getBytes());

            HColumnDescriptor[] columnFamilies = tableDesc.getColumnFamilies();
            for (HColumnDescriptor columnFamily : columnFamilies) {
                if (columnFamily.getNameAsString().equals(defaultFamilyName)) {
                    if (columnFamily.getBloomFilterType() != BloomType.NONE) {
                        properties.put(PhoenixTableProperties.BLOOMFILTER, columnFamily.getBloomFilterType());
                    }
                    if (columnFamily.getMaxVersions() != 1) {
                        properties.put(PhoenixTableProperties.VERSIONS, columnFamily.getMaxVersions());
                    }
                    if (columnFamily.getMinVersions() > 0) {
                        properties.put(PhoenixTableProperties.MIN_VERSIONS, columnFamily.getMinVersions());
                    }
                    if (columnFamily.getCompression() != Compression.Algorithm.NONE) {
                        properties.put(PhoenixTableProperties.COMPRESSION, columnFamily.getCompression());
                    }
                    if (columnFamily.getTimeToLive() < FOREVER) {
                        properties.put(PhoenixTableProperties.TTL, columnFamily.getTimeToLive());
                    }
                    if (columnFamily.getDataBlockEncoding() != DataBlockEncoding.NONE) {
                        properties.put(PhoenixTableProperties.DATA_BLOCK_ENCODING, columnFamily.getDataBlockEncoding());
                    }
                    break;
                }
            }
        }
        catch (IOException | SQLException e) {
            throw new PrestoException(PHOENIX_METADATA_ERROR, "Couldn't get Phoenix table properties", e);
        }
        return properties.build();
    }

    private static ColumnMapping arrayColumnMapping(ConnectorSession session, ArrayType arrayType, String elementJdbcTypeName)
    {
        return ColumnMapping.objectMapping(
                arrayType,
                arrayReadFunction(session, arrayType.getElementType()),
                arrayWriteFunction(session, arrayType.getElementType(), elementJdbcTypeName));
    }

    private static ObjectReadFunction arrayReadFunction(ConnectorSession session, Type elementType)
    {
        return ObjectReadFunction.of(Block.class, (resultSet, columnIndex) -> {
            Object[] objectArray = toBoxedArray(resultSet.getArray(columnIndex).getArray());
            return jdbcObjectArrayToBlock(session, elementType, objectArray);
        });
    }

    private static ObjectWriteFunction arrayWriteFunction(ConnectorSession session, Type elementType, String elementJdbcTypeName)
    {
        return ObjectWriteFunction.of(Block.class, (statement, index, block) -> {
            Array jdbcArray = statement.getConnection().createArrayOf(elementJdbcTypeName, getJdbcObjectArray(session, elementType, block));
            statement.setArray(index, jdbcArray);
        });
    }

    private JdbcTypeHandle getArrayElementTypeHandle(JdbcTypeHandle arrayTypeHandle)
    {
        String arrayTypeName = arrayTypeHandle.getJdbcTypeName()
                .orElseThrow(() -> new PrestoException(PHOENIX_METADATA_ERROR, "Type name is missing for jdbc type: " + JDBCType.valueOf(arrayTypeHandle.getJdbcType())));
        checkArgument(arrayTypeName.endsWith(" ARRAY"), "array type must end with ' ARRAY'");
        arrayTypeName = arrayTypeName.substring(0, arrayTypeName.length() - " ARRAY".length());
        verify(arrayTypeHandle.getCaseSensitivity().isEmpty(), "Case sensitivity not supported");
        return new JdbcTypeHandle(
                PDataType.fromSqlTypeName(arrayTypeName).getSqlType(),
                Optional.of(arrayTypeName),
                arrayTypeHandle.getColumnSize(),
                arrayTypeHandle.getDecimalDigits(),
                arrayTypeHandle.getArrayDimensions(),
                Optional.empty());
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
