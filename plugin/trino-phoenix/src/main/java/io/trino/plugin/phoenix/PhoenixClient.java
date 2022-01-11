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
package io.trino.plugin.phoenix;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcSortItem;
import io.trino.plugin.jdbc.JdbcSplit;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.ObjectReadFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.WriteFunction;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.mapping.IdentifierMapping;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
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
import static io.trino.plugin.jdbc.DecimalConfig.DecimalMapping.ALLOW_OVERFLOW;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalDefaultScale;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRounding;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRoundingMode;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateColumnMappingUsingSqlDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateWriteFunctionUsingSqlDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.defaultCharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.defaultVarcharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.realColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timeWriteFunctionUsingSqlTime;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.plugin.phoenix.MetadataUtil.getEscapedTableName;
import static io.trino.plugin.phoenix.MetadataUtil.toPhoenixSchemaName;
import static io.trino.plugin.phoenix.PhoenixClientModule.getConnectionProperties;
import static io.trino.plugin.phoenix.PhoenixColumnProperties.isPrimaryKey;
import static io.trino.plugin.phoenix.PhoenixErrorCode.PHOENIX_METADATA_ERROR;
import static io.trino.plugin.phoenix.PhoenixErrorCode.PHOENIX_QUERY_ERROR;
import static io.trino.plugin.phoenix.PhoenixMetadata.DEFAULT_SCHEMA;
import static io.trino.plugin.phoenix.TypeUtils.getArrayElementPhoenixTypeName;
import static io.trino.plugin.phoenix.TypeUtils.getJdbcObjectArray;
import static io.trino.plugin.phoenix.TypeUtils.jdbcObjectArrayToBlock;
import static io.trino.plugin.phoenix.TypeUtils.toBoxedArray;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.DEFAULT_PRECISION;
import static io.trino.spi.type.DecimalType.DEFAULT_SCALE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME;
import static io.trino.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.Math.max;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.math.RoundingMode.UNNECESSARY;
import static java.sql.Types.ARRAY;
import static java.sql.Types.LONGNVARCHAR;
import static java.sql.Types.LONGVARCHAR;
import static java.sql.Types.NVARCHAR;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TIMESTAMP_WITH_TIMEZONE;
import static java.sql.Types.TIME_WITH_TIMEZONE;
import static java.sql.Types.VARCHAR;
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
    private static final long MAX_TOPN_LIMIT = 2000000;

    private final Configuration configuration;

    @Inject
    public PhoenixClient(PhoenixConfig config, ConnectionFactory connectionFactory, IdentifierMapping identifierMapping)
            throws SQLException
    {
        super(
                ESCAPE_CHARACTER,
                connectionFactory,
                ImmutableSet.of(),
                identifierMapping);
        this.configuration = new Configuration(false);
        getConnectionProperties(config).forEach((k, v) -> configuration.set((String) k, (String) v));
    }

    public Connection getConnection(ConnectorSession session)
            throws SQLException
    {
        return connectionFactory.openConnection(session);
    }

    public org.apache.hadoop.hbase.client.Connection getHConnection()
            throws IOException
    {
        return HBaseFactoryProvider.getHConnectionFactory().createConnection(configuration);
    }

    @Override
    public void execute(ConnectorSession session, String statement)
    {
        super.execute(session, statement);
    }

    @Override
    public Collection<String> listSchemas(Connection connection)
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
            throw new TrinoException(PHOENIX_METADATA_ERROR, e);
        }
    }

    @Override
    public PreparedStatement buildSql(ConnectorSession session, Connection connection, JdbcSplit split, JdbcTableHandle table, List<JdbcColumnHandle> columnHandles)
            throws SQLException
    {
        PreparedStatement query = prepareStatement(
                session,
                connection,
                table,
                columnHandles,
                Optional.of(split));
        QueryPlan queryPlan = getQueryPlan((PhoenixPreparedStatement) query);
        ResultSet resultSet = getResultSet(((PhoenixSplit) split).getPhoenixInputSplit(), queryPlan);
        return new DelegatePreparedStatement(query)
        {
            @Override
            public ResultSet executeQuery()
            {
                return resultSet;
            }
        };
    }

    public PreparedStatement prepareStatement(
            ConnectorSession session,
            Connection connection,
            JdbcTableHandle table,
            List<JdbcColumnHandle> columns,
            Optional<JdbcSplit> split)
            throws SQLException
    {
        PreparedQuery preparedQuery = prepareQuery(
                session,
                connection,
                table,
                Optional.empty(),
                columns,
                ImmutableMap.of(),
                split);
        return new QueryBuilder(this).prepareStatement(session, connection, preparedQuery);
    }

    @Override
    public boolean supportsTopN(ConnectorSession session, JdbcTableHandle handle, List<JdbcSortItem> sortOrder)
    {
        return true;
    }

    @Override
    protected Optional<TopNFunction> topNFunction()
    {
        return Optional.of((query, sortItems, limit) -> {
            // TODO: Remove when this is fixed in Phoenix.
            // Phoenix severely over-estimates the memory
            // required to execute a topN query.
            // https://issues.apache.org/jira/browse/PHOENIX-6436
            if (limit > MAX_TOPN_LIMIT) {
                return query;
            }
            return TopNFunction.sqlStandard(this::quoted).apply(query, sortItems, limit);
        });
    }

    @Override
    public boolean isTopNGuaranteed(ConnectorSession session)
    {
        return false;
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> sql + " LIMIT " + limit);
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
        return false;
    }

    @Override
    public String buildInsertSql(JdbcOutputTableHandle handle, List<WriteFunction> columnWriters)
    {
        PhoenixOutputTableHandle outputHandle = (PhoenixOutputTableHandle) handle;
        String params = columnWriters.stream()
                .map(WriteFunction::getBindExpression)
                .collect(joining(","));
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
    public ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        return super.getTables(connection, schemaName.map(MetadataUtil::toPhoenixSchemaName), tableName);
    }

    @Override
    protected String getTableSchemaName(ResultSet resultSet)
            throws SQLException
    {
        return firstNonNull(resultSet.getString("TABLE_SCHEM"), DEFAULT_SCHEMA);
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }

        switch (typeHandle.getJdbcType()) {
            case Types.BOOLEAN:
                return Optional.of(booleanColumnMapping());

            case Types.TINYINT:
                return Optional.of(tinyintColumnMapping());

            case Types.SMALLINT:
                return Optional.of(smallintColumnMapping());

            case Types.INTEGER:
                return Optional.of(integerColumnMapping());

            case Types.BIGINT:
                return Optional.of(bigintColumnMapping());

            case Types.FLOAT:
                return Optional.of(realColumnMapping());

            case Types.DOUBLE:
                return Optional.of(doubleColumnMapping());

            case Types.DECIMAL:
                Optional<Integer> columnSize = typeHandle.getColumnSize();
                int precision = columnSize.orElse(DEFAULT_PRECISION);
                int decimalDigits = typeHandle.getDecimalDigits().orElse(DEFAULT_SCALE);
                if (getDecimalRounding(session) == ALLOW_OVERFLOW) {
                    if (columnSize.isEmpty()) {
                        return Optional.of(decimalColumnMapping(createDecimalType(Decimals.MAX_PRECISION, getDecimalDefaultScale(session)), getDecimalRoundingMode(session)));
                    }
                }
                // TODO does phoenix support negative scale?
                precision = precision + max(-decimalDigits, 0); // Map decimal(p, -s) (negative scale) to decimal(p+s, 0).
                if (precision > Decimals.MAX_PRECISION) {
                    break;
                }
                return Optional.of(decimalColumnMapping(createDecimalType(precision, max(decimalDigits, 0)), UNNECESSARY));

            case Types.CHAR:
                return Optional.of(defaultCharColumnMapping(typeHandle.getRequiredColumnSize(), true));

            case VARCHAR:
            case NVARCHAR:
            case LONGVARCHAR:
            case LONGNVARCHAR:
                if (typeHandle.getColumnSize().isEmpty()) {
                    return Optional.of(varcharColumnMapping(createUnboundedVarcharType(), true));
                }
                return Optional.of(defaultVarcharColumnMapping(typeHandle.getRequiredColumnSize(), true));

            case Types.BINARY:
            case Types.VARBINARY:
                return Optional.of(varbinaryColumnMapping());

            case Types.DATE:
                return Optional.of(dateColumnMappingUsingSqlDate());

            // TODO add support for TIMESTAMP after Phoenix adds support for LocalDateTime
            case TIMESTAMP:
            case TIME_WITH_TIMEZONE:
            case TIMESTAMP_WITH_TIMEZONE:
                if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
                    return mapToUnboundedVarchar(typeHandle);
                }
                return Optional.empty();

            case ARRAY:
                JdbcTypeHandle elementTypeHandle = getArrayElementTypeHandle(typeHandle);
                if (elementTypeHandle.getJdbcType() == Types.VARBINARY) {
                    return Optional.empty();
                }
                return toColumnMapping(session, connection, elementTypeHandle)
                        .map(elementMapping -> {
                            ArrayType trinoArrayType = new ArrayType(elementMapping.getType());
                            String jdbcTypeName = elementTypeHandle.getJdbcTypeName()
                                    .orElseThrow(() -> new TrinoException(
                                            PHOENIX_METADATA_ERROR,
                                            "Type name is missing for jdbc type: " + JDBCType.valueOf(elementTypeHandle.getJdbcType())));
                            return arrayColumnMapping(session, trinoArrayType, jdbcTypeName);
                        });
        }
        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return mapToUnboundedVarchar(typeHandle);
        }
        return Optional.empty();
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type == BOOLEAN) {
            return WriteMapping.booleanMapping("boolean", booleanWriteFunction());
        }

        if (type == TINYINT) {
            return WriteMapping.longMapping("tinyint", tinyintWriteFunction());
        }
        if (type == SMALLINT) {
            return WriteMapping.longMapping("smallint", smallintWriteFunction());
        }
        if (type == INTEGER) {
            return WriteMapping.longMapping("integer", integerWriteFunction());
        }
        if (type == BIGINT) {
            return WriteMapping.longMapping("bigint", bigintWriteFunction());
        }
        if (type == REAL) {
            return WriteMapping.longMapping("float", realWriteFunction());
        }
        if (type == DOUBLE) {
            return WriteMapping.doubleMapping("double", doubleWriteFunction());
        }

        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            String dataType = format("decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.objectMapping(dataType, longDecimalWriteFunction(decimalType));
        }

        if (type instanceof CharType) {
            return WriteMapping.sliceMapping("char(" + ((CharType) type).getLength() + ")", charWriteFunction());
        }
        if (type instanceof VarcharType) {
            VarcharType varcharType = (VarcharType) type;
            String dataType;
            if (varcharType.isUnbounded()) {
                dataType = "varchar";
            }
            else {
                dataType = "varchar(" + varcharType.getBoundedLength() + ")";
            }
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }
        if (type instanceof VarbinaryType) {
            return WriteMapping.sliceMapping("varbinary", varbinaryWriteFunction());
        }

        if (type == DATE) {
            return WriteMapping.longMapping("date", dateWriteFunctionUsingSqlDate());
        }
        if (TIME.equals(type)) {
            return WriteMapping.longMapping("time", timeWriteFunctionUsingSqlTime());
        }
        // Phoenix doesn't support _WITH_TIME_ZONE
        if (TIME_WITH_TIME_ZONE.equals(type) || TIMESTAMP_TZ_MILLIS.equals(type)) {
            throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
        }
        if (type instanceof ArrayType) {
            Type elementType = ((ArrayType) type).getElementType();
            String elementDataType = toWriteMapping(session, elementType).getDataType().toUpperCase(ENGLISH);
            String elementWriteName = getArrayElementPhoenixTypeName(session, this, elementType);
            return WriteMapping.objectMapping(elementDataType + " ARRAY", arrayWriteFunction(session, elementType, elementWriteName));
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    @Override
    public JdbcOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schema = schemaTableName.getSchemaName();
        String table = schemaTableName.getTableName();

        if (!getSchemaNames(session).contains(schema)) {
            throw new SchemaNotFoundException(schema);
        }

        try (Connection connection = connectionFactory.openConnection(session)) {
            ConnectorIdentity identity = session.getIdentity();
            schema = getIdentifierMapping().toRemoteSchemaName(identity, connection, schema);
            table = getIdentifierMapping().toRemoteTableName(identity, connection, schema, table);
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
                String columnName = getIdentifierMapping().toRemoteColumnName(connection, column.getName());
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
                throw new TrinoException(ALREADY_EXISTS, "Phoenix table already exists", e);
            }
            throw new TrinoException(PHOENIX_METADATA_ERROR, "Error creating Phoenix table", e);
        }
    }

    @Override
    protected void renameTable(ConnectorSession session, String catalogName, String schemaName, String tableName, SchemaTableName newTable)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming tables");
    }

    @Override
    public Map<String, Object> getTableProperties(ConnectorSession session, JdbcTableHandle handle)
    {
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();

        try (Connection connection = connectionFactory.openConnection(session);
                HBaseAdmin admin = connection.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
            String schemaName = toPhoenixSchemaName(handle.getSchemaName());
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
            throw new TrinoException(PHOENIX_METADATA_ERROR, "Couldn't get Phoenix table properties", e);
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
                .orElseThrow(() -> new TrinoException(PHOENIX_METADATA_ERROR, "Type name is missing for jdbc type: " + JDBCType.valueOf(arrayTypeHandle.getJdbcType())));
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
            throw new TrinoException(PHOENIX_QUERY_ERROR, "Failed to get the Phoenix query plan", e);
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
            ResultIterator iterator = ConcatResultIterator.newIterator(iterators);
            if (context.getSequenceManager().getSequenceCount() > 0) {
                iterator = new SequenceResultIterator(iterator, context.getSequenceManager());
            }
            // Clone the row projector as it's not thread safe and would be used simultaneously by
            // multiple threads otherwise.
            return new PhoenixResultSet(iterator, queryPlan.getProjector().cloneIfNecessary(), context);
        }
        catch (SQLException e) {
            throw new TrinoException(PHOENIX_QUERY_ERROR, "Error while setting up Phoenix ResultSet", e);
        }
        catch (IOException e) {
            throw new TrinoException(PhoenixErrorCode.PHOENIX_INTERNAL_ERROR, "Error while copying scan", e);
        }
    }
}
