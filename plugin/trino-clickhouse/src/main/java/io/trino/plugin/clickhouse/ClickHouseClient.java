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
package io.trino.plugin.clickhouse;

import com.clickhouse.client.ClickHouseColumn;
import com.clickhouse.client.ClickHouseDataType;
import com.google.common.base.Enums;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.InetAddresses;
import com.google.common.primitives.Shorts;
import io.airlift.slice.Slice;
import io.trino.plugin.base.aggregation.AggregateFunctionRewriter;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.SliceWriteFunction;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.aggregation.ImplementAvgFloatingPoint;
import io.trino.plugin.jdbc.aggregation.ImplementCount;
import io.trino.plugin.jdbc.aggregation.ImplementCountAll;
import io.trino.plugin.jdbc.aggregation.ImplementMinMax;
import io.trino.plugin.jdbc.aggregation.ImplementSum;
import io.trino.plugin.jdbc.expression.JdbcConnectorExpressionRewriterBuilder;
import io.trino.plugin.jdbc.mapping.IdentifierMapping;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.clickhouse.ClickHouseSessionProperties.isMapStringAsVarchar;
import static io.trino.plugin.clickhouse.ClickHouseTableProperties.ENGINE_PROPERTY;
import static io.trino.plugin.clickhouse.ClickHouseTableProperties.ORDER_BY_PROPERTY;
import static io.trino.plugin.clickhouse.ClickHouseTableProperties.PARTITION_BY_PROPERTY;
import static io.trino.plugin.clickhouse.ClickHouseTableProperties.PRIMARY_KEY_PROPERTY;
import static io.trino.plugin.clickhouse.ClickHouseTableProperties.SAMPLE_BY_PROPERTY;
import static io.trino.plugin.jdbc.DecimalConfig.DecimalMapping.ALLOW_OVERFLOW;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalDefaultScale;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRounding;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRoundingMode;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateReadFunctionUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampColumnMappingUsingSqlTimestampWithRounding;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.javaUuidToTrinoUuid;
import static io.trino.spi.type.UuidType.trinoUuidToJavaUuid;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.max;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.lang.System.arraycopy;
import static java.math.RoundingMode.UNNECESSARY;
import static java.time.ZoneOffset.UTC;
import static java.util.Locale.ENGLISH;

public class ClickHouseClient
        extends BaseJdbcClient
{
    private static final Splitter TABLE_PROPERTY_SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();

    private static final long UINT8_MIN_VALUE = 0L;
    private static final long UINT8_MAX_VALUE = 255L;

    private static final long UINT16_MIN_VALUE = 0L;
    private static final long UINT16_MAX_VALUE = 65535L;

    private static final long UINT32_MIN_VALUE = 0L;
    private static final long UINT32_MAX_VALUE = 4294967295L;

    private static final DecimalType UINT64_TYPE = createDecimalType(20, 0);
    private static final BigDecimal UINT64_MIN_VALUE = BigDecimal.ZERO;
    private static final BigDecimal UINT64_MAX_VALUE = new BigDecimal("18446744073709551615");

    private static final long MIN_SUPPORTED_DATE_EPOCH = LocalDate.parse("1970-01-01").toEpochDay();
    private static final long MAX_SUPPORTED_DATE_EPOCH = LocalDate.parse("2106-02-07").toEpochDay(); // The max date is '2148-12-31' in new ClickHouse version

    private static final LocalDateTime MIN_SUPPORTED_TIMESTAMP = LocalDateTime.parse("1970-01-01T00:00:00");
    private static final LocalDateTime MAX_SUPPORTED_TIMESTAMP = LocalDateTime.parse("2105-12-31T23:59:59");
    private static final long MIN_SUPPORTED_TIMESTAMP_EPOCH = MIN_SUPPORTED_TIMESTAMP.toEpochSecond(UTC);
    private static final long MAX_SUPPORTED_TIMESTAMP_EPOCH = MAX_SUPPORTED_TIMESTAMP.toEpochSecond(UTC);

    private final ConnectorExpressionRewriter<String> connectorExpressionRewriter;
    private final AggregateFunctionRewriter<JdbcExpression, String> aggregateFunctionRewriter;
    private final Type uuidType;
    private final Type ipAddressType;

    @Inject
    public ClickHouseClient(
            BaseJdbcConfig config,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            TypeManager typeManager,
            IdentifierMapping identifierMapping)
    {
        super(config, "\"", connectionFactory, queryBuilder, identifierMapping);
        this.uuidType = typeManager.getType(new TypeSignature(StandardTypes.UUID));
        this.ipAddressType = typeManager.getType(new TypeSignature(StandardTypes.IPADDRESS));
        JdbcTypeHandle bigintTypeHandle = new JdbcTypeHandle(Types.BIGINT, Optional.of("bigint"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        this.connectorExpressionRewriter = JdbcConnectorExpressionRewriterBuilder.newBuilder()
                .addStandardRules(this::quoted)
                .build();
        this.aggregateFunctionRewriter = new AggregateFunctionRewriter<>(
                this.connectorExpressionRewriter,
                ImmutableSet.<AggregateFunctionRule<JdbcExpression, String>>builder()
                        .add(new ImplementCountAll(bigintTypeHandle))
                        .add(new ImplementCount(bigintTypeHandle))
                        .add(new ImplementMinMax(false)) // TODO: Revisit once https://github.com/trinodb/trino/issues/7100 is resolved
                        .add(new ImplementSum(ClickHouseClient::toTypeHandle))
                        .add(new ImplementAvgFloatingPoint())
                        .add(new ImplementAvgBigint())
                        .build());
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        // TODO support complex ConnectorExpressions
        return aggregateFunctionRewriter.rewrite(session, aggregate, assignments);
    }

    @Override
    public boolean supportsAggregationPushdown(ConnectorSession session, JdbcTableHandle table, List<AggregateFunction> aggregates, Map<String, ColumnHandle> assignments, List<List<ColumnHandle>> groupingSets)
    {
        // TODO: Remove override once https://github.com/trinodb/trino/issues/7100 is resolved. Currently pushdown for textual types is not tested and may lead to incorrect results.
        return preventTextualTypeAggregationPushdown(groupingSets);
    }

    private static Optional<JdbcTypeHandle> toTypeHandle(DecimalType decimalType)
    {
        return Optional.of(new JdbcTypeHandle(Types.DECIMAL, Optional.of("Decimal"), Optional.of(decimalType.getPrecision()), Optional.of(decimalType.getScale()), Optional.empty(), Optional.empty()));
    }

    @Override
    protected String quoted(@Nullable String catalog, @Nullable String schema, String table)
    {
        StringBuilder sb = new StringBuilder();
        if (!isNullOrEmpty(schema)) {
            sb.append(quoted(schema)).append(".");
        }
        sb.append(quoted(table));
        return sb.toString();
    }

    @Override
    protected void copyTableSchema(Connection connection, String catalogName, String schemaName, String tableName, String newTableName, List<String> columnNames)
    {
        // ClickHouse does not support `create table tbl as select * from tbl2 where 0=1`
        // ClickHouse supports the following two methods to copy schema
        // 1. create table tbl as tbl2
        // 2. create table tbl1 ENGINE=<engine> as select * from tbl2
        String sql = format(
                "CREATE TABLE %s AS %s ",
                quoted(null, schemaName, newTableName),
                quoted(null, schemaName, tableName));
        execute(connection, sql);
    }

    @Override
    public Optional<String> getTableComment(ResultSet resultSet)
    {
        // Don't return a comment until the connector supports creating tables with comment
        return Optional.empty();
    }

    @Override
    protected String createTableSql(RemoteTableName remoteTableName, List<String> columns, ConnectorTableMetadata tableMetadata)
    {
        ImmutableList.Builder<String> tableOptions = ImmutableList.builder();
        Map<String, Object> tableProperties = tableMetadata.getProperties();
        ClickHouseEngineType engine = ClickHouseTableProperties.getEngine(tableProperties);
        tableOptions.add("ENGINE = " + engine.getEngineType());
        if (engine == ClickHouseEngineType.MERGETREE && formatProperty(ClickHouseTableProperties.getOrderBy(tableProperties)).isEmpty()) {
            // order_by property is required
            throw new TrinoException(INVALID_TABLE_PROPERTY,
                    format("The property of %s is required for table engine %s", ClickHouseTableProperties.ORDER_BY_PROPERTY, engine.getEngineType()));
        }
        formatProperty(ClickHouseTableProperties.getOrderBy(tableProperties)).ifPresent(value -> tableOptions.add("ORDER BY " + value));
        formatProperty(ClickHouseTableProperties.getPrimaryKey(tableProperties)).ifPresent(value -> tableOptions.add("PRIMARY KEY " + value));
        formatProperty(ClickHouseTableProperties.getPartitionBy(tableProperties)).ifPresent(value -> tableOptions.add("PARTITION BY " + value));
        ClickHouseTableProperties.getSampleBy(tableProperties).ifPresent(value -> tableOptions.add("SAMPLE BY " + value));

        return format("CREATE TABLE %s (%s) %s", quoted(remoteTableName), join(", ", columns), join(" ", tableOptions.build()));
    }

    @Override
    public Map<String, Object> getTableProperties(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        try (Connection connection = connectionFactory.openConnection(session);
                PreparedStatement statement = connection.prepareStatement("" +
                        "SELECT engine, sorting_key, partition_key, primary_key, sampling_key " +
                        "FROM system.tables " +
                        "WHERE database = ? AND name = ?")) {
            statement.setString(1, tableHandle.asPlainTable().getRemoteTableName().getSchemaName().orElse(null));
            statement.setString(2, tableHandle.asPlainTable().getRemoteTableName().getTableName());

            try (ResultSet resultSet = statement.executeQuery()) {
                ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
                while (resultSet.next()) {
                    String engine = resultSet.getString("engine");
                    if (!isNullOrEmpty(engine)) {
                        // Don't throw an exception because many table engines aren't supported in ClickHouseEngineType
                        Optional<ClickHouseEngineType> engineType = Enums.getIfPresent(ClickHouseEngineType.class, engine.toUpperCase(ENGLISH)).toJavaUtil();
                        engineType.ifPresent(type -> properties.put(ENGINE_PROPERTY, type));
                    }
                    String sortingKey = resultSet.getString("sorting_key");
                    if (!isNullOrEmpty(sortingKey)) {
                        properties.put(ORDER_BY_PROPERTY, TABLE_PROPERTY_SPLITTER.splitToList(sortingKey));
                    }
                    String partitionKey = resultSet.getString("partition_key");
                    if (!isNullOrEmpty(partitionKey)) {
                        properties.put(PARTITION_BY_PROPERTY, TABLE_PROPERTY_SPLITTER.splitToList(partitionKey));
                    }
                    String primaryKey = resultSet.getString("primary_key");
                    if (!isNullOrEmpty(primaryKey)) {
                        properties.put(PRIMARY_KEY_PROPERTY, TABLE_PROPERTY_SPLITTER.splitToList(primaryKey));
                    }
                    String samplingKey = resultSet.getString("sampling_key");
                    if (!isNullOrEmpty(samplingKey)) {
                        properties.put(SAMPLE_BY_PROPERTY, samplingKey);
                    }
                }
                return properties.buildOrThrow();
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void setTableProperties(ConnectorSession session, JdbcTableHandle handle, Map<String, Optional<Object>> nullableProperties)
    {
        // TODO: Support other table properties
        checkArgument(nullableProperties.size() == 1 && nullableProperties.containsKey(SAMPLE_BY_PROPERTY), "Only support setting 'sample_by' property");
        // TODO: Support sampling key removal when we support a newer version of ClickHouse. See https://github.com/ClickHouse/ClickHouse/pull/30180.
        checkArgument(nullableProperties.values().stream().noneMatch(Optional::isEmpty), "Setting a property to null is not supported");

        Map<String, Object> properties = nullableProperties.entrySet().stream()
                .filter(entry -> entry.getValue().isPresent())
                .collect(toImmutableMap(Entry::getKey, entry -> entry.getValue().orElseThrow()));

        ImmutableList.Builder<String> tableOptions = ImmutableList.builder();
        ClickHouseTableProperties.getSampleBy(properties).ifPresent(value -> tableOptions.add("SAMPLE BY " + value));

        try (Connection connection = connectionFactory.openConnection(session)) {
            String sql = format(
                    "ALTER TABLE %s MODIFY %s",
                    quoted(handle.asPlainTable().getRemoteTableName()),
                    join(" ", tableOptions.build()));
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected String getColumnDefinitionSql(ConnectorSession session, ColumnMetadata column, String columnName)
    {
        StringBuilder sb = new StringBuilder()
                .append(quoted(columnName))
                .append(" ");
        if (column.isNullable()) {
            // set column nullable property explicitly
            sb.append("Nullable(").append(toWriteMapping(session, column.getType()).getDataType()).append(")");
        }
        else {
            // By default, the clickhouse column is not allowed to be null
            sb.append(toWriteMapping(session, column.getType()).getDataType());
        }
        return sb.toString();
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName)
    {
        execute(session, "CREATE DATABASE " + quoted(schemaName));
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        execute(session, "DROP DATABASE " + quoted(schemaName));
    }

    @Override
    protected String renameSchemaSql(String remoteSchemaName, String newRemoteSchemaName)
    {
        return "RENAME DATABASE " + quoted(remoteSchemaName) + " TO " + quoted(newRemoteSchemaName);
    }

    @Override
    public void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column)
    {
        try (Connection connection = connectionFactory.openConnection(session)) {
            String remoteColumnName = getIdentifierMapping().toRemoteColumnName(connection, column.getName());
            String sql = format(
                    "ALTER TABLE %s ADD COLUMN %s",
                    quoted(handle.asPlainTable().getRemoteTableName()),
                    getColumnDefinitionSql(session, column, remoteColumnName));
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void renameColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        try (Connection connection = connectionFactory.openConnection(session)) {
            String newRemoteColumnName = getIdentifierMapping().toRemoteColumnName(connection, newColumnName);
            String sql = format("ALTER TABLE %s RENAME COLUMN %s TO %s ",
                    quoted(handle.getRemoteTableName()),
                    quoted(jdbcColumn.getColumnName()),
                    quoted(newRemoteColumnName));
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void setColumnComment(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Optional<String> comment)
    {
        String sql = format(
                "ALTER TABLE %s COMMENT COLUMN %s '%s'",
                quoted(handle.asPlainTable().getRemoteTableName()),
                quoted(column.getColumnName()),
                comment.orElse(""));
        execute(session, sql);
    }

    @Override
    protected Optional<List<String>> getTableTypes()
    {
        return Optional.empty();
    }

    @Override
    public void dropTable(ConnectorSession session, JdbcTableHandle handle)
    {
        String sql = "DROP TABLE " + quoted(handle.getRemoteTableName());
        execute(session, sql);
    }

    @Override
    protected void renameTable(ConnectorSession session, String catalogName, String schemaName, String tableName, SchemaTableName newTable)
    {
        String sql = format("RENAME TABLE %s.%s TO %s.%s",
                quoted(schemaName),
                quoted(tableName),
                quoted(newTable.getSchemaName()),
                quoted(newTable.getTableName()));
        execute(session, sql);
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> sql + " LIMIT " + limit);
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    public OptionalLong delete(ConnectorSession session, JdbcTableHandle handle)
    {
        // ClickHouse does not support DELETE syntax, but is using custom: ALTER TABLE [db.]table [ON CLUSTER cluster] DELETE WHERE filter_expr
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support deletes");
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        String jdbcTypeName = typeHandle.getJdbcTypeName()
                .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + typeHandle));

        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }

        ClickHouseColumn column = ClickHouseColumn.of("", jdbcTypeName);
        ClickHouseDataType columnDataType = column.getDataType();
        switch (columnDataType) {
            case UInt8:
                return Optional.of(ColumnMapping.longMapping(SMALLINT, ResultSet::getShort, uInt8WriteFunction()));
            case UInt16:
                return Optional.of(ColumnMapping.longMapping(INTEGER, ResultSet::getInt, uInt16WriteFunction()));
            case UInt32:
                return Optional.of(ColumnMapping.longMapping(BIGINT, ResultSet::getLong, uInt32WriteFunction()));
            case UInt64:
                return Optional.of(ColumnMapping.objectMapping(
                        UINT64_TYPE,
                        longDecimalReadFunction(UINT64_TYPE, UNNECESSARY),
                        uInt64WriteFunction()));
            case IPv4:
                return Optional.of(ipAddressColumnMapping("IPv4StringToNum(?)"));
            case IPv6:
                return Optional.of(ipAddressColumnMapping("IPv6StringToNum(?)"));
            case Enum8:
            case Enum16:
                return Optional.of(ColumnMapping.sliceMapping(
                        createUnboundedVarcharType(),
                        varcharReadFunction(createUnboundedVarcharType()),
                        varcharWriteFunction(),
                        // TODO (https://github.com/trinodb/trino/issues/7100) Currently pushdown would not work and may require a custom bind expression
                        DISABLE_PUSHDOWN));

            case FixedString: // FixedString(n)
            case String:
                if (isMapStringAsVarchar(session)) {
                    return Optional.of(ColumnMapping.sliceMapping(
                            createUnboundedVarcharType(),
                            varcharReadFunction(createUnboundedVarcharType()),
                            varcharWriteFunction(),
                            DISABLE_PUSHDOWN));
                }
                // TODO (https://github.com/trinodb/trino/issues/7100) test & enable predicate pushdown
                return Optional.of(varbinaryColumnMapping());
            case UUID:
                return Optional.of(uuidColumnMapping());
            default:
                // no-op
        }

        switch (typeHandle.getJdbcType()) {
            case Types.TINYINT:
                return Optional.of(tinyintColumnMapping());

            case Types.SMALLINT:
                return Optional.of(smallintColumnMapping());

            case Types.INTEGER:
                return Optional.of(integerColumnMapping());

            case Types.BIGINT:
                return Optional.of(bigintColumnMapping());

            case Types.FLOAT:
            case Types.REAL:
                return Optional.of(ColumnMapping.longMapping(
                        REAL,
                        (resultSet, columnIndex) -> floatToRawIntBits(resultSet.getFloat(columnIndex)),
                        realWriteFunction(),
                        DISABLE_PUSHDOWN));

            case Types.DOUBLE:
                return Optional.of(doubleColumnMapping());

            case Types.DECIMAL:
                int decimalDigits = typeHandle.getRequiredDecimalDigits();
                int precision = typeHandle.getRequiredColumnSize();

                ColumnMapping decimalColumnMapping;
                if (getDecimalRounding(session) == ALLOW_OVERFLOW && precision > Decimals.MAX_PRECISION) {
                    int scale = Math.min(decimalDigits, getDecimalDefaultScale(session));
                    decimalColumnMapping = decimalColumnMapping(createDecimalType(Decimals.MAX_PRECISION, scale), getDecimalRoundingMode(session));
                }
                else {
                    decimalColumnMapping = decimalColumnMapping(createDecimalType(precision, max(decimalDigits, 0)));
                }
                return Optional.of(new ColumnMapping(
                        decimalColumnMapping.getType(),
                        decimalColumnMapping.getReadFunction(),
                        decimalColumnMapping.getWriteFunction(),
                        // TODO (https://github.com/trinodb/trino/issues/7100) fix, enable and test decimal pushdown
                        DISABLE_PUSHDOWN));

            case Types.DATE:
                return Optional.of(dateColumnMappingUsingLocalDate());

            case Types.TIMESTAMP:
                if (columnDataType == ClickHouseDataType.DateTime) {
                    verify(typeHandle.getRequiredDecimalDigits() == 0, "Expected 0 as timestamp precision, but got %s", typeHandle.getRequiredDecimalDigits());
                    return Optional.of(ColumnMapping.longMapping(
                            TIMESTAMP_SECONDS,
                            timestampReadFunction(TIMESTAMP_SECONDS),
                            timestampSecondsWriteFunction()));
                }
                // TODO (https://github.com/trinodb/trino/issues/10537) Add support for Datetime64 type
                return Optional.of(timestampColumnMappingUsingSqlTimestampWithRounding(TIMESTAMP_MILLIS));
        }

        return Optional.empty();
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type == BOOLEAN) {
            // ClickHouse is no separate type for boolean values. Use UInt8 type, restricted to the values 0 or 1.
            return WriteMapping.booleanMapping("UInt8", booleanWriteFunction());
        }
        if (type == TINYINT) {
            return WriteMapping.longMapping("Int8", tinyintWriteFunction());
        }
        if (type == SMALLINT) {
            return WriteMapping.longMapping("Int16", smallintWriteFunction());
        }
        if (type == INTEGER) {
            return WriteMapping.longMapping("Int32", integerWriteFunction());
        }
        if (type == BIGINT) {
            return WriteMapping.longMapping("Int64", bigintWriteFunction());
        }
        if (type == REAL) {
            return WriteMapping.longMapping("Float32", realWriteFunction());
        }
        if (type == DOUBLE) {
            return WriteMapping.doubleMapping("Float64", doubleWriteFunction());
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            String dataType = format("Decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.objectMapping(dataType, longDecimalWriteFunction(decimalType));
        }
        if (type instanceof CharType || type instanceof VarcharType) {
            // The String type replaces the types VARCHAR, BLOB, CLOB, and others from other DBMSs.
            return WriteMapping.sliceMapping("String", varcharWriteFunction());
        }
        if (type instanceof VarbinaryType) {
            // Strings of an arbitrary length. The length is not limited
            return WriteMapping.sliceMapping("String", varbinaryWriteFunction());
        }
        if (type == DATE) {
            // TODO (https://github.com/trinodb/trino/issues/10055) Deny unsupported dates to prevent inserting wrong values. 2106-02-07 is max value in version 20.8
            return WriteMapping.longMapping("Date", dateWriteFunctionUsingLocalDate());
        }
        if (type == TIMESTAMP_SECONDS) {
            return WriteMapping.longMapping("DateTime", timestampSecondsWriteFunction());
        }
        if (type.equals(uuidType)) {
            return WriteMapping.sliceMapping("UUID", uuidWriteFunction());
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type);
    }

    /**
     * format property to match ClickHouse create table statement
     *
     * @param prop property will be formatted
     * @return formatted property
     */
    private Optional<String> formatProperty(List<String> prop)
    {
        if (prop == null || prop.isEmpty()) {
            return Optional.empty();
        }
        if (prop.size() == 1) {
            // only one column
            return Optional.of(prop.get(0));
        }
        // include more than one column
        return Optional.of("(" + String.join(",", prop) + ")");
    }

    private static LongWriteFunction uInt8WriteFunction()
    {
        return (statement, index, value) -> {
            // ClickHouse stores incorrect results when the values are out of supported range.
            if (value < UINT8_MIN_VALUE || value > UINT8_MAX_VALUE) {
                throw new TrinoException(INVALID_ARGUMENTS, format("Value must be between %s and %s in ClickHouse: %s", UINT8_MIN_VALUE, UINT8_MAX_VALUE, value));
            }
            statement.setShort(index, Shorts.checkedCast(value));
        };
    }

    private static LongWriteFunction uInt16WriteFunction()
    {
        return (statement, index, value) -> {
            // ClickHouse stores incorrect results when the values are out of supported range.
            if (value < UINT16_MIN_VALUE || value > UINT16_MAX_VALUE) {
                throw new TrinoException(INVALID_ARGUMENTS, format("Value must be between %s and %s in ClickHouse: %s", UINT16_MIN_VALUE, UINT16_MAX_VALUE, value));
            }
            statement.setInt(index, toIntExact(value));
        };
    }

    private static LongWriteFunction uInt32WriteFunction()
    {
        return (preparedStatement, parameterIndex, value) -> {
            // ClickHouse stores incorrect results when the values are out of supported range.
            if (value < UINT32_MIN_VALUE || value > UINT32_MAX_VALUE) {
                throw new TrinoException(INVALID_ARGUMENTS, format("Value must be between %s and %s in ClickHouse: %s", UINT32_MIN_VALUE, UINT32_MAX_VALUE, value));
            }
            preparedStatement.setLong(parameterIndex, value);
        };
    }

    private static ObjectWriteFunction uInt64WriteFunction()
    {
        return ObjectWriteFunction.of(
                Int128.class,
                (statement, index, value) -> {
                    BigInteger unscaledValue = value.toBigInteger();
                    BigDecimal bigDecimal = new BigDecimal(unscaledValue, UINT64_TYPE.getScale(), new MathContext(UINT64_TYPE.getPrecision()));
                    // ClickHouse stores incorrect results when the values are out of supported range.
                    if (bigDecimal.compareTo(UINT64_MIN_VALUE) < 0 || bigDecimal.compareTo(UINT64_MAX_VALUE) > 0) {
                        throw new TrinoException(INVALID_ARGUMENTS, format("Value must be between %s and %s in ClickHouse: %s", UINT64_MIN_VALUE, UINT64_MAX_VALUE, bigDecimal));
                    }
                    statement.setBigDecimal(index, bigDecimal);
                });
    }

    private static ColumnMapping dateColumnMappingUsingLocalDate()
    {
        return ColumnMapping.longMapping(
                DATE,
                dateReadFunctionUsingLocalDate(),
                dateWriteFunctionUsingLocalDate());
    }

    private static LongWriteFunction dateWriteFunctionUsingLocalDate()
    {
        return (statement, index, value) -> {
            verifySupportedDate(value);
            statement.setObject(index, LocalDate.ofEpochDay(value));
        };
    }

    private static void verifySupportedDate(long value)
    {
        // Deny unsupported dates eagerly to prevent unexpected results. ClickHouse stores '1970-01-01' when the date is out of supported range.
        if (value < MIN_SUPPORTED_DATE_EPOCH || value > MAX_SUPPORTED_DATE_EPOCH) {
            throw new TrinoException(INVALID_ARGUMENTS, format("Date must be between %s and %s in ClickHouse: %s", LocalDate.ofEpochDay(MIN_SUPPORTED_DATE_EPOCH), LocalDate.ofEpochDay(MAX_SUPPORTED_DATE_EPOCH), LocalDate.ofEpochDay(value)));
        }
    }

    private static LongWriteFunction timestampSecondsWriteFunction()
    {
        return (statement, index, value) -> {
            long epochSecond = floorDiv(value, MICROSECONDS_PER_SECOND);
            int nanoFraction = floorMod(value, MICROSECONDS_PER_SECOND) * NANOSECONDS_PER_MICROSECOND;
            verify(nanoFraction == 0, "Nanos of second must be zero: '%s'", value);
            verifySupportedTimestamp(epochSecond);
            statement.setObject(index, LocalDateTime.ofEpochSecond(epochSecond, 0, UTC));
        };
    }

    private static void verifySupportedTimestamp(long epochSecond)
    {
        if (epochSecond < MIN_SUPPORTED_TIMESTAMP_EPOCH || epochSecond > MAX_SUPPORTED_TIMESTAMP_EPOCH) {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss");
            throw new TrinoException(INVALID_ARGUMENTS, format("Timestamp must be between %s and %s in ClickHouse: %s", MIN_SUPPORTED_TIMESTAMP.format(formatter), MAX_SUPPORTED_TIMESTAMP.format(formatter), LocalDateTime.ofEpochSecond(epochSecond, 0, UTC).format(formatter)));
        }
    }

    private ColumnMapping ipAddressColumnMapping(String writeBindExpression)
    {
        return ColumnMapping.sliceMapping(
                ipAddressType,
                (resultSet, columnIndex) -> {
                    // copied from IpAddressOperators.castFromVarcharToIpAddress
                    byte[] address = InetAddresses.forString(resultSet.getString(columnIndex)).getAddress();

                    byte[] bytes;
                    if (address.length == 4) {
                        bytes = new byte[16];
                        bytes[10] = (byte) 0xff;
                        bytes[11] = (byte) 0xff;
                        arraycopy(address, 0, bytes, 12, 4);
                    }
                    else if (address.length == 16) {
                        bytes = address;
                    }
                    else {
                        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Invalid InetAddress length: " + address.length);
                    }

                    return wrappedBuffer(bytes);
                },
                ipAddressWriteFunction(writeBindExpression));
    }

    private static SliceWriteFunction ipAddressWriteFunction(String bindExpression)
    {
        return new SliceWriteFunction() {
            @Override
            public String getBindExpression()
            {
                return bindExpression;
            }

            @Override
            public void set(PreparedStatement statement, int index, Slice value)
                    throws SQLException
            {
                try {
                    statement.setObject(index, InetAddresses.toAddrString(InetAddress.getByAddress(value.getBytes())), Types.OTHER);
                }
                catch (UnknownHostException e) {
                    throw new UncheckedIOException(e);
                }
            }
        };
    }

    private ColumnMapping uuidColumnMapping()
    {
        return ColumnMapping.sliceMapping(
                uuidType,
                (resultSet, columnIndex) -> javaUuidToTrinoUuid((UUID) resultSet.getObject(columnIndex)),
                uuidWriteFunction());
    }

    private static SliceWriteFunction uuidWriteFunction()
    {
        return (statement, index, value) -> statement.setObject(index, trinoUuidToJavaUuid(value), Types.OTHER);
    }
}
