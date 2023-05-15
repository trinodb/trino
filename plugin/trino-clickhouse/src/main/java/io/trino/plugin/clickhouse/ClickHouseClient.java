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

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseVersion;
import com.google.common.base.Enums;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.InetAddresses;
import com.google.common.primitives.Shorts;
import com.google.inject.Inject;
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
import io.trino.plugin.jdbc.LongReadFunction;
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
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.plugin.jdbc.mapping.IdentifierMapping;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import javax.annotation.Nullable;

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
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static com.clickhouse.data.ClickHouseValues.convertToQuotedString;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.emptyToNull;
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
import static io.trino.plugin.clickhouse.TrinoToClickHouseWriteChecker.DATETIME;
import static io.trino.plugin.clickhouse.TrinoToClickHouseWriteChecker.UINT16;
import static io.trino.plugin.clickhouse.TrinoToClickHouseWriteChecker.UINT32;
import static io.trino.plugin.clickhouse.TrinoToClickHouseWriteChecker.UINT64;
import static io.trino.plugin.clickhouse.TrinoToClickHouseWriteChecker.UINT8;
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
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.ConnectorMetadata.MODIFYING_ROWS_MESSAGE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_SECONDS;
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
import static java.util.Objects.requireNonNull;

public class ClickHouseClient
        extends BaseJdbcClient
{
    private static final Splitter TABLE_PROPERTY_SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();

    private static final DecimalType UINT64_TYPE = createDecimalType(20, 0);

    // An empty character means that the table doesn't have a comment in ClickHouse
    private static final String NO_COMMENT = "";

    public static final int DEFAULT_DOMAIN_COMPACTION_THRESHOLD = 1_000;

    private final ConnectorExpressionRewriter<ParameterizedExpression> connectorExpressionRewriter;
    private final AggregateFunctionRewriter<JdbcExpression, ?> aggregateFunctionRewriter;
    private final Type uuidType;
    private final Type ipAddressType;
    private final AtomicReference<ClickHouseVersion> clickHouseVersion = new AtomicReference<>();

    @Inject
    public ClickHouseClient(
            BaseJdbcConfig config,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            TypeManager typeManager,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier)
    {
        super("\"", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, queryModifier, false);
        this.uuidType = typeManager.getType(new TypeSignature(StandardTypes.UUID));
        this.ipAddressType = typeManager.getType(new TypeSignature(StandardTypes.IPADDRESS));
        JdbcTypeHandle bigintTypeHandle = new JdbcTypeHandle(Types.BIGINT, Optional.of("bigint"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        this.connectorExpressionRewriter = JdbcConnectorExpressionRewriterBuilder.newBuilder()
                .addStandardRules(this::quoted)
                .build();
        this.aggregateFunctionRewriter = new AggregateFunctionRewriter<>(
                this.connectorExpressionRewriter,
                ImmutableSet.<AggregateFunctionRule<JdbcExpression, ParameterizedExpression>>builder()
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
    protected void copyTableSchema(ConnectorSession session, Connection connection, String catalogName, String schemaName, String tableName, String newTableName, List<String> columnNames)
    {
        // ClickHouse does not support `create table tbl as select * from tbl2 where 0=1`
        // ClickHouse supports the following two methods to copy schema
        // 1. create table tbl as tbl2
        // 2. create table tbl1 ENGINE=<engine> as select * from tbl2
        String sql = format(
                "CREATE TABLE %s AS %s ",
                quoted(null, schemaName, newTableName),
                quoted(null, schemaName, tableName));
        try {
            execute(session, connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public Optional<String> getTableComment(ResultSet resultSet)
            throws SQLException
    {
        // Empty remarks means that the table doesn't have a comment in ClickHouse
        return Optional.ofNullable(emptyToNull(resultSet.getString("REMARKS")));
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
        ClickHouseTableProperties.getSampleBy(tableProperties).ifPresent(value -> tableOptions.add("SAMPLE BY " + quoted(value)));
        tableMetadata.getComment().ifPresent(comment -> tableOptions.add(format("COMMENT %s", clickhouseVarcharLiteral(comment))));

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
        ClickHouseTableProperties.getSampleBy(properties).ifPresent(value -> tableOptions.add("SAMPLE BY " + quoted(value)));

        try (Connection connection = connectionFactory.openConnection(session)) {
            String sql = format(
                    "ALTER TABLE %s MODIFY %s",
                    quoted(handle.asPlainTable().getRemoteTableName()),
                    join(" ", tableOptions.build()));
            execute(session, connection, sql);
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
        if (column.getComment() != null) {
            sb.append(format(" COMMENT %s", clickhouseVarcharLiteral(column.getComment())));
        }
        return sb.toString();
    }

    @Override
    protected void createSchema(ConnectorSession session, Connection connection, String remoteSchemaName)
            throws SQLException
    {
        execute(session, connection, "CREATE DATABASE " + quoted(remoteSchemaName));
    }

    @Override
    protected void dropSchema(ConnectorSession session, Connection connection, String remoteSchemaName)
            throws SQLException
    {
        execute(session, connection, "DROP DATABASE " + quoted(remoteSchemaName));
    }

    @Override
    protected void renameSchema(ConnectorSession session, Connection connection, String remoteSchemaName, String newRemoteSchemaName)
            throws SQLException
    {
        execute(session, connection, "RENAME DATABASE " + quoted(remoteSchemaName) + " TO " + quoted(newRemoteSchemaName));
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
            execute(session, connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void setTableComment(ConnectorSession session, JdbcTableHandle handle, Optional<String> comment)
    {
        String sql = format(
                "ALTER TABLE %s MODIFY COMMENT %s",
                quoted(handle.asPlainTable().getRemoteTableName()),
                clickhouseVarcharLiteral(comment.orElse(NO_COMMENT)));
        execute(session, sql);
    }

    @Override
    public void setColumnComment(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Optional<String> comment)
    {
        String sql = format(
                "ALTER TABLE %s COMMENT COLUMN %s %s",
                quoted(handle.asPlainTable().getRemoteTableName()),
                quoted(column.getColumnName()),
                clickhouseVarcharLiteral(comment.orElse("")));
        execute(session, sql);
    }

    @Override
    public void setColumnType(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Type type)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support setting column types");
    }

    private static String clickhouseVarcharLiteral(String value)
    {
        requireNonNull(value, "value is null");
        return convertToQuotedString(value);
    }

    @Override
    protected Optional<List<String>> getTableTypes()
    {
        return Optional.empty();
    }

    @Override
    protected void renameTable(ConnectorSession session, Connection connection, String catalogName, String remoteSchemaName, String remoteTableName, String newRemoteSchemaName, String newRemoteTableName)
            throws SQLException
    {
        execute(session, connection, format("RENAME TABLE %s.%s TO %s.%s",
                quoted(remoteSchemaName),
                quoted(remoteTableName),
                quoted(newRemoteSchemaName),
                quoted(newRemoteTableName)));
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
        throw new TrinoException(NOT_SUPPORTED, MODIFYING_ROWS_MESSAGE);
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
                return Optional.of(ColumnMapping.longMapping(SMALLINT, ResultSet::getShort, uInt8WriteFunction(getClickHouseServerVersion(session))));
            case UInt16:
                return Optional.of(ColumnMapping.longMapping(INTEGER, ResultSet::getInt, uInt16WriteFunction(getClickHouseServerVersion(session))));
            case UInt32:
                return Optional.of(ColumnMapping.longMapping(BIGINT, ResultSet::getLong, uInt32WriteFunction(getClickHouseServerVersion(session))));
            case UInt64:
                return Optional.of(ColumnMapping.objectMapping(
                        UINT64_TYPE,
                        longDecimalReadFunction(UINT64_TYPE, UNNECESSARY),
                        uInt64WriteFunction(getClickHouseServerVersion(session))));
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
                return Optional.of(dateColumnMappingUsingLocalDate(getClickHouseServerVersion(session)));

            case Types.TIMESTAMP:
                if (columnDataType == ClickHouseDataType.DateTime) {
                    // ClickHouse DateTime does not have sub-second precision
                    verify(typeHandle.getRequiredDecimalDigits() == 0, "Expected 0 as timestamp precision, but got %s", typeHandle.getRequiredDecimalDigits());
                    return Optional.of(ColumnMapping.longMapping(
                            TIMESTAMP_SECONDS,
                            timestampReadFunction(TIMESTAMP_SECONDS),
                            timestampSecondsWriteFunction(getClickHouseServerVersion(session))));
                }
                // TODO (https://github.com/trinodb/trino/issues/10537) Add support for Datetime64 type
                return Optional.of(timestampColumnMappingUsingSqlTimestampWithRounding(TIMESTAMP_MILLIS));

            case Types.TIMESTAMP_WITH_TIMEZONE:
                if (columnDataType == ClickHouseDataType.DateTime) {
                    // ClickHouse DateTime does not have sub-second precision
                    verify(typeHandle.getRequiredDecimalDigits() == 0, "Expected 0 as timestamp with time zone precision, but got %s", typeHandle.getRequiredDecimalDigits());
                    return Optional.of(ColumnMapping.longMapping(
                            TIMESTAMP_TZ_SECONDS,
                            shortTimestampWithTimeZoneReadFunction(),
                            shortTimestampWithTimeZoneWriteFunction()));
                }
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
        if (type instanceof DecimalType decimalType) {
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
            return WriteMapping.longMapping("Date", dateWriteFunctionUsingLocalDate(getClickHouseServerVersion(session)));
        }
        if (type == TIMESTAMP_SECONDS) {
            return WriteMapping.longMapping("DateTime", timestampSecondsWriteFunction(getClickHouseServerVersion(session)));
        }
        if (type.equals(uuidType)) {
            return WriteMapping.sliceMapping("UUID", uuidWriteFunction());
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type);
    }

    private ClickHouseVersion getClickHouseServerVersion(ConnectorSession session)
    {
        return clickHouseVersion.updateAndGet(current -> {
            if (current != null) {
                return current;
            }

            try (Connection connection = connectionFactory.openConnection(session);
                    PreparedStatement statement = connection.prepareStatement("SELECT version()");
                    ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    current = ClickHouseVersion.of(resultSet.getString(1));
                }
                return current;
            }
            catch (SQLException e) {
                throw new TrinoException(JDBC_ERROR, e);
            }
        });
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
            return Optional.of(quoted(prop.get(0)));
        }
        // include more than one column
        return Optional.of(prop.stream().map(this::quoted).collect(Collectors.joining(",", "(", ")")));
    }

    private static LongWriteFunction uInt8WriteFunction(ClickHouseVersion version)
    {
        return (statement, index, value) -> {
            // ClickHouse stores incorrect results when the values are out of supported range.
            UINT8.validate(version, value);
            statement.setShort(index, Shorts.checkedCast(value));
        };
    }

    private static LongWriteFunction uInt16WriteFunction(ClickHouseVersion version)
    {
        return (statement, index, value) -> {
            // ClickHouse stores incorrect results when the values are out of supported range.
            UINT16.validate(version, value);
            statement.setInt(index, toIntExact(value));
        };
    }

    private static LongWriteFunction uInt32WriteFunction(ClickHouseVersion version)
    {
        return (preparedStatement, parameterIndex, value) -> {
            // ClickHouse stores incorrect results when the values are out of supported range.
            UINT32.validate(version, value);
            preparedStatement.setLong(parameterIndex, value);
        };
    }

    private static ObjectWriteFunction uInt64WriteFunction(ClickHouseVersion version)
    {
        return ObjectWriteFunction.of(
                Int128.class,
                (statement, index, value) -> {
                    BigInteger unscaledValue = value.toBigInteger();
                    BigDecimal bigDecimal = new BigDecimal(unscaledValue, UINT64_TYPE.getScale(), new MathContext(UINT64_TYPE.getPrecision()));
                    // ClickHouse stores incorrect results when the values are out of supported range.
                    UINT64.validate(version, bigDecimal);
                    statement.setBigDecimal(index, bigDecimal);
                });
    }

    private static ColumnMapping dateColumnMappingUsingLocalDate(ClickHouseVersion version)
    {
        return ColumnMapping.longMapping(
                DATE,
                dateReadFunctionUsingLocalDate(),
                dateWriteFunctionUsingLocalDate(version));
    }

    private static LongWriteFunction dateWriteFunctionUsingLocalDate(ClickHouseVersion version)
    {
        return (statement, index, value) -> {
            LocalDate date = LocalDate.ofEpochDay(value);
            // Deny unsupported dates eagerly to prevent unexpected results. ClickHouse stores '1970-01-01' when the date is out of supported range.
            TrinoToClickHouseWriteChecker.DATE.validate(version, date);
            statement.setObject(index, date);
        };
    }

    private static LongWriteFunction timestampSecondsWriteFunction(ClickHouseVersion version)
    {
        return (statement, index, value) -> {
            long epochSecond = floorDiv(value, MICROSECONDS_PER_SECOND);
            int nanoFraction = floorMod(value, MICROSECONDS_PER_SECOND) * NANOSECONDS_PER_MICROSECOND;
            verify(nanoFraction == 0, "Nanos of second must be zero: '%s'", value);
            LocalDateTime timestamp = LocalDateTime.ofEpochSecond(epochSecond, 0, UTC);
            // ClickHouse stores incorrect results when the values are out of supported range.
            DATETIME.validate(version, timestamp);
            statement.setObject(index, timestamp);
        };
    }

    private static LongReadFunction shortTimestampWithTimeZoneReadFunction()
    {
        return (resultSet, columnIndex) -> {
            ZonedDateTime zonedDateTime = resultSet.getObject(columnIndex, ZonedDateTime.class);
            return packDateTimeWithZone(zonedDateTime.toInstant().toEpochMilli(), zonedDateTime.getZone().getId());
        };
    }

    private static LongWriteFunction shortTimestampWithTimeZoneWriteFunction()
    {
        return (statement, index, value) -> {
            long millisUtc = unpackMillisUtc(value);
            TimeZoneKey timeZoneKey = unpackZoneKey(value);
            statement.setObject(index, Instant.ofEpochMilli(millisUtc).atZone(timeZoneKey.getZoneId()));
        };
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
        return new SliceWriteFunction()
        {
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
