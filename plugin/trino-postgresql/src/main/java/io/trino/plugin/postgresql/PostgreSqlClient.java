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
package io.trino.plugin.postgresql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.math.LongMath;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.plugin.base.aggregation.AggregateFunctionRewriter;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.BooleanReadFunction;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DoubleReadFunction;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcJoinCondition;
import io.trino.plugin.jdbc.JdbcSortItem;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongReadFunction;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.ObjectReadFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.plugin.jdbc.PredicatePushdownController;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.ReadFunction;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.SliceReadFunction;
import io.trino.plugin.jdbc.SliceWriteFunction;
import io.trino.plugin.jdbc.UnsupportedTypeHandling;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.aggregation.ImplementAvgDecimal;
import io.trino.plugin.jdbc.aggregation.ImplementAvgFloatingPoint;
import io.trino.plugin.jdbc.aggregation.ImplementCorr;
import io.trino.plugin.jdbc.aggregation.ImplementCount;
import io.trino.plugin.jdbc.aggregation.ImplementCountAll;
import io.trino.plugin.jdbc.aggregation.ImplementCountDistinct;
import io.trino.plugin.jdbc.aggregation.ImplementCovariancePop;
import io.trino.plugin.jdbc.aggregation.ImplementCovarianceSamp;
import io.trino.plugin.jdbc.aggregation.ImplementMinMax;
import io.trino.plugin.jdbc.aggregation.ImplementRegrIntercept;
import io.trino.plugin.jdbc.aggregation.ImplementRegrSlope;
import io.trino.plugin.jdbc.aggregation.ImplementStddevPop;
import io.trino.plugin.jdbc.aggregation.ImplementStddevSamp;
import io.trino.plugin.jdbc.aggregation.ImplementSum;
import io.trino.plugin.jdbc.aggregation.ImplementVariancePop;
import io.trino.plugin.jdbc.aggregation.ImplementVarianceSamp;
import io.trino.plugin.jdbc.expression.JdbcConnectorExpressionRewriterBuilder;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.expression.RewriteComparison;
import io.trino.plugin.jdbc.expression.RewriteIn;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.plugin.jdbc.mapping.IdentifierMapping;
import io.trino.plugin.postgresql.PostgreSqlConfig.ArrayMapping;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.SingleMapBlock;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.JoinCondition;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.predicate.Domain;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.VarcharType;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.postgresql.core.TypeInfo;
import org.postgresql.jdbc.PgConnection;

import java.io.IOException;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.base.util.JsonTypeUtil.jsonParse;
import static io.trino.plugin.base.util.JsonTypeUtil.toJsonValue;
import static io.trino.plugin.jdbc.DecimalConfig.DecimalMapping.ALLOW_OVERFLOW;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalDefaultScale;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRounding;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRoundingMode;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.JdbcJoinPushdownUtil.implementJoinCostAware;
import static io.trino.plugin.jdbc.JdbcMetadataSessionProperties.getDomainCompactionThreshold;
import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.PredicatePushdownController.FULL_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateColumnMappingUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateWriteFunctionUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.fromTrinoTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.realColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.IGNORE;
import static io.trino.plugin.postgresql.PostgreSqlConfig.ArrayMapping.AS_ARRAY;
import static io.trino.plugin.postgresql.PostgreSqlConfig.ArrayMapping.AS_JSON;
import static io.trino.plugin.postgresql.PostgreSqlConfig.ArrayMapping.DISABLED;
import static io.trino.plugin.postgresql.PostgreSqlSessionProperties.getArrayMapping;
import static io.trino.plugin.postgresql.PostgreSqlSessionProperties.isEnableStringPushdownWithCollate;
import static io.trino.plugin.postgresql.TypeUtils.arrayDepth;
import static io.trino.plugin.postgresql.TypeUtils.getArrayElementPgTypeName;
import static io.trino.plugin.postgresql.TypeUtils.getJdbcObjectArray;
import static io.trino.plugin.postgresql.TypeUtils.toPgTimestamp;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.StandardTypes.JSON;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.round;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.spi.type.UuidType.javaUuidToTrinoUuid;
import static io.trino.spi.type.UuidType.trinoUuidToJavaUuid;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.math.RoundingMode.UNNECESSARY;
import static java.sql.DatabaseMetaData.columnNoNulls;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;

public class PostgreSqlClient
        extends BaseJdbcClient
{
    private static final Logger log = Logger.get(PostgreSqlClient.class);

    /**
     * @see Array#getResultSet()
     */
    private static final int ARRAY_RESULT_SET_VALUE_COLUMN = 2;
    private static final String DUPLICATE_TABLE_SQLSTATE = "42P07";
    private static final int POSTGRESQL_MAX_SUPPORTED_TIMESTAMP_PRECISION = 6;
    private static final int PRECISION_OF_UNSPECIFIED_DECIMAL = 0;

    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS");

    private static final PredicatePushdownController POSTGRESQL_STRING_COLLATION_AWARE_PUSHDOWN = (session, domain) -> {
        if (domain.isOnlyNull()) {
            return FULL_PUSHDOWN.apply(session, domain);
        }

        if (isEnableStringPushdownWithCollate(session)) {
            return FULL_PUSHDOWN.apply(session, domain);
        }

        Domain simplifiedDomain = domain.simplify(getDomainCompactionThreshold(session));
        if (!simplifiedDomain.getValues().isDiscreteSet()) {
            // Domain#simplify can turn a discrete set into a range predicate
            return DISABLE_PUSHDOWN.apply(session, domain);
        }

        return FULL_PUSHDOWN.apply(session, simplifiedDomain);
    };

    private final Type jsonType;
    private final Type uuidType;
    private final MapType varcharMapType;
    private final List<String> tableTypes;
    private final boolean statisticsEnabled;
    private final ConnectorExpressionRewriter<ParameterizedExpression> connectorExpressionRewriter;
    private final AggregateFunctionRewriter<JdbcExpression, ?> aggregateFunctionRewriter;

    @Inject
    public PostgreSqlClient(
            BaseJdbcConfig config,
            PostgreSqlConfig postgreSqlConfig,
            JdbcStatisticsConfig statisticsConfig,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            TypeManager typeManager,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier)
    {
        super("\"", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, queryModifier, true);
        this.jsonType = typeManager.getType(new TypeSignature(JSON));
        this.uuidType = typeManager.getType(new TypeSignature(StandardTypes.UUID));
        this.varcharMapType = (MapType) typeManager.getType(mapType(VARCHAR.getTypeSignature(), VARCHAR.getTypeSignature()));

        ImmutableList.Builder<String> tableTypes = ImmutableList.builder();
        tableTypes.add("TABLE", "PARTITIONED TABLE", "VIEW", "MATERIALIZED VIEW", "FOREIGN TABLE");
        if (postgreSqlConfig.isIncludeSystemTables()) {
            tableTypes.add("SYSTEM TABLE", "SYSTEM VIEW");
        }
        this.tableTypes = tableTypes.build();

        this.statisticsEnabled = statisticsConfig.isEnabled();

        this.connectorExpressionRewriter = JdbcConnectorExpressionRewriterBuilder.newBuilder()
                .addStandardRules(this::quoted)
                // TODO allow all comparison operators for numeric types
                .add(new RewriteComparison(ImmutableSet.of(RewriteComparison.ComparisonOperator.EQUAL, RewriteComparison.ComparisonOperator.NOT_EQUAL)))
                .add(new RewriteIn())
                .withTypeClass("integer_type", ImmutableSet.of("tinyint", "smallint", "integer", "bigint"))
                .map("$add(left: integer_type, right: integer_type)").to("left + right")
                .map("$subtract(left: integer_type, right: integer_type)").to("left - right")
                .map("$multiply(left: integer_type, right: integer_type)").to("left * right")
                .map("$divide(left: integer_type, right: integer_type)").to("left / right")
                .map("$modulus(left: integer_type, right: integer_type)").to("left % right")
                .map("$negate(value: integer_type)").to("-value")
                .map("$like(value: varchar, pattern: varchar): boolean").to("value LIKE pattern")
                .map("$like(value: varchar, pattern: varchar, escape: varchar(1)): boolean").to("value LIKE pattern ESCAPE escape")
                .map("$not($is_null(value))").to("value IS NOT NULL")
                .map("$not(value: boolean)").to("NOT value")
                .map("$is_null(value)").to("value IS NULL")
                .map("$nullif(first, second)").to("NULLIF(first, second)")
                .build();

        JdbcTypeHandle bigintTypeHandle = new JdbcTypeHandle(Types.BIGINT, Optional.of("bigint"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        this.aggregateFunctionRewriter = new AggregateFunctionRewriter<>(
                this.connectorExpressionRewriter,
                ImmutableSet.<AggregateFunctionRule<JdbcExpression, ParameterizedExpression>>builder()
                        .add(new ImplementCountAll(bigintTypeHandle))
                        .add(new ImplementMinMax(false))
                        .add(new ImplementCount(bigintTypeHandle))
                        .add(new ImplementCountDistinct(bigintTypeHandle, false))
                        .add(new ImplementSum(PostgreSqlClient::toTypeHandle))
                        .add(new ImplementAvgFloatingPoint())
                        .add(new ImplementAvgDecimal())
                        .add(new ImplementAvgBigint())
                        .add(new ImplementStddevSamp())
                        .add(new ImplementStddevPop())
                        .add(new ImplementVarianceSamp())
                        .add(new ImplementVariancePop())
                        .add(new ImplementCovarianceSamp())
                        .add(new ImplementCovariancePop())
                        .add(new ImplementCorr())
                        .add(new ImplementRegrIntercept())
                        .add(new ImplementRegrSlope())
                        .build());
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        try {
            createTable(session, tableMetadata, tableMetadata.getTable().getTableName());
        }
        catch (SQLException e) {
            boolean exists = DUPLICATE_TABLE_SQLSTATE.equals(e.getSQLState());
            throw new TrinoException(exists ? ALREADY_EXISTS : JDBC_ERROR, e);
        }
    }

    @Override
    protected List<String> createTableSqls(RemoteTableName remoteTableName, List<String> columns, ConnectorTableMetadata tableMetadata)
    {
        checkArgument(tableMetadata.getProperties().isEmpty(), "Unsupported table properties: %s", tableMetadata.getProperties());
        ImmutableList.Builder<String> createTableSqlsBuilder = ImmutableList.builder();
        createTableSqlsBuilder.add(format("CREATE TABLE %s (%s)", quoted(remoteTableName), join(", ", columns)));
        Optional<String> tableComment = tableMetadata.getComment();
        if (tableComment.isPresent()) {
            createTableSqlsBuilder.add(buildTableCommentSql(remoteTableName, tableComment));
        }
        return createTableSqlsBuilder.build();
    }

    @Override
    public void setTableComment(ConnectorSession session, JdbcTableHandle handle, Optional<String> comment)
    {
        execute(session, buildTableCommentSql(handle.asPlainTable().getRemoteTableName(), comment));
    }

    private String buildTableCommentSql(RemoteTableName remoteTableName, Optional<String> comment)
    {
        return format(
                "COMMENT ON TABLE %s IS %s",
                quoted(remoteTableName),
                comment.map(BaseJdbcClient::varcharLiteral).orElse("NULL"));
    }

    @Override
    protected void renameTable(ConnectorSession session, Connection connection, String catalogName, String remoteSchemaName, String remoteTableName, String newRemoteSchemaName, String newRemoteTableName)
            throws SQLException
    {
        if (!remoteSchemaName.equals(newRemoteSchemaName)) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming tables across schemas");
        }

        execute(session, connection, format(
                "ALTER TABLE %s RENAME TO %s",
                quoted(catalogName, remoteSchemaName, remoteTableName),
                quoted(newRemoteTableName)));
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql, Optional<Integer> columnCount)
            throws SQLException
    {
        // fetch-size is ignored when connection is in auto-commit
        connection.setAutoCommit(false);
        PreparedStatement statement = connection.prepareStatement(sql);
        // This is a heuristic, not exact science. A better formula can perhaps be found with measurements.
        // Column count is not known for non-SELECT queries. Not setting fetch size for these.
        if (columnCount.isPresent()) {
            statement.setFetchSize(max(100_000 / columnCount.get(), 1_000));
        }
        return statement;
    }

    @Override
    protected Optional<List<String>> getTableTypes()
    {
        return Optional.of(tableTypes);
    }

    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        if (tableHandle.getColumns().isPresent()) {
            return tableHandle.getColumns().get();
        }
        checkArgument(tableHandle.isNamedRelation(), "Cannot get columns for %s", tableHandle);
        SchemaTableName schemaTableName = tableHandle.getRequiredNamedRelation().getSchemaTableName();

        try (Connection connection = connectionFactory.openConnection(session)) {
            Map<String, Integer> arrayColumnDimensions = ImmutableMap.of();
            if (getArrayMapping(session) == AS_ARRAY) {
                arrayColumnDimensions = getArrayColumnDimensions(connection, tableHandle);
            }
            try (ResultSet resultSet = getColumns(tableHandle, connection.getMetaData())) {
                int allColumns = 0;
                List<JdbcColumnHandle> columns = new ArrayList<>();
                while (resultSet.next()) {
                    allColumns++;
                    String columnName = resultSet.getString("COLUMN_NAME");
                    JdbcTypeHandle typeHandle = new JdbcTypeHandle(
                            getInteger(resultSet, "DATA_TYPE").orElseThrow(() -> new IllegalStateException("DATA_TYPE is null")),
                            Optional.of(resultSet.getString("TYPE_NAME")),
                            getInteger(resultSet, "COLUMN_SIZE"),
                            getInteger(resultSet, "DECIMAL_DIGITS"),
                            Optional.ofNullable(arrayColumnDimensions.get(columnName)),
                            Optional.empty());
                    Optional<ColumnMapping> columnMapping = toColumnMapping(session, connection, typeHandle);
                    log.debug("Mapping data type of '%s' column '%s': %s mapped to %s", schemaTableName, columnName, typeHandle, columnMapping);
                    // skip unsupported column types
                    if (columnMapping.isPresent()) {
                        boolean nullable = (resultSet.getInt("NULLABLE") != columnNoNulls);
                        Optional<String> comment = Optional.ofNullable(resultSet.getString("REMARKS"));
                        columns.add(JdbcColumnHandle.builder()
                                .setColumnName(columnName)
                                .setJdbcTypeHandle(typeHandle)
                                .setColumnType(columnMapping.get().getType())
                                .setNullable(nullable)
                                .setComment(comment)
                                .build());
                    }
                    if (columnMapping.isEmpty()) {
                        UnsupportedTypeHandling unsupportedTypeHandling = getUnsupportedTypeHandling(session);
                        verify(
                                unsupportedTypeHandling == IGNORE,
                                "Unsupported type handling is set to %s, but toColumnMapping() returned empty for %s",
                                unsupportedTypeHandling,
                                typeHandle);
                    }
                }
                if (columns.isEmpty()) {
                    // A table may have no supported columns. In rare cases a table might have no columns at all.
                    throw new TableNotFoundException(
                            schemaTableName,
                            format("Table '%s' has no supported columns (all %s columns are not supported)", schemaTableName, allColumns));
                }
                return ImmutableList.copyOf(columns);
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    private static Map<String, Integer> getArrayColumnDimensions(Connection connection, JdbcTableHandle tableHandle)
            throws SQLException
    {
        String sql = "" +
                "SELECT att.attname, greatest(att.attndims, 1) AS attndims " +
                "FROM pg_attribute att " +
                "  JOIN pg_type attyp ON att.atttypid = attyp.oid" +
                "  JOIN pg_class tbl ON tbl.oid = att.attrelid " +
                "  JOIN pg_namespace ns ON tbl.relnamespace = ns.oid " +
                "WHERE ns.nspname = ? " +
                "AND tbl.relname = ? " +
                "AND attyp.typcategory = 'A' ";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            RemoteTableName remoteTableName = tableHandle.getRequiredNamedRelation().getRemoteTableName();
            statement.setString(1, remoteTableName.getSchemaName().orElse(null));
            statement.setString(2, remoteTableName.getTableName());

            Map<String, Integer> arrayColumnDimensions = new HashMap<>();
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    arrayColumnDimensions.put(resultSet.getString("attname"), resultSet.getInt("attndims"));
                }
            }
            return arrayColumnDimensions;
        }
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
        switch (jdbcTypeName) {
            case "money":
                return Optional.of(moneyColumnMapping());
            case "uuid":
                return Optional.of(uuidColumnMapping());
            case "jsonb":
            case "json":
                return Optional.of(jsonColumnMapping());
            case "timestamptz":
                // PostgreSQL's "timestamp with time zone" is reported as Types.TIMESTAMP rather than Types.TIMESTAMP_WITH_TIMEZONE
                int decimalDigits = typeHandle.getRequiredDecimalDigits();
                return Optional.of(timestampWithTimeZoneColumnMapping(decimalDigits));
            case "hstore":
                return Optional.of(hstoreColumnMapping(session));
        }

        switch (typeHandle.getJdbcType()) {
            case Types.BIT:
                return Optional.of(booleanColumnMapping());

            case Types.SMALLINT:
                return Optional.of(smallintColumnMapping());

            case Types.INTEGER:
                return Optional.of(integerColumnMapping());

            case Types.BIGINT:
                return Optional.of(bigintColumnMapping());

            case Types.REAL:
                return Optional.of(realColumnMapping());

            case Types.DOUBLE:
                return Optional.of(doubleColumnMapping());

            case Types.NUMERIC: {
                int columnSize = typeHandle.getRequiredColumnSize();
                int precision;
                int decimalDigits = typeHandle.getDecimalDigits().orElse(0);
                if (getDecimalRounding(session) == ALLOW_OVERFLOW) {
                    if (columnSize == PRECISION_OF_UNSPECIFIED_DECIMAL) {
                        // decimal type with unspecified scale - up to 131072 digits before the decimal point; up to 16383 digits after the decimal point)
                        return Optional.of(decimalColumnMapping(createDecimalType(Decimals.MAX_PRECISION, getDecimalDefaultScale(session)), getDecimalRoundingMode(session)));
                    }
                    precision = columnSize;
                    if (precision > Decimals.MAX_PRECISION) {
                        int scale = min(decimalDigits, getDecimalDefaultScale(session));
                        return Optional.of(decimalColumnMapping(createDecimalType(Decimals.MAX_PRECISION, scale), getDecimalRoundingMode(session)));
                    }
                }
                precision = columnSize + max(-decimalDigits, 0); // Map decimal(p, -s) (negative scale) to decimal(p+s, 0).
                if (columnSize == PRECISION_OF_UNSPECIFIED_DECIMAL || precision > Decimals.MAX_PRECISION) {
                    break;
                }
                return Optional.of(decimalColumnMapping(createDecimalType(precision, max(decimalDigits, 0)), UNNECESSARY));
            }

            case Types.CHAR:
                return Optional.of(charColumnMapping(typeHandle.getRequiredColumnSize()));

            case Types.VARCHAR:
                if (!jdbcTypeName.equals("varchar")) {
                    // This can be e.g. an ENUM
                    return Optional.of(typedVarcharColumnMapping(jdbcTypeName));
                }
                return Optional.of(varcharColumnMapping(typeHandle.getRequiredColumnSize()));

            case Types.BINARY:
                return Optional.of(varbinaryColumnMapping());

            case Types.DATE:
                return Optional.of(dateColumnMappingUsingLocalDate());

            case Types.TIME:
                return Optional.of(timeColumnMapping(typeHandle.getRequiredDecimalDigits()));

            case Types.TIMESTAMP:
                TimestampType timestampType = createTimestampType(typeHandle.getRequiredDecimalDigits());
                return Optional.of(ColumnMapping.longMapping(
                        timestampType,
                        timestampReadFunction(timestampType),
                        PostgreSqlClient::shortTimestampWriteFunction));

            case Types.ARRAY:
                Optional<ColumnMapping> columnMapping = arrayToTrinoType(session, connection, typeHandle);
                if (columnMapping.isPresent()) {
                    return columnMapping;
                }
                break;
        }

        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return mapToUnboundedVarchar(typeHandle);
        }

        return Optional.empty();
    }

    private Optional<ColumnMapping> arrayToTrinoType(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        checkArgument(typeHandle.getJdbcType() == Types.ARRAY, "Not array type");

        ArrayMapping arrayMapping = getArrayMapping(session);
        if (arrayMapping == DISABLED) {
            return Optional.empty();
        }
        // resolve and map base array element type
        JdbcTypeHandle baseElementTypeHandle = getArrayElementTypeHandle(connection, typeHandle);
        String baseElementTypeName = baseElementTypeHandle.getJdbcTypeName()
                .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Element type name is missing: " + baseElementTypeHandle));
        if (baseElementTypeHandle.getJdbcType() == Types.BINARY) {
            // PostgreSQL jdbc driver doesn't currently support array of varbinary (bytea[])
            // https://github.com/pgjdbc/pgjdbc/pull/1184
            return Optional.empty();
        }
        Optional<ColumnMapping> baseElementMapping = toColumnMapping(session, connection, baseElementTypeHandle);

        if (arrayMapping == AS_ARRAY) {
            if (typeHandle.getArrayDimensions().isEmpty()) {
                return Optional.empty();
            }
            return baseElementMapping
                    .map(elementMapping -> {
                        ArrayType trinoArrayType = new ArrayType(elementMapping.getType());
                        ColumnMapping arrayColumnMapping = arrayColumnMapping(session, trinoArrayType, elementMapping, baseElementTypeName);

                        int arrayDimensions = typeHandle.getArrayDimensions().get();
                        for (int i = 1; i < arrayDimensions; i++) {
                            trinoArrayType = new ArrayType(trinoArrayType);
                            arrayColumnMapping = arrayColumnMapping(session, trinoArrayType, arrayColumnMapping, baseElementTypeName);
                        }
                        return arrayColumnMapping;
                    });
        }
        if (arrayMapping == AS_JSON) {
            return baseElementMapping
                    .map(elementMapping -> arrayAsJsonColumnMapping(session, elementMapping));
        }
        throw new IllegalStateException("Unsupported array mapping type: " + arrayMapping);
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type == BOOLEAN) {
            return WriteMapping.booleanMapping("boolean", booleanWriteFunction());
        }

        if (type == TINYINT) {
            // PostgreSQL has no type corresponding to tinyint
            return WriteMapping.longMapping("smallint", tinyintWriteFunction());
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
            return WriteMapping.longMapping("real", realWriteFunction());
        }
        if (type == DOUBLE) {
            return WriteMapping.doubleMapping("double precision", doubleWriteFunction());
        }

        if (type instanceof DecimalType decimalType) {
            String dataType = format("decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.objectMapping(dataType, longDecimalWriteFunction(decimalType));
        }

        if (type instanceof CharType) {
            return WriteMapping.sliceMapping("char(" + ((CharType) type).getLength() + ")", charWriteFunction());
        }

        if (type instanceof VarcharType varcharType) {
            String dataType;
            if (varcharType.isUnbounded()) {
                dataType = "varchar";
            }
            else {
                dataType = "varchar(" + varcharType.getBoundedLength() + ")";
            }
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }
        if (VARBINARY.equals(type)) {
            return WriteMapping.sliceMapping("bytea", varbinaryWriteFunction());
        }

        if (type == DATE) {
            return WriteMapping.longMapping("date", dateWriteFunctionUsingLocalDate());
        }

        if (type instanceof TimeType timeType) {
            if (timeType.getPrecision() <= POSTGRESQL_MAX_SUPPORTED_TIMESTAMP_PRECISION) {
                return WriteMapping.longMapping(format("time(%s)", timeType.getPrecision()), timeWriteFunction(timeType.getPrecision()));
            }
            return WriteMapping.longMapping(format("time(%s)", POSTGRESQL_MAX_SUPPORTED_TIMESTAMP_PRECISION), timeWriteFunction(POSTGRESQL_MAX_SUPPORTED_TIMESTAMP_PRECISION));
        }

        if (type instanceof TimestampType timestampType) {
            if (timestampType.getPrecision() <= POSTGRESQL_MAX_SUPPORTED_TIMESTAMP_PRECISION) {
                verify(timestampType.getPrecision() <= TimestampType.MAX_SHORT_PRECISION);
                return WriteMapping.longMapping(format("timestamp(%s)", timestampType.getPrecision()), PostgreSqlClient::shortTimestampWriteFunction);
            }
            verify(timestampType.getPrecision() > TimestampType.MAX_SHORT_PRECISION);
            return WriteMapping.objectMapping(format("timestamp(%s)", POSTGRESQL_MAX_SUPPORTED_TIMESTAMP_PRECISION), longTimestampWriteFunction());
        }
        if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
            if (timestampWithTimeZoneType.getPrecision() <= POSTGRESQL_MAX_SUPPORTED_TIMESTAMP_PRECISION) {
                String dataType = format("timestamptz(%d)", timestampWithTimeZoneType.getPrecision());
                if (timestampWithTimeZoneType.getPrecision() <= TimestampWithTimeZoneType.MAX_SHORT_PRECISION) {
                    return WriteMapping.longMapping(dataType, shortTimestampWithTimeZoneWriteFunction());
                }
                return WriteMapping.objectMapping(dataType, longTimestampWithTimeZoneWriteFunction());
            }
            return WriteMapping.objectMapping(format("timestamptz(%d)", POSTGRESQL_MAX_SUPPORTED_TIMESTAMP_PRECISION), longTimestampWithTimeZoneWriteFunction());
        }
        if (type.equals(jsonType)) {
            return WriteMapping.sliceMapping("jsonb", typedVarcharWriteFunction("json"));
        }
        if (type.equals(uuidType)) {
            return WriteMapping.sliceMapping("uuid", uuidWriteFunction());
        }
        if (type instanceof ArrayType arrayType && getArrayMapping(session) == AS_ARRAY) {
            Type elementType = arrayType.getElementType();
            String elementDataType = toWriteMapping(session, elementType).getDataType();
            return WriteMapping.objectMapping(elementDataType + "[]", arrayWriteFunction(session, elementType, getArrayElementPgTypeName(session, this, elementType)));
        }

        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
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
        // Postgres sorts textual types differently compared to Trino so we cannot safely pushdown any aggregations which take a text type as an input or as part of grouping set
        return preventTextualTypeAggregationPushdown(groupingSets);
    }

    @Override
    public Optional<ParameterizedExpression> convertPredicate(ConnectorSession session, ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        return connectorExpressionRewriter.rewrite(session, expression, assignments);
    }

    private static Optional<JdbcTypeHandle> toTypeHandle(DecimalType decimalType)
    {
        return Optional.of(new JdbcTypeHandle(Types.NUMERIC, Optional.of("decimal"), Optional.of(decimalType.getPrecision()), Optional.of(decimalType.getScale()), Optional.empty(), Optional.empty()));
    }

    @Override
    public boolean supportsTopN(ConnectorSession session, JdbcTableHandle handle, List<JdbcSortItem> sortOrder)
    {
        for (JdbcSortItem sortItem : sortOrder) {
            Type sortItemType = sortItem.getColumn().getColumnType();
            if (sortItemType instanceof CharType || sortItemType instanceof VarcharType) {
                if (!isCollatable(sortItem.getColumn())) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    protected Optional<TopNFunction> topNFunction()
    {
        return Optional.of((query, sortItems, limit) -> {
            String orderBy = sortItems.stream()
                    .map(sortItem -> {
                        String ordering = sortItem.getSortOrder().isAscending() ? "ASC" : "DESC";
                        String nullsHandling = sortItem.getSortOrder().isNullsFirst() ? "NULLS FIRST" : "NULLS LAST";
                        String collation = "";
                        if (isCollatable(sortItem.getColumn())) {
                            collation = "COLLATE \"C\"";
                        }
                        return format("%s %s %s %s", quoted(sortItem.getColumn().getColumnName()), collation, ordering, nullsHandling);
                    })
                    .collect(joining(", "));
            return format("%s ORDER BY %s LIMIT %d", query, orderBy, limit);
        });
    }

    protected static boolean isCollatable(JdbcColumnHandle column)
    {
        if (column.getColumnType() instanceof CharType || column.getColumnType() instanceof VarcharType) {
            String jdbcTypeName = column.getJdbcTypeHandle().getJdbcTypeName()
                    .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + column.getJdbcTypeHandle()));
            return isCollatable(jdbcTypeName);
        }

        // non-textual types don't have the concept of collation
        return false;
    }

    private static boolean isCollatable(String jdbcTypeName)
    {
        // Only char (internally named bpchar)/varchar/text are the built-in collatable types
        return "bpchar".equals(jdbcTypeName) || "varchar".equals(jdbcTypeName) || "text".equals(jdbcTypeName);
    }

    @Override
    public boolean isTopNGuaranteed(ConnectorSession session)
    {
        return true;
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
        checkArgument(handle.isNamedRelation(), "Unable to delete from synthetic table: %s", handle);
        checkArgument(handle.getLimit().isEmpty(), "Unable to delete when limit is set: %s", handle);
        checkArgument(handle.getSortOrder().isEmpty(), "Unable to delete when sort order is set: %s", handle);
        try (Connection connection = connectionFactory.openConnection(session)) {
            verify(connection.getAutoCommit());
            PreparedQuery preparedQuery = queryBuilder.prepareDeleteQuery(
                    this,
                    session,
                    connection,
                    handle.getRequiredNamedRelation(),
                    handle.getConstraint(),
                    getAdditionalPredicate(handle.getConstraintExpressions(), Optional.empty()));
            try (PreparedStatement preparedStatement = queryBuilder.prepareStatement(this, session, connection, preparedQuery, Optional.empty())) {
                int affectedRowsCount = preparedStatement.executeUpdate();
                // In getPreparedStatement we set autocommit to false so here we need an explicit commit
                connection.commit();
                return OptionalLong.of(affectedRowsCount);
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle)
    {
        if (!statisticsEnabled) {
            return TableStatistics.empty();
        }
        if (!handle.isNamedRelation()) {
            return TableStatistics.empty();
        }
        try {
            return readTableStatistics(session, handle);
        }
        catch (SQLException | RuntimeException e) {
            throwIfInstanceOf(e, TrinoException.class);
            throw new TrinoException(JDBC_ERROR, "Failed fetching statistics for table: " + handle, e);
        }
    }

    private TableStatistics readTableStatistics(ConnectorSession session, JdbcTableHandle table)
            throws SQLException
    {
        checkArgument(table.isNamedRelation(), "Relation is not a table: %s", table);

        try (Connection connection = connectionFactory.openConnection(session);
                Handle handle = Jdbi.open(connection)) {
            StatisticsDao statisticsDao = new StatisticsDao(handle);

            Optional<Long> optionalRowCount = readRowCountTableStat(statisticsDao, table);
            if (optionalRowCount.isEmpty()) {
                // Table not found
                return TableStatistics.empty();
            }
            long rowCount = optionalRowCount.get();
            if (rowCount == -1) {
                // Table has never yet been vacuumed or analyzed
                return TableStatistics.empty();
            }
            TableStatistics.Builder tableStatistics = TableStatistics.builder();
            tableStatistics.setRowCount(Estimate.of(rowCount));

            if (rowCount == 0) {
                return tableStatistics.build();
            }

            RemoteTableName remoteTableName = table.getRequiredNamedRelation().getRemoteTableName();
            Map<String, ColumnStatisticsResult> columnStatistics = statisticsDao.getColumnStatistics(remoteTableName.getSchemaName().orElse(null), remoteTableName.getTableName()).stream()
                    .collect(toImmutableMap(ColumnStatisticsResult::getColumnName, identity()));

            for (JdbcColumnHandle column : this.getColumns(session, table)) {
                ColumnStatisticsResult result = columnStatistics.get(column.getColumnName());
                if (result == null) {
                    continue;
                }

                ColumnStatistics statistics = ColumnStatistics.builder()
                        .setNullsFraction(result.getNullsFraction()
                                .map(Estimate::of)
                                .orElseGet(Estimate::unknown))
                        .setDistinctValuesCount(result.getDistinctValuesIndicator()
                                .map(distinctValuesIndicator -> {
                                    if (distinctValuesIndicator >= 0.0) {
                                        return distinctValuesIndicator;
                                    }
                                    return -distinctValuesIndicator * rowCount;
                                })
                                .map(Estimate::of)
                                .orElseGet(Estimate::unknown))
                        .setDataSize(result.getAverageColumnLength()
                                .flatMap(averageColumnLength ->
                                        result.getNullsFraction().map(nullsFraction ->
                                                Estimate.of(1.0 * averageColumnLength * rowCount * (1 - nullsFraction))))
                                .orElseGet(Estimate::unknown))
                        .build();

                tableStatistics.setColumnStatistics(column, statistics);
            }

            return tableStatistics.build();
        }
    }

    private static Optional<Long> readRowCountTableStat(StatisticsDao statisticsDao, JdbcTableHandle table)
    {
        RemoteTableName remoteTableName = table.getRequiredNamedRelation().getRemoteTableName();
        String schemaName = remoteTableName.getSchemaName().orElse(null);
        Optional<Long> rowCount = statisticsDao.getRowCountFromPgClass(schemaName, remoteTableName.getTableName());
        if (rowCount.isEmpty()) {
            // Table not found
            return Optional.empty();
        }

        if (statisticsDao.isPartitionedTable(schemaName, remoteTableName.getTableName())) {
            Optional<Long> partitionedTableRowCount = statisticsDao.getRowCountPartitionedTableFromPgClass(schemaName, remoteTableName.getTableName());
            if (partitionedTableRowCount.isPresent()) {
                return partitionedTableRowCount;
            }

            return statisticsDao.getRowCountPartitionedTableFromPgStats(schemaName, remoteTableName.getTableName());
        }

        if (rowCount.get() == 0) {
            // `pg_class.reltuples = 0` may mean an empty table or a recently populated table (CTAS, LOAD or INSERT)
            // `pg_stat_all_tables.n_live_tup` can be way off, so we use it only as a fallback
            rowCount = statisticsDao.getRowCountFromPgStat(schemaName, remoteTableName.getTableName());
        }

        return rowCount;
    }

    @Override
    public Optional<PreparedQuery> implementJoin(
            ConnectorSession session,
            JoinType joinType,
            PreparedQuery leftSource,
            PreparedQuery rightSource,
            List<JdbcJoinCondition> joinConditions,
            Map<JdbcColumnHandle, String> rightAssignments,
            Map<JdbcColumnHandle, String> leftAssignments,
            JoinStatistics statistics)
    {
        if (joinType == JoinType.FULL_OUTER) {
            // FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
            return Optional.empty();
        }
        return implementJoinCostAware(
                session,
                joinType,
                leftSource,
                rightSource,
                statistics,
                () -> super.implementJoin(session, joinType, leftSource, rightSource, joinConditions, rightAssignments, leftAssignments, statistics));
    }

    @Override
    protected boolean isSupportedJoinCondition(ConnectorSession session, JdbcJoinCondition joinCondition)
    {
        boolean isVarchar = Stream.of(joinCondition.getLeftColumn(), joinCondition.getRightColumn())
                .map(JdbcColumnHandle::getColumnType)
                .anyMatch(type -> type instanceof CharType || type instanceof VarcharType);
        if (isVarchar) {
            // PostgreSQL is case sensitive by default, but orders varchars differently
            JoinCondition.Operator operator = joinCondition.getOperator();
            switch (operator) {
                case LESS_THAN:
                case LESS_THAN_OR_EQUAL:
                case GREATER_THAN:
                case GREATER_THAN_OR_EQUAL:
                    return isEnableStringPushdownWithCollate(session);
                case EQUAL:
                case NOT_EQUAL:
                case IS_DISTINCT_FROM:
                    return true;
            }
            return false;
        }

        return true;
    }

    @Override
    protected void verifySchemaName(DatabaseMetaData databaseMetadata, String schemaName)
            throws SQLException
    {
        // PostgreSQL truncates schema name to 63 chars silently
        if (schemaName.length() > databaseMetadata.getMaxSchemaNameLength()) {
            throw new TrinoException(NOT_SUPPORTED, format("Schema name must be shorter than or equal to '%s' characters but got '%s'", databaseMetadata.getMaxSchemaNameLength(), schemaName.length()));
        }
    }

    @Override
    protected void verifyTableName(DatabaseMetaData databaseMetadata, String tableName)
            throws SQLException
    {
        // PostgreSQL truncates table name to 63 chars silently
        if (tableName.length() > databaseMetadata.getMaxTableNameLength()) {
            throw new TrinoException(NOT_SUPPORTED, format("Table name must be shorter than or equal to '%s' characters but got '%s'", databaseMetadata.getMaxTableNameLength(), tableName.length()));
        }
    }

    @Override
    protected void verifyColumnName(DatabaseMetaData databaseMetadata, String columnName)
            throws SQLException
    {
        // PostgreSQL truncates table name to 63 chars silently
        // PostgreSQL driver caches the max column name length in a DatabaseMetaData object. The cost to call this method per column is low.
        if (columnName.length() > databaseMetadata.getMaxColumnNameLength()) {
            throw new TrinoException(NOT_SUPPORTED, format("Column name must be shorter than or equal to '%s' characters but got '%s': '%s'", databaseMetadata.getMaxColumnNameLength(), columnName.length(), columnName));
        }
    }

    private static ColumnMapping charColumnMapping(int charLength)
    {
        if (charLength > CharType.MAX_LENGTH) {
            return varcharColumnMapping(charLength);
        }
        CharType charType = createCharType(charLength);
        return ColumnMapping.sliceMapping(
                charType,
                charReadFunction(charType),
                charWriteFunction(),
                POSTGRESQL_STRING_COLLATION_AWARE_PUSHDOWN);
    }

    private static ColumnMapping varcharColumnMapping(int varcharLength)
    {
        VarcharType varcharType = varcharLength <= VarcharType.MAX_LENGTH
                ? createVarcharType(varcharLength)
                : createUnboundedVarcharType();
        return ColumnMapping.sliceMapping(
                varcharType,
                varcharReadFunction(varcharType),
                varcharWriteFunction(),
                POSTGRESQL_STRING_COLLATION_AWARE_PUSHDOWN);
    }

    private static ColumnMapping timeColumnMapping(int precision)
    {
        verify(precision <= 6, "Unsupported precision: %s", precision); // PostgreSQL limit but also assumption within this method
        return ColumnMapping.longMapping(
                createTimeType(precision),
                (resultSet, columnIndex) -> {
                    LocalTime time = resultSet.getObject(columnIndex, LocalTime.class);
                    long nanosOfDay = time.toNanoOfDay();
                    if (nanosOfDay == NANOSECONDS_PER_DAY - 1) {
                        // PostgreSQL's 24:00:00 is returned as 23:59:59.999999999, regardless of column precision
                        nanosOfDay = NANOSECONDS_PER_DAY - LongMath.pow(10, 9 - precision);
                    }

                    long picosOfDay = nanosOfDay * PICOSECONDS_PER_NANOSECOND;
                    return round(picosOfDay, 12 - precision);
                },
                timeWriteFunction(precision),
                // Pushdown disabled because PostgreSQL distinguishes TIME '24:00:00' and TIME '00:00:00' whereas Trino does not.
                DISABLE_PUSHDOWN);
    }

    private static LongWriteFunction timeWriteFunction(int precision)
    {
        checkArgument(precision <= 6, "Unsupported precision: %s", precision); // PostgreSQL limit but also assumption within this method
        String bindExpression = format("CAST(? AS time(%s))", precision);
        return new LongWriteFunction()
        {
            @Override
            public String getBindExpression()
            {
                return bindExpression;
            }

            @Override
            public void set(PreparedStatement statement, int index, long picosOfDay)
                    throws SQLException
            {
                picosOfDay = round(picosOfDay, 12 - precision);
                if (picosOfDay == PICOSECONDS_PER_DAY) {
                    picosOfDay = 0;
                }
                LocalTime localTime = LocalTime.ofNanoOfDay(picosOfDay / PICOSECONDS_PER_NANOSECOND);
                // statement.setObject(.., localTime) would yield incorrect end result for 23:59:59.999000
                statement.setString(index, TIME_FORMATTER.format(localTime));
            }
        };
    }

    // When writing with setObject() using LocalDateTime, driver converts the value to string representing date-time in JVM zone,
    // therefore cannot represent local date-time which is a "gap" in this zone.
    // TODO replace this method with StandardColumnMappings#timestampWriteFunction when https://github.com/pgjdbc/pgjdbc/issues/1390 is done
    private static void shortTimestampWriteFunction(PreparedStatement statement, int index, long epochMicros)
            throws SQLException
    {
        LocalDateTime localDateTime = fromTrinoTimestamp(epochMicros);
        statement.setObject(index, toPgTimestamp(localDateTime));
    }

    private static ObjectWriteFunction longTimestampWriteFunction()
    {
        return ObjectWriteFunction.of(LongTimestamp.class, ((statement, index, timestamp) -> {
            // PostgreSQL supports up to 6 digits of precision
            //noinspection ConstantConditions
            verify(POSTGRESQL_MAX_SUPPORTED_TIMESTAMP_PRECISION == 6);

            long epochMicros = timestamp.getEpochMicros();
            if (timestamp.getPicosOfMicro() >= PICOSECONDS_PER_MICROSECOND / 2) {
                epochMicros++;
            }
            shortTimestampWriteFunction(statement, index, epochMicros);
        }));
    }

    @Override
    public void setColumnComment(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Optional<String> comment)
    {
        // PostgreSQL doesn't support prepared statement for COMMENT statement
        String sql = format(
                "COMMENT ON COLUMN %s.%s IS %s",
                quoted(handle.asPlainTable().getRemoteTableName()),
                quoted(column.getColumnName()),
                comment.map(BaseJdbcClient::varcharLiteral).orElse("NULL"));
        execute(session, sql);
    }

    private static ColumnMapping timestampWithTimeZoneColumnMapping(int precision)
    {
        // PostgreSQL supports timestamptz precision up to microseconds
        checkArgument(precision <= POSTGRESQL_MAX_SUPPORTED_TIMESTAMP_PRECISION, "unsupported precision value %s", precision);
        TimestampWithTimeZoneType trinoType = createTimestampWithTimeZoneType(precision);
        if (precision <= TimestampWithTimeZoneType.MAX_SHORT_PRECISION) {
            return ColumnMapping.longMapping(
                    trinoType,
                    shortTimestampWithTimeZoneReadFunction(),
                    shortTimestampWithTimeZoneWriteFunction());
        }
        return ColumnMapping.objectMapping(
                trinoType,
                longTimestampWithTimeZoneReadFunction(),
                longTimestampWithTimeZoneWriteFunction());
    }

    private static LongReadFunction shortTimestampWithTimeZoneReadFunction()
    {
        return (resultSet, columnIndex) -> {
            // PostgreSQL does not store zone information in "timestamp with time zone" data type
            long millisUtc = resultSet.getTimestamp(columnIndex).getTime();
            return packDateTimeWithZone(millisUtc, UTC_KEY);
        };
    }

    private static LongWriteFunction shortTimestampWithTimeZoneWriteFunction()
    {
        return (statement, index, value) -> {
            // PostgreSQL does not store zone information in "timestamp with time zone" data type
            long millisUtc = unpackMillisUtc(value);
            statement.setTimestamp(index, new Timestamp(millisUtc));
        };
    }

    private static ObjectReadFunction longTimestampWithTimeZoneReadFunction()
    {
        return ObjectReadFunction.of(
                LongTimestampWithTimeZone.class,
                (resultSet, columnIndex) -> {
                    // PostgreSQL does not store zone information in "timestamp with time zone" data type
                    OffsetDateTime offsetDateTime = resultSet.getObject(columnIndex, OffsetDateTime.class);
                    return LongTimestampWithTimeZone.fromEpochSecondsAndFraction(
                            offsetDateTime.toEpochSecond(),
                            (long) offsetDateTime.getNano() * PICOSECONDS_PER_NANOSECOND,
                            UTC_KEY);
                });
    }

    private static ObjectWriteFunction longTimestampWithTimeZoneWriteFunction()
    {
        return ObjectWriteFunction.of(
                LongTimestampWithTimeZone.class,
                (statement, index, value) -> {
                    // PostgreSQL does not store zone information in "timestamp with time zone" data type
                    long epochSeconds = floorDiv(value.getEpochMillis(), MILLISECONDS_PER_SECOND);
                    long nanosOfSecond = floorMod(value.getEpochMillis(), MILLISECONDS_PER_SECOND) * NANOSECONDS_PER_MILLISECOND + value.getPicosOfMilli() / PICOSECONDS_PER_NANOSECOND;
                    statement.setObject(index, OffsetDateTime.ofInstant(Instant.ofEpochSecond(epochSeconds, nanosOfSecond), UTC_KEY.getZoneId()));
                });
    }

    private ColumnMapping hstoreColumnMapping(ConnectorSession session)
    {
        return ColumnMapping.objectMapping(
                varcharMapType,
                varcharMapReadFunction(),
                hstoreWriteFunction(session),
                DISABLE_PUSHDOWN);
    }

    private ObjectReadFunction varcharMapReadFunction()
    {
        return ObjectReadFunction.of(Block.class, (resultSet, columnIndex) -> {
            @SuppressWarnings("unchecked")
            Map<String, String> map = (Map<String, String>) resultSet.getObject(columnIndex);
            BlockBuilder keyBlockBuilder = varcharMapType.getKeyType().createBlockBuilder(null, map.size());
            BlockBuilder valueBlockBuilder = varcharMapType.getValueType().createBlockBuilder(null, map.size());
            for (Map.Entry<String, String> entry : map.entrySet()) {
                if (entry.getKey() == null) {
                    throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "hstore key is null");
                }
                varcharMapType.getKeyType().writeSlice(keyBlockBuilder, utf8Slice(entry.getKey()));
                if (entry.getValue() == null) {
                    valueBlockBuilder.appendNull();
                }
                else {
                    varcharMapType.getValueType().writeSlice(valueBlockBuilder, utf8Slice(entry.getValue()));
                }
            }
            return varcharMapType.createBlockFromKeyValue(Optional.empty(), new int[] {0, map.size()}, keyBlockBuilder.build(), valueBlockBuilder.build())
                    .getObject(0, Block.class);
        });
    }

    private ObjectWriteFunction hstoreWriteFunction(ConnectorSession session)
    {
        return ObjectWriteFunction.of(Block.class, (statement, index, block) -> {
            checkArgument(block instanceof SingleMapBlock, "wrong block type: %s. expected SingleMapBlock", block.getClass().getSimpleName());
            Map<Object, Object> map = new HashMap<>();
            for (int i = 0; i < block.getPositionCount(); i += 2) {
                map.put(varcharMapType.getKeyType().getObjectValue(session, block, i), varcharMapType.getValueType().getObjectValue(session, block, i + 1));
            }
            statement.setObject(index, Collections.unmodifiableMap(map));
        });
    }

    private static ColumnMapping arrayColumnMapping(ConnectorSession session, ArrayType arrayType, ColumnMapping arrayElementMapping, String baseElementJdbcTypeName)
    {
        return ColumnMapping.objectMapping(
                arrayType,
                arrayReadFunction(arrayType.getElementType(), arrayElementMapping.getReadFunction()),
                arrayWriteFunction(session, arrayType.getElementType(), baseElementJdbcTypeName));
    }

    private static ObjectReadFunction arrayReadFunction(Type elementType, ReadFunction elementReadFunction)
    {
        return ObjectReadFunction.of(Block.class, (resultSet, columnIndex) -> {
            Array array = resultSet.getArray(columnIndex);
            BlockBuilder builder = elementType.createBlockBuilder(null, 10);
            try (ResultSet arrayAsResultSet = array.getResultSet()) {
                while (arrayAsResultSet.next()) {
                    if (elementReadFunction.isNull(arrayAsResultSet, ARRAY_RESULT_SET_VALUE_COLUMN)) {
                        builder.appendNull();
                    }
                    else if (elementType.getJavaType() == boolean.class) {
                        elementType.writeBoolean(builder, ((BooleanReadFunction) elementReadFunction).readBoolean(arrayAsResultSet, ARRAY_RESULT_SET_VALUE_COLUMN));
                    }
                    else if (elementType.getJavaType() == long.class) {
                        elementType.writeLong(builder, ((LongReadFunction) elementReadFunction).readLong(arrayAsResultSet, ARRAY_RESULT_SET_VALUE_COLUMN));
                    }
                    else if (elementType.getJavaType() == double.class) {
                        elementType.writeDouble(builder, ((DoubleReadFunction) elementReadFunction).readDouble(arrayAsResultSet, ARRAY_RESULT_SET_VALUE_COLUMN));
                    }
                    else if (elementType.getJavaType() == Slice.class) {
                        elementType.writeSlice(builder, ((SliceReadFunction) elementReadFunction).readSlice(arrayAsResultSet, ARRAY_RESULT_SET_VALUE_COLUMN));
                    }
                    else {
                        elementType.writeObject(builder, ((ObjectReadFunction) elementReadFunction).readObject(arrayAsResultSet, ARRAY_RESULT_SET_VALUE_COLUMN));
                    }
                }
            }

            return builder.build();
        });
    }

    private static ObjectWriteFunction arrayWriteFunction(ConnectorSession session, Type elementType, String baseElementJdbcTypeName)
    {
        return ObjectWriteFunction.of(Block.class, (statement, index, block) -> {
            Array jdbcArray = statement.getConnection().createArrayOf(baseElementJdbcTypeName, getJdbcObjectArray(session, elementType, block));
            statement.setArray(index, jdbcArray);
        });
    }

    private ColumnMapping arrayAsJsonColumnMapping(ConnectorSession session, ColumnMapping baseElementMapping)
    {
        return ColumnMapping.sliceMapping(
                jsonType,
                arrayAsJsonReadFunction(session, baseElementMapping),
                (statement, index, block) -> { throw new UnsupportedOperationException(); },
                DISABLE_PUSHDOWN);
    }

    private static SliceReadFunction arrayAsJsonReadFunction(ConnectorSession session, ColumnMapping baseElementMapping)
    {
        return (resultSet, columnIndex) -> {
            // resolve array type
            Object jdbcArray = resultSet.getArray(columnIndex).getArray();
            int arrayDimensions = arrayDepth(jdbcArray);

            ReadFunction readFunction = baseElementMapping.getReadFunction();
            Type type = baseElementMapping.getType();
            for (int i = 0; i < arrayDimensions; i++) {
                readFunction = arrayReadFunction(type, readFunction);
                type = new ArrayType(type);
            }

            // read array into a block
            Block block = (Block) ((ObjectReadFunction) readFunction).readObject(resultSet, columnIndex);

            // convert block to JSON slice
            BlockBuilder builder = type.createBlockBuilder(null, 1);
            type.writeObject(builder, block);
            Object value = type.getObjectValue(session, builder.build(), 0);

            try {
                return toJsonValue(value);
            }
            catch (IOException e) {
                throw new TrinoException(JDBC_ERROR, "Conversion to JSON failed for  " + type.getDisplayName(), e);
            }
        };
    }

    private static JdbcTypeHandle getArrayElementTypeHandle(Connection connection, JdbcTypeHandle arrayTypeHandle)
    {
        String jdbcTypeName = arrayTypeHandle.getJdbcTypeName()
                .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + arrayTypeHandle));
        try {
            TypeInfo typeInfo = connection.unwrap(PgConnection.class).getTypeInfo();
            int pgElementOid = typeInfo.getPGArrayElement(typeInfo.getPGType(jdbcTypeName));
            verify(arrayTypeHandle.getCaseSensitivity().isEmpty(), "Case sensitivity not supported");
            return new JdbcTypeHandle(
                    typeInfo.getSQLType(pgElementOid),
                    Optional.of(typeInfo.getPGType(pgElementOid)),
                    arrayTypeHandle.getColumnSize(),
                    arrayTypeHandle.getDecimalDigits(),
                    arrayTypeHandle.getArrayDimensions(),
                    Optional.empty());
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    private ColumnMapping jsonColumnMapping()
    {
        return ColumnMapping.sliceMapping(
                jsonType,
                (resultSet, columnIndex) -> jsonParse(utf8Slice(resultSet.getString(columnIndex))),
                typedVarcharWriteFunction("json"),
                DISABLE_PUSHDOWN);
    }

    private static ColumnMapping typedVarcharColumnMapping(String jdbcTypeName)
    {
        return ColumnMapping.sliceMapping(
                VARCHAR,
                (resultSet, columnIndex) -> utf8Slice(resultSet.getString(columnIndex)),
                typedVarcharWriteFunction(jdbcTypeName),
                POSTGRESQL_STRING_COLLATION_AWARE_PUSHDOWN);
    }

    private static SliceWriteFunction typedVarcharWriteFunction(String jdbcTypeName)
    {
        String bindExpression = format("CAST(? AS %s)", requireNonNull(jdbcTypeName, "jdbcTypeName is null"));

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
                statement.setString(index, value.toStringUtf8());
            }
        };
    }

    private static ColumnMapping moneyColumnMapping()
    {
        /*
         * PostgreSQL JDBC maps "money" to Types.DOUBLE, but fails to retrieve double value for amounts
         * greater than or equal to 1000. Upon `ResultSet#getString`, the driver returns e.g. "$10.00" or "$10,000.00"
         * (currency symbol depends on the server side configuration).
         *
         * The following mapping maps PostgreSQL "money" to Trino "varchar".
         * Writing is disabled for simplicity.
         *
         * Money mapping can be improved when PostgreSQL JDBC gains explicit money type support.
         * See https://github.com/pgjdbc/pgjdbc/issues/425 for more information.
         */
        return ColumnMapping.sliceMapping(
                VARCHAR,
                new SliceReadFunction()
                {
                    @Override
                    public boolean isNull(ResultSet resultSet, int columnIndex)
                            throws SQLException
                    {
                        // super calls ResultSet#getObject(), which for money type calls .getDouble and the call may fail to parse the money value.
                        resultSet.getString(columnIndex);
                        return resultSet.wasNull();
                    }

                    @Override
                    public Slice readSlice(ResultSet resultSet, int columnIndex)
                            throws SQLException
                    {
                        return utf8Slice(resultSet.getString(columnIndex));
                    }
                },
                (statement, index, value) -> { throw new TrinoException(NOT_SUPPORTED, "Money type is not supported for INSERT"); },
                DISABLE_PUSHDOWN);
    }

    private static SliceWriteFunction uuidWriteFunction()
    {
        return (statement, index, value) -> statement.setObject(index, trinoUuidToJavaUuid(value), Types.OTHER);
    }

    private ColumnMapping uuidColumnMapping()
    {
        return ColumnMapping.sliceMapping(
                uuidType,
                (resultSet, columnIndex) -> javaUuidToTrinoUuid((UUID) resultSet.getObject(columnIndex)),
                uuidWriteFunction());
    }

    private static class StatisticsDao
    {
        private final Handle handle;

        public StatisticsDao(Handle handle)
        {
            this.handle = requireNonNull(handle, "handle is null");
        }

        Optional<Long> getRowCountFromPgClass(String schema, String tableName)
        {
            return handle.createQuery("" +
                            "SELECT reltuples " +
                            "FROM pg_class " +
                            "WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = :schema) " +
                            "AND relname = :table_name")
                    .bind("schema", schema)
                    .bind("table_name", tableName)
                    .mapTo(Long.class)
                    .findOne();
        }

        Optional<Long> getRowCountFromPgStat(String schema, String tableName)
        {
            return handle.createQuery("SELECT n_live_tup FROM pg_stat_all_tables WHERE schemaname = :schema AND relname = :table_name")
                    .bind("schema", schema)
                    .bind("table_name", tableName)
                    .mapTo(Long.class)
                    .findOne();
        }

        Optional<Long> getRowCountPartitionedTableFromPgClass(String schema, String tableName)
        {
            return handle.createQuery("" +
                            "SELECT SUM(child.reltuples) " +
                            "FROM pg_inherits " +
                            "JOIN pg_class parent ON pg_inherits.inhparent = parent.oid " +
                            "JOIN pg_class child ON pg_inherits.inhrelid = child.oid " +
                            "JOIN pg_namespace parent_ns ON parent_ns.oid = parent.relnamespace " +
                            "JOIN pg_namespace child_ns ON child_ns.oid = child.relnamespace " +
                            "WHERE parent.oid = :schema_table_name::regclass")
                    .bind("schema_table_name", format("%s.%s", schema, tableName))
                    .mapTo(Long.class)
                    .findOne();
        }

        Optional<Long> getRowCountPartitionedTableFromPgStats(String schema, String tableName)
        {
            return handle.createQuery("" +
                            "SELECT SUM(stat.n_live_tup) " +
                            "FROM pg_inherits " +
                            "JOIN pg_class parent ON pg_inherits.inhparent = parent.oid " +
                            "JOIN pg_class child ON pg_inherits.inhrelid = child.oid " +
                            "JOIN pg_namespace parent_ns ON parent_ns.oid = parent.relnamespace " +
                            "JOIN pg_namespace child_ns ON child_ns.oid = child.relnamespace " +
                            "JOIN pg_stat_all_tables stat ON stat.schemaname = child_ns.nspname AND stat.relname = child.relname " +
                            "WHERE parent.oid = :schema_table_name::regclass")
                    .bind("schema_table_name", format("%s.%s", schema, tableName))
                    .mapTo(Long.class)
                    .findOne();
        }

        List<ColumnStatisticsResult> getColumnStatistics(String schema, String tableName)
        {
            return handle.createQuery("SELECT attname, null_frac, n_distinct, avg_width FROM pg_stats WHERE schemaname = :schema AND tablename = :table_name")
                    .bind("schema", schema)
                    .bind("table_name", tableName)
                    .map((rs, ctx) -> new ColumnStatisticsResult(
                            requireNonNull(rs.getString("attname"), "attname is null"),
                            Optional.ofNullable(rs.getObject("null_frac", Float.class)),
                            Optional.ofNullable(rs.getObject("n_distinct", Float.class)),
                            Optional.ofNullable(rs.getObject("avg_width", Integer.class))))
                    .list();
        }

        boolean isPartitionedTable(String schema, String tableName)
        {
            return handle.createQuery("" +
                            "SELECT true " +
                            "FROM pg_class " +
                            "WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = :schema) " +
                            "AND relname = :table_name " +
                            "AND relkind = 'p'")
                    .bind("schema", schema)
                    .bind("table_name", tableName)
                    .mapTo(Boolean.class)
                    .findOne()
                    .orElse(false);
        }
    }

    private static class ColumnStatisticsResult
    {
        private final String columnName;
        private final Optional<Float> nullsFraction;
        private final Optional<Float> distinctValuesIndicator;
        private final Optional<Integer> averageColumnLength;

        public ColumnStatisticsResult(String columnName, Optional<Float> nullsFraction, Optional<Float> distinctValuesIndicator, Optional<Integer> averageColumnLength)
        {
            this.columnName = columnName;
            this.nullsFraction = nullsFraction;
            this.distinctValuesIndicator = distinctValuesIndicator;
            this.averageColumnLength = averageColumnLength;
        }

        public String getColumnName()
        {
            return columnName;
        }

        public Optional<Float> getNullsFraction()
        {
            return nullsFraction;
        }

        public Optional<Float> getDistinctValuesIndicator()
        {
            return distinctValuesIndicator;
        }

        public Optional<Integer> getAverageColumnLength()
        {
            return averageColumnLength;
        }
    }
}
