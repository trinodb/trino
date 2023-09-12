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
package io.trino.plugin.redshift;

import com.amazon.redshift.jdbc.RedshiftPreparedStatement;
import com.amazon.redshift.util.RedshiftObject;
import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.trino.plugin.base.aggregation.AggregateFunctionRewriter;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcJoinCondition;
import io.trino.plugin.jdbc.JdbcSortItem;
import io.trino.plugin.jdbc.JdbcSplit;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.ObjectReadFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.SliceWriteFunction;
import io.trino.plugin.jdbc.StandardColumnMappings;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.aggregation.ImplementAvgFloatingPoint;
import io.trino.plugin.jdbc.aggregation.ImplementCount;
import io.trino.plugin.jdbc.aggregation.ImplementCountAll;
import io.trino.plugin.jdbc.aggregation.ImplementCountDistinct;
import io.trino.plugin.jdbc.aggregation.ImplementMinMax;
import io.trino.plugin.jdbc.aggregation.ImplementStddevPop;
import io.trino.plugin.jdbc.aggregation.ImplementStddevSamp;
import io.trino.plugin.jdbc.aggregation.ImplementSum;
import io.trino.plugin.jdbc.aggregation.ImplementVariancePop;
import io.trino.plugin.jdbc.aggregation.ImplementVarianceSamp;
import io.trino.plugin.jdbc.expression.JdbcConnectorExpressionRewriterBuilder;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.JoinCondition;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Chars;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_NON_TRANSIENT_ERROR;
import static io.trino.plugin.jdbc.JdbcJoinPushdownUtil.implementJoinCostAware;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charReadFunction;
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
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.realColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timeColumnMappingUsingSqlTime;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampColumnMappingUsingSqlTimestampWithRounding;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.LongTimestampWithTimeZone.fromEpochSecondsAndFraction;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.roundDiv;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.math.RoundingMode.UNNECESSARY;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class RedshiftClient
        extends BaseJdbcClient
{
    /**
     * Redshift does not handle values larger than 64 bits for
     * {@code DECIMAL(19, s)}. It supports the full range of values for all
     * other precisions.
     *
     * @see <a href="https://docs.aws.amazon.com/redshift/latest/dg/r_Numeric_types201.html#r_Numeric_types201-decimal-or-numeric-type">
     * Redshift documentation</a>
     */
    private static final int REDSHIFT_DECIMAL_CUTOFF_PRECISION = 19;

    static final int REDSHIFT_MAX_DECIMAL_PRECISION = 38;

    /**
     * Maximum size of a {@link BigInteger} storing a Redshift {@code DECIMAL}
     * with precision {@link #REDSHIFT_DECIMAL_CUTOFF_PRECISION}.
     */
    // actual value is 63
    private static final int REDSHIFT_DECIMAL_CUTOFF_BITS = BigInteger.valueOf(Long.MAX_VALUE).bitLength();

    /**
     * Maximum size of a Redshift CHAR column.
     *
     * @see <a href="https://docs.aws.amazon.com/redshift/latest/dg/r_Character_types.html">
     * Redshift documentation</a>
     */
    private static final int REDSHIFT_MAX_CHAR = 4096;

    /**
     * Maximum size of a Redshift VARCHAR column.
     *
     * @see <a href="https://docs.aws.amazon.com/redshift/latest/dg/r_Character_types.html">
     * Redshift documentation</a>
     */
    static final int REDSHIFT_MAX_VARCHAR = 65535;

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyy-MM-dd[ G]");
    private static final DateTimeFormatter DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("yyy-MM-dd HH:mm:ss")
            .optionalStart()
            .appendFraction(NANO_OF_SECOND, 0, 6, true)
            .optionalEnd()
            .appendPattern("[ G]")
            .toFormatter();
    private static final OffsetDateTime REDSHIFT_MIN_SUPPORTED_TIMESTAMP_TZ = OffsetDateTime.of(-4712, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);

    private final AggregateFunctionRewriter<JdbcExpression, ?> aggregateFunctionRewriter;
    private final boolean statisticsEnabled;
    private final RedshiftTableStatisticsReader statisticsReader;
    private final boolean legacyTypeMapping;

    @Inject
    public RedshiftClient(
            BaseJdbcConfig config,
            ConnectionFactory connectionFactory,
            JdbcStatisticsConfig statisticsConfig,
            QueryBuilder queryBuilder,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier,
            RedshiftConfig redshiftConfig)
    {
        super("\"", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, queryModifier, true);
        this.legacyTypeMapping = redshiftConfig.isLegacyTypeMapping();
        ConnectorExpressionRewriter<ParameterizedExpression> connectorExpressionRewriter = JdbcConnectorExpressionRewriterBuilder.newBuilder()
                .addStandardRules(this::quoted)
                .build();

        JdbcTypeHandle bigintTypeHandle = new JdbcTypeHandle(Types.BIGINT, Optional.of("bigint"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());

        aggregateFunctionRewriter = new AggregateFunctionRewriter<>(
                connectorExpressionRewriter,
                ImmutableSet.<AggregateFunctionRule<JdbcExpression, ParameterizedExpression>>builder()
                        .add(new ImplementCountAll(bigintTypeHandle))
                        .add(new ImplementCount(bigintTypeHandle))
                        .add(new ImplementCountDistinct(bigintTypeHandle, true))
                        .add(new ImplementMinMax(true))
                        .add(new ImplementSum(RedshiftClient::toTypeHandle))
                        .add(new ImplementAvgFloatingPoint())
                        .add(new ImplementRedshiftAvgDecimal())
                        .add(new ImplementRedshiftAvgBigint())
                        .add(new ImplementStddevSamp())
                        .add(new ImplementStddevPop())
                        .add(new ImplementVarianceSamp())
                        .add(new ImplementVariancePop())
                        .build());

        this.statisticsEnabled = requireNonNull(statisticsConfig, "statisticsConfig is null").isEnabled();
        this.statisticsReader = new RedshiftTableStatisticsReader(connectionFactory);
    }

    private static Optional<JdbcTypeHandle> toTypeHandle(DecimalType decimalType)
    {
        return Optional.of(
                new JdbcTypeHandle(
                        Types.NUMERIC,
                        Optional.of("decimal"),
                        Optional.of(decimalType.getPrecision()),
                        Optional.of(decimalType.getScale()),
                        Optional.empty(),
                        Optional.empty()));
    }

    @Override
    public Connection getConnection(ConnectorSession session, JdbcSplit split, JdbcTableHandle tableHandle)
            throws SQLException
    {
        Connection connection = super.getConnection(session, split, tableHandle);
        try {
            // super.getConnection sets read-only, since the connection is going to be used only for reads.
            // However, for a complex query, Redshift may decide to create some temporary tables behind
            // the scenes, and this requires the connection not to be read-only, otherwise Redshift
            // may fail with "ERROR: transaction is read-only".
            connection.setReadOnly(false);
        }
        catch (SQLException e) {
            connection.close();
            throw e;
        }
        return connection;
    }

    @Override
    protected void dropSchema(ConnectorSession session, Connection connection, String remoteSchemaName, boolean cascade)
            throws SQLException
    {
        if (cascade) {
            // Dropping schema with cascade option may lead to other metadata listing operations. Disable until finding the solution.
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping schemas with CASCADE option");
        }
        execute(session, connection, "DROP SCHEMA " + quoted(remoteSchemaName));
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

    private String buildTableCommentSql(RemoteTableName remoteTableName, Optional<String> tableComment)
    {
        return format(
                "COMMENT ON TABLE %s IS %s",
                quoted(remoteTableName),
                tableComment.map(RedshiftClient::redshiftVarcharLiteral).orElse("NULL"));
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        return aggregateFunctionRewriter.rewrite(session, aggregate, assignments);
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle, TupleDomain<ColumnHandle> tupleDomain)
    {
        if (!statisticsEnabled) {
            return TableStatistics.empty();
        }
        if (!handle.isNamedRelation()) {
            return TableStatistics.empty();
        }
        try {
            return statisticsReader.readTableStatistics(session, handle, () -> this.getColumns(session, handle));
        }
        catch (SQLException | RuntimeException e) {
            throwIfInstanceOf(e, TrinoException.class);
            throw new TrinoException(JDBC_ERROR, "Failed fetching statistics for table: " + handle, e);
        }
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
            String orderBy = sortItems.stream()
                    .map(sortItem -> {
                        String ordering = sortItem.getSortOrder().isAscending() ? "ASC" : "DESC";
                        String nullsHandling = sortItem.getSortOrder().isNullsFirst() ? "NULLS FIRST" : "NULLS LAST";
                        return format("%s %s %s", quoted(sortItem.getColumn().getColumnName()), ordering, nullsHandling);
                    })
                    .collect(joining(", "));

            return format("%s ORDER BY %s LIMIT %d", query, orderBy, limit);
        });
    }

    @Override
    public boolean isTopNGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    protected boolean isSupportedJoinCondition(ConnectorSession session, JdbcJoinCondition joinCondition)
    {
        return joinCondition.getOperator() != JoinCondition.Operator.IS_DISTINCT_FROM;
    }

    @Override
    public Optional<PreparedQuery> implementJoin(ConnectorSession session,
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
        // In PostgreSQL, fetch-size is ignored when connection is in auto-commit. Redshift JDBC documentation does not state this requirement
        // but it still links to https://jdbc.postgresql.org/documentation/head/query.html#query-with-cursor for more information, which states
        // that.
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
    public OptionalLong delete(ConnectorSession session, JdbcTableHandle handle)
    {
        checkArgument(handle.isNamedRelation(), "Unable to delete from synthetic table: %s", handle);
        checkArgument(handle.getLimit().isEmpty(), "Unable to delete when limit is set: %s", handle);
        checkArgument(handle.getSortOrder().isEmpty(), "Unable to delete when sort order is set: %s", handle);
        try (Connection connection = connectionFactory.openConnection(session)) {
            verify(connection.getAutoCommit());
            PreparedQuery preparedQuery = queryBuilder.prepareDeleteQuery(this, session, connection, handle.getRequiredNamedRelation(), handle.getConstraint(), Optional.empty());
            try (PreparedStatement preparedStatement = queryBuilder.prepareStatement(this, session, connection, preparedQuery, Optional.empty())) {
                int affectedRowsCount = preparedStatement.executeUpdate();
                // connection.getAutoCommit() == true is not enough to make DELETE effective and explicit commit is required
                connection.commit();
                return OptionalLong.of(affectedRowsCount);
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected void addColumn(ConnectorSession session, Connection connection, RemoteTableName table, ColumnMetadata column)
            throws SQLException
    {
        if (!column.isNullable()) {
            // Redshift doesn't support adding not null columns without default expression
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support adding not null columns");
        }
        super.addColumn(session, connection, table, column);
    }

    @Override
    protected void verifySchemaName(DatabaseMetaData databaseMetadata, String schemaName)
            throws SQLException
    {
        // Redshift truncates schema name to 127 chars silently
        if (schemaName.length() > databaseMetadata.getMaxSchemaNameLength()) {
            throw new TrinoException(NOT_SUPPORTED, "Schema name must be shorter than or equal to '%d' characters but got '%d'".formatted(databaseMetadata.getMaxSchemaNameLength(), schemaName.length()));
        }
    }

    @Override
    protected void verifyTableName(DatabaseMetaData databaseMetadata, String tableName)
            throws SQLException
    {
        // Redshift truncates table name to 127 chars silently
        if (tableName.length() > databaseMetadata.getMaxTableNameLength()) {
            throw new TrinoException(NOT_SUPPORTED, "Table name must be shorter than or equal to '%d' characters but got '%d'".formatted(databaseMetadata.getMaxTableNameLength(), tableName.length()));
        }
    }

    @Override
    protected void verifyColumnName(DatabaseMetaData databaseMetadata, String columnName)
            throws SQLException
    {
        // Redshift truncates table name to 127 chars silently
        if (columnName.length() > databaseMetadata.getMaxColumnNameLength()) {
            throw new TrinoException(NOT_SUPPORTED, "Column name must be shorter than or equal to '%d' characters but got '%d'".formatted(databaseMetadata.getMaxColumnNameLength(), columnName.length()));
        }
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle type)
    {
        // todo remove this when legacy type mapping is no longer supported
        if (legacyTypeMapping) {
            return legacyToColumnMapping(session, type);
        }

        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(type);
        if (mapping.isPresent()) {
            return mapping;
        }

        if ("time".equals(type.getJdbcTypeName().orElse(""))) {
            return Optional.of(ColumnMapping.longMapping(
                    TIME_MICROS,
                    RedshiftClient::readTime,
                    RedshiftClient::writeTime));
        }

        switch (type.getJdbcType()) {
            case Types.BIT: // Redshift uses this for booleans
                return Optional.of(booleanColumnMapping());

            // case Types.TINYINT: -- Redshift doesn't support tinyint
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
                int precision = type.getRequiredColumnSize();
                int scale = type.getRequiredDecimalDigits();
                DecimalType decimalType = createDecimalType(precision, scale);
                if (precision == REDSHIFT_DECIMAL_CUTOFF_PRECISION) {
                    return Optional.of(ColumnMapping.objectMapping(
                            decimalType,
                            longDecimalReadFunction(decimalType),
                            writeDecimalAtRedshiftCutoff(scale)));
                }
                return Optional.of(decimalColumnMapping(decimalType, UNNECESSARY));
            }

            case Types.CHAR:
                CharType charType = createCharType(type.getRequiredColumnSize());
                return Optional.of(ColumnMapping.sliceMapping(
                        charType,
                        charReadFunction(charType),
                        RedshiftClient::writeChar));

            case Types.VARCHAR: {
                int length = type.getRequiredColumnSize();
                return Optional.of(varcharColumnMapping(
                        length < VarcharType.MAX_LENGTH
                                ? createVarcharType(length)
                                : createUnboundedVarcharType(),
                        true));
            }

            case Types.LONGVARBINARY:
                return Optional.of(ColumnMapping.sliceMapping(
                        VARBINARY,
                        varbinaryReadFunction(),
                        varbinaryWriteFunction()));

            case Types.DATE:
                return Optional.of(ColumnMapping.longMapping(
                        DATE,
                        RedshiftClient::readDate,
                        RedshiftClient::writeDate));

            case Types.TIMESTAMP:
                return Optional.of(ColumnMapping.longMapping(
                        TIMESTAMP_MICROS,
                        RedshiftClient::readTimestamp,
                        RedshiftClient::writeShortTimestamp));

            case Types.TIMESTAMP_WITH_TIMEZONE:
                return Optional.of(ColumnMapping.objectMapping(
                        TIMESTAMP_TZ_MICROS,
                        longTimestampWithTimeZoneReadFunction(),
                        longTimestampWithTimeZoneWriteFunction()));
        }

        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return mapToUnboundedVarchar(type);
        }
        return Optional.empty();
    }

    private Optional<ColumnMapping> legacyToColumnMapping(ConnectorSession session, JdbcTypeHandle typeHandle)
    {
        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }
        Optional<ColumnMapping> connectorMapping = legacyDefaultColumnMapping(typeHandle);
        if (connectorMapping.isPresent()) {
            return connectorMapping;
        }
        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return mapToUnboundedVarchar(typeHandle);
        }
        return Optional.empty();
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        // todo remove this when legacy type mapping is no longer supported
        if (legacyTypeMapping) {
            return legacyToWriteMapping(type);
        }

        if (BOOLEAN.equals(type)) {
            return WriteMapping.booleanMapping("boolean", booleanWriteFunction());
        }
        if (TINYINT.equals(type)) {
            // Redshift doesn't have tinyint
            return WriteMapping.longMapping("smallint", tinyintWriteFunction());
        }
        if (SMALLINT.equals(type)) {
            return WriteMapping.longMapping("smallint", smallintWriteFunction());
        }
        if (INTEGER.equals(type)) {
            return WriteMapping.longMapping("integer", integerWriteFunction());
        }
        if (BIGINT.equals(type)) {
            return WriteMapping.longMapping("bigint", bigintWriteFunction());
        }
        if (REAL.equals(type)) {
            return WriteMapping.longMapping("real", realWriteFunction());
        }
        if (DOUBLE.equals(type)) {
            return WriteMapping.doubleMapping("double precision", doubleWriteFunction());
        }

        if (type instanceof DecimalType decimal) {
            if (decimal.getPrecision() == REDSHIFT_DECIMAL_CUTOFF_PRECISION) {
                // See doc for REDSHIFT_DECIMAL_CUTOFF_PRECISION
                return WriteMapping.objectMapping(
                        format("decimal(%s, %s)", decimal.getPrecision(), decimal.getScale()),
                        writeDecimalAtRedshiftCutoff(decimal.getScale()));
            }
            String name = format("decimal(%s, %s)", decimal.getPrecision(), decimal.getScale());
            return decimal.isShort()
                    ? WriteMapping.longMapping(name, shortDecimalWriteFunction(decimal))
                    : WriteMapping.objectMapping(name, longDecimalWriteFunction(decimal));
        }

        if (type instanceof CharType) {
            // Redshift has no unbounded text/binary types, so if a CHAR is too
            // large for Redshift, we write as VARCHAR. If too large for that,
            // we use the largest VARCHAR Redshift supports.
            int size = ((CharType) type).getLength();
            if (size <= REDSHIFT_MAX_CHAR) {
                return WriteMapping.sliceMapping(
                        format("char(%d)", size),
                        RedshiftClient::writeChar);
            }
            int redshiftVarcharWidth = min(size, REDSHIFT_MAX_VARCHAR);
            return WriteMapping.sliceMapping(
                    format("varchar(%d)", redshiftVarcharWidth),
                    (statement, index, value) -> writeCharAsVarchar(statement, index, value, redshiftVarcharWidth));
        }

        if (type instanceof VarcharType) {
            // Redshift has no unbounded text/binary types, so if a VARCHAR is
            // larger than Redshift's limit, we make it that big instead.
            int size = ((VarcharType) type).getLength()
                    .filter(n -> n <= REDSHIFT_MAX_VARCHAR)
                    .orElse(REDSHIFT_MAX_VARCHAR);
            return WriteMapping.sliceMapping(format("varchar(%d)", size), varcharWriteFunction());
        }

        if (VARBINARY.equals(type)) {
            return WriteMapping.sliceMapping("varbyte", varbinaryWriteFunction());
        }

        if (DATE.equals(type)) {
            return WriteMapping.longMapping("date", RedshiftClient::writeDate);
        }

        if (type instanceof TimeType) {
            return WriteMapping.longMapping("time", RedshiftClient::writeTime);
        }

        if (type instanceof TimestampType) {
            if (((TimestampType) type).isShort()) {
                return WriteMapping.longMapping(
                        "timestamp",
                        RedshiftClient::writeShortTimestamp);
            }
            return WriteMapping.objectMapping(
                    "timestamp",
                    LongTimestamp.class,
                    RedshiftClient::writeLongTimestamp);
        }

        if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
            if (timestampWithTimeZoneType.getPrecision() <= TimestampWithTimeZoneType.MAX_SHORT_PRECISION) {
                return WriteMapping.longMapping("timestamptz", shortTimestampWithTimeZoneWriteFunction());
            }
            return WriteMapping.objectMapping("timestamptz", longTimestampWithTimeZoneWriteFunction());
        }

        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
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
    public void setColumnComment(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Optional<String> comment)
    {
        // Redshift doesn't support prepared statement for COMMENT statement
        String sql = format(
                "COMMENT ON COLUMN %s.%s IS %s",
                quoted(handle.asPlainTable().getRemoteTableName()),
                quoted(column.getColumnName()),
                comment.map(RedshiftClient::redshiftVarcharLiteral).orElse("NULL"));
        execute(session, sql);
    }

    @Override
    public void setColumnType(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Type type)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support setting column types");
    }

    private static String redshiftVarcharLiteral(String value)
    {
        requireNonNull(value, "value is null");
        return "'" + value.replace("'", "''").replace("\\", "\\\\") + "'";
    }

    private static ObjectReadFunction longTimestampWithTimeZoneReadFunction()
    {
        return ObjectReadFunction.of(
                LongTimestampWithTimeZone.class,
                (resultSet, columnIndex) -> {
                    // Redshift does not store zone information in "timestamp with time zone" data type
                    OffsetDateTime offsetDateTime = resultSet.getObject(columnIndex, OffsetDateTime.class);
                    return fromEpochSecondsAndFraction(
                            offsetDateTime.toEpochSecond(),
                            (long) offsetDateTime.getNano() * PICOSECONDS_PER_NANOSECOND,
                            UTC_KEY);
                });
    }

    private static LongWriteFunction shortTimestampWithTimeZoneWriteFunction()
    {
        return (statement, index, value) -> {
            // Redshift does not store zone information in "timestamp with time zone" data type
            long millisUtc = unpackMillisUtc(value);
            long epochSeconds = floorDiv(millisUtc, MILLISECONDS_PER_SECOND);
            int nanosOfSecond = floorMod(millisUtc, MILLISECONDS_PER_SECOND) * NANOSECONDS_PER_MILLISECOND;
            OffsetDateTime offsetDateTime = OffsetDateTime.ofInstant(Instant.ofEpochSecond(epochSeconds, nanosOfSecond), UTC_KEY.getZoneId());
            verifySupportedTimestampWithTimeZone(offsetDateTime);
            statement.setObject(index, offsetDateTime);
        };
    }

    private static ObjectWriteFunction longTimestampWithTimeZoneWriteFunction()
    {
        return ObjectWriteFunction.of(
                LongTimestampWithTimeZone.class,
                (statement, index, value) -> {
                    // Redshift does not store zone information in "timestamp with time zone" data type
                    long epochSeconds = floorDiv(value.getEpochMillis(), MILLISECONDS_PER_SECOND);
                    long nanosOfSecond = ((long) floorMod(value.getEpochMillis(), MILLISECONDS_PER_SECOND) * NANOSECONDS_PER_MILLISECOND) +
                            (value.getPicosOfMilli() / PICOSECONDS_PER_NANOSECOND);
                    OffsetDateTime offsetDateTime = OffsetDateTime.ofInstant(Instant.ofEpochSecond(epochSeconds, nanosOfSecond), UTC_KEY.getZoneId());
                    verifySupportedTimestampWithTimeZone(offsetDateTime);
                    statement.setObject(index, offsetDateTime);
                });
    }

    private static void verifySupportedTimestampWithTimeZone(OffsetDateTime value)
    {
        if (value.isBefore(REDSHIFT_MIN_SUPPORTED_TIMESTAMP_TZ)) {
            DateTimeFormatter format = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSSSS");
            throw new TrinoException(
                    INVALID_ARGUMENTS,
                    format("Minimum timestamp with time zone in Redshift is %s: %s", REDSHIFT_MIN_SUPPORTED_TIMESTAMP_TZ.format(format), value.format(format)));
        }
    }

    /**
     * Decimal write function for precision {@link #REDSHIFT_DECIMAL_CUTOFF_PRECISION}.
     * Ensures that values fit in 8 bytes.
     */
    private static ObjectWriteFunction writeDecimalAtRedshiftCutoff(int scale)
    {
        return ObjectWriteFunction.of(
                Int128.class,
                (statement, index, decimal) -> {
                    BigInteger unscaled = decimal.toBigInteger();
                    if (unscaled.bitLength() > REDSHIFT_DECIMAL_CUTOFF_BITS) {
                        throw new TrinoException(JDBC_NON_TRANSIENT_ERROR, format(
                                "Value out of range for Redshift DECIMAL(%d, %d)",
                                REDSHIFT_DECIMAL_CUTOFF_PRECISION,
                                scale));
                    }
                    MathContext precision = new MathContext(REDSHIFT_DECIMAL_CUTOFF_PRECISION);
                    statement.setBigDecimal(index, new BigDecimal(unscaled, scale, precision));
                });
    }

    /**
     * Like {@link StandardColumnMappings#charWriteFunction}, but restrict to
     * ASCII because Redshift only allows ASCII in {@code CHAR} values.
     */
    private static void writeChar(PreparedStatement statement, int index, Slice slice)
            throws SQLException
    {
        String value = slice.toStringUtf8();
        if (!CharMatcher.ascii().matchesAllOf(value)) {
            throw new TrinoException(
                    JDBC_NON_TRANSIENT_ERROR,
                    format("Value for Redshift CHAR must be ASCII, but found '%s'", value));
        }
        statement.setString(index, slice.toStringAscii());
    }

    /**
     * Like {@link StandardColumnMappings#charWriteFunction}, but pads
     * the value with spaces to simulate {@code CHAR} semantics.
     */
    private static void writeCharAsVarchar(PreparedStatement statement, int index, Slice slice, int columnLength)
            throws SQLException
    {
        // Redshift counts varchar size limits in UTF-8 bytes, so this may make the string longer than
        // the limit, but Redshift also truncates extra trailing spaces, so that doesn't cause any problems.
        statement.setString(index, Chars.padSpaces(slice, columnLength).toStringUtf8());
    }

    private static void writeDate(PreparedStatement statement, int index, long day)
            throws SQLException
    {
        statement.setObject(index, new RedshiftObject("date", DATE_FORMATTER.format(LocalDate.ofEpochDay(day))));
    }

    private static long readDate(ResultSet results, int index)
            throws SQLException
    {
        // Reading date as string to workaround issues around julian->gregorian calendar switch
        return LocalDate.parse(results.getString(index), DATE_FORMATTER).toEpochDay();
    }

    /**
     * Write time with microsecond precision
     */
    private static void writeTime(PreparedStatement statement, int index, long picos)
            throws SQLException
    {
        statement.setObject(index, LocalTime.ofNanoOfDay((roundDiv(picos, PICOSECONDS_PER_MICROSECOND) % MICROSECONDS_PER_DAY) * NANOSECONDS_PER_MICROSECOND));
    }

    /**
     * Read a time value with microsecond precision
     */
    private static long readTime(ResultSet results, int index)
            throws SQLException
    {
        return results.getObject(index, LocalTime.class).toNanoOfDay() * PICOSECONDS_PER_NANOSECOND;
    }

    private static void writeShortTimestamp(PreparedStatement statement, int index, long epochMicros)
            throws SQLException
    {
        statement.setObject(index, new RedshiftObject("timestamp", DATE_TIME_FORMATTER.format(StandardColumnMappings.fromTrinoTimestamp(epochMicros))));
    }

    private static void writeLongTimestamp(PreparedStatement statement, int index, Object value)
            throws SQLException
    {
        LongTimestamp timestamp = (LongTimestamp) value;
        long epochMicros = timestamp.getEpochMicros();
        if (timestamp.getPicosOfMicro() >= PICOSECONDS_PER_MICROSECOND / 2) {
            epochMicros += 1; // Add one micro if picos round up
        }
        statement.setObject(index, new RedshiftObject("timestamp", DATE_TIME_FORMATTER.format(StandardColumnMappings.fromTrinoTimestamp(epochMicros))));
    }

    private static long readTimestamp(ResultSet results, int index)
            throws SQLException
    {
        return StandardColumnMappings.toTrinoTimestamp(TIMESTAMP_MICROS, results.getObject(index, LocalDateTime.class));
    }

    private static SliceWriteFunction varbinaryWriteFunction()
    {
        return (statement, index, value) -> statement.unwrap(RedshiftPreparedStatement.class).setVarbyte(index, value.getBytes());
    }

    private static Optional<ColumnMapping> legacyDefaultColumnMapping(JdbcTypeHandle typeHandle)
    {
        // This method is copied from deprecated StandardColumnMappings.legacyDefaultColumnMapping()
        switch (typeHandle.getJdbcType()) {
            case Types.BIT:
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

            case Types.REAL:
                return Optional.of(realColumnMapping());

            case Types.FLOAT:
            case Types.DOUBLE:
                return Optional.of(doubleColumnMapping());

            case Types.NUMERIC:
            case Types.DECIMAL:
                int decimalDigits = typeHandle.getRequiredDecimalDigits();
                int precision = typeHandle.getRequiredColumnSize() + max(-decimalDigits, 0); // Map decimal(p, -s) (negative scale) to decimal(p+s, 0).
                if (precision > Decimals.MAX_PRECISION) {
                    return Optional.empty();
                }
                return Optional.of(decimalColumnMapping(createDecimalType(precision, max(decimalDigits, 0))));

            case Types.CHAR:
            case Types.NCHAR:
                return Optional.of(defaultCharColumnMapping(typeHandle.getRequiredColumnSize(), false));

            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                return Optional.of(defaultVarcharColumnMapping(typeHandle.getRequiredColumnSize(), false));

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return Optional.of(varbinaryColumnMapping());

            case Types.DATE:
                return Optional.of(dateColumnMappingUsingSqlDate());

            case Types.TIME:
                // TODO default to `timeColumnMapping`
                return Optional.of(timeColumnMappingUsingSqlTime());

            case Types.TIMESTAMP:
                // TODO default to `timestampColumnMapping`
                return Optional.of(timestampColumnMappingUsingSqlTimestampWithRounding(TIMESTAMP_MILLIS));
        }
        return Optional.empty();
    }

    private static WriteMapping legacyToWriteMapping(Type type)
    {
        // This method is copied from deprecated BaseJdbcClient.legacyToWriteMapping()
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
        if (type instanceof CharType charType) {
            return WriteMapping.sliceMapping("char(" + charType.getLength() + ")", charWriteFunction());
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
        if (type == VARBINARY) {
            return WriteMapping.sliceMapping("varbinary", varbinaryWriteFunction());
        }
        if (type == DATE) {
            return WriteMapping.longMapping("date", dateWriteFunctionUsingSqlDate());
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }
}
