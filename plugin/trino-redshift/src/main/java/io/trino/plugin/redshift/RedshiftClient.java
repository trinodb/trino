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
import io.trino.plugin.base.projection.ProjectFunctionRewriter;
import io.trino.plugin.base.projection.ProjectFunctionRule;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcJoinCondition;
import io.trino.plugin.jdbc.JdbcMetadata;
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
import io.trino.plugin.jdbc.expression.ComparisonOperator;
import io.trino.plugin.jdbc.expression.JdbcConnectorExpressionRewriterBuilder;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.expression.RewriteComparison;
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
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Chars;
import io.trino.spi.type.DecimalType;
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
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_NON_TRANSIENT_ERROR;
import static io.trino.plugin.jdbc.JdbcJoinPushdownUtil.implementJoinCostAware;
import static io.trino.plugin.jdbc.PredicatePushdownController.pushdownDiscreteValues;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.realColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.plugin.redshift.RedshiftErrorCode.REDSHIFT_INVALID_TYPE;
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

    private final ProjectFunctionRewriter<JdbcExpression, ParameterizedExpression> projectFunctionRewriter;
    private final AggregateFunctionRewriter<JdbcExpression, ?> aggregateFunctionRewriter;
    private final boolean statisticsEnabled;
    private final RedshiftTableStatisticsReader statisticsReader;
    private final ConnectorExpressionRewriter<ParameterizedExpression> connectorExpressionRewriter;
    private final Optional<Integer> fetchSize;

    @Inject
    public RedshiftClient(
            BaseJdbcConfig config,
            RedshiftConfig redshiftConfig,
            ConnectionFactory connectionFactory,
            JdbcStatisticsConfig statisticsConfig,
            QueryBuilder queryBuilder,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier)
    {
        super("\"", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, queryModifier, true);
        connectorExpressionRewriter = JdbcConnectorExpressionRewriterBuilder.newBuilder()
                .addStandardRules(this::quoted)
                .add(new RewriteComparison(ImmutableSet.of(ComparisonOperator.EQUAL, ComparisonOperator.NOT_EQUAL)))
                .map("$less_than(left, right)").to("left < right")
                .map("$less_than_or_equal(left, right)").to("left <= right")
                .map("$greater_than(left, right)").to("left > right")
                .map("$greater_than_or_equal(left, right)").to("left >= right")
                .build();

        this.projectFunctionRewriter = new ProjectFunctionRewriter<>(
                connectorExpressionRewriter,
                ImmutableSet.<ProjectFunctionRule<JdbcExpression, ParameterizedExpression>>builder()
                        .add(new RewriteCast((session, type) -> toWriteMapping(session, type).getDataType()))
                        .build());

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
        this.fetchSize = redshiftConfig.getFetchSize();
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
    protected ResultSet getAllTableColumns(Connection connection, Optional<String> remoteSchemaName)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        return metadata.getColumns(
                metadata.getConnection().getCatalog(),
                escapeObjectNameForMetadataQuery(remoteSchemaName, metadata.getSearchStringEscape()).orElse(null),
                null,
                null);
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
    public Optional<JdbcExpression> convertProjection(ConnectorSession session, JdbcTableHandle handle, ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        return projectFunctionRewriter.rewrite(session, handle, expression, assignments);
    }

    @Override
    public Optional<ParameterizedExpression> convertPredicate(ConnectorSession session, ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        return connectorExpressionRewriter.rewrite(session, expression, assignments);
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
            return statisticsReader.readTableStatistics(session, handle, () -> JdbcMetadata.getColumns(session, this, handle));
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
                        String ordering = sortItem.sortOrder().isAscending() ? "ASC" : "DESC";
                        String nullsHandling = sortItem.sortOrder().isNullsFirst() ? "NULLS FIRST" : "NULLS LAST";
                        return format("%s %s %s", quoted(sortItem.column().getColumnName()), ordering, nullsHandling);
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
        return joinCondition.getOperator() != JoinCondition.Operator.IDENTICAL;
    }

    @Override
    public Optional<PreparedQuery> implementJoin(ConnectorSession session,
            JoinType joinType,
            PreparedQuery leftSource,
            Map<JdbcColumnHandle, String> leftProjections,
            PreparedQuery rightSource,
            Map<JdbcColumnHandle, String> rightProjections,
            List<ParameterizedExpression> joinConditions,
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
                () -> super.implementJoin(session, joinType, leftSource, leftProjections, rightSource, rightProjections, joinConditions, statistics));
    }

    @Override
    public Optional<PreparedQuery> legacyImplementJoin(ConnectorSession session,
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
                () -> super.legacyImplementJoin(session, joinType, leftSource, rightSource, joinConditions, rightAssignments, leftAssignments, statistics));
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
        Optional<Integer> fetchSize = Optional.ofNullable(this.fetchSize.orElseGet(() ->
                columnCount.map(count -> max(100_000 / count, 1_000))
                        .orElse(null)));
        if (fetchSize.isPresent()) {
            statement.setFetchSize(fetchSize.get());
        }
        return statement;
    }

    @Override
    public OptionalLong delete(ConnectorSession session, JdbcTableHandle handle)
    {
        checkArgument(handle.isNamedRelation(), "Unable to delete from synthetic table: %s", handle);
        checkArgument(handle.getLimit().isEmpty(), "Unable to delete when limit is set: %s", handle);
        checkArgument(handle.getSortOrder().isEmpty(), "Unable to delete when sort order is set: %s", handle);
        checkArgument(handle.getUpdateAssignments().isEmpty(), "Unable to delete when update assignments are set: %s", handle);
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
    public OptionalLong update(ConnectorSession session, JdbcTableHandle handle)
    {
        checkArgument(handle.isNamedRelation(), "Unable to update from synthetic table: %s", handle);
        checkArgument(handle.getLimit().isEmpty(), "Unable to update when limit is set: %s", handle);
        checkArgument(handle.getSortOrder().isEmpty(), "Unable to update when sort order is set: %s", handle);
        checkArgument(!handle.getUpdateAssignments().isEmpty(), "Unable to update when update assignments are not set: %s", handle);
        try (Connection connection = connectionFactory.openConnection(session)) {
            verify(connection.getAutoCommit());
            PreparedQuery preparedQuery = queryBuilder.prepareUpdateQuery(
                    this,
                    session,
                    connection,
                    handle.getRequiredNamedRelation(),
                    handle.getConstraint(),
                    getAdditionalPredicate(handle.getConstraintExpressions(), Optional.empty()),
                    handle.getUpdateAssignments());
            try (PreparedStatement preparedStatement = queryBuilder.prepareStatement(this, session, connection, preparedQuery, Optional.empty())) {
                int affectedRows = preparedStatement.executeUpdate();
                // connection.getAutoCommit() == true is not enough to make UPDATE effective and explicit commit is required
                connection.commit();
                return OptionalLong.of(affectedRows);
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public OptionalInt getMaxColumnNameLength(ConnectorSession session)
    {
        return getMaxColumnNameLengthFromDatabaseMetaData(session);
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
        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(type);
        if (mapping.isPresent()) {
            return mapping;
        }

        if ("time".equals(type.jdbcTypeName().orElse(""))) {
            return Optional.of(ColumnMapping.longMapping(
                    TIME_MICROS,
                    RedshiftClient::readTime,
                    RedshiftClient::writeTime));
        }

        switch (type.jdbcType()) {
            case Types.BIT: // Redshift uses this for booleans
                return Optional.of(booleanColumnMapping());

            // case Types.TINYINT: -- Redshift doesn't support tinyint
            case Types.SMALLINT:
                // IN clause query in Redshift performs better compared to range queries, hence convert range queries to discrete set where possible.
                return Optional.of(ColumnMapping.longMapping(SMALLINT, ResultSet::getShort, smallintWriteFunction(), pushdownDiscreteValues(SMALLINT)));
            case Types.INTEGER:
                // IN clause query in Redshift performs better compared to range queries, hence convert range queries to discrete set where possible.
                return Optional.of(ColumnMapping.longMapping(INTEGER, ResultSet::getInt, integerWriteFunction(), pushdownDiscreteValues(INTEGER)));
            case Types.BIGINT:
                // IN clause query in Redshift performs better compared to range queries, hence convert range queries to discrete set where possible.
                return Optional.of(ColumnMapping.longMapping(BIGINT, ResultSet::getLong, bigintWriteFunction(), pushdownDiscreteValues(BIGINT)));

            case Types.REAL:
                return Optional.of(realColumnMapping());
            case Types.DOUBLE:
                return Optional.of(doubleColumnMapping());

            case Types.NUMERIC: {
                int precision = type.requiredColumnSize();
                int scale = type.requiredDecimalDigits();
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
                CharType charType = createCharType(type.requiredColumnSize());
                return Optional.of(ColumnMapping.sliceMapping(
                        charType,
                        charReadFunction(charType),
                        RedshiftClient::writeChar));

            case Types.VARCHAR: {
                if (type.columnSize().isEmpty()) {
                    throw new TrinoException(REDSHIFT_INVALID_TYPE, "column size not present");
                }
                int length = type.requiredColumnSize();
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

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
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

    @Override
    public void dropNotNullConstraint(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping a not null constraint");
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
}
