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
package io.trino.plugin.mysql;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.mysql.cj.jdbc.JdbcStatement;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.trino.plugin.base.aggregation.AggregateFunctionRewriter;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.CaseSensitivity;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcJoinCondition;
import io.trino.plugin.jdbc.JdbcMetadata;
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
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.aggregation.ImplementAvgDecimal;
import io.trino.plugin.jdbc.aggregation.ImplementAvgFloatingPoint;
import io.trino.plugin.jdbc.aggregation.ImplementCount;
import io.trino.plugin.jdbc.aggregation.ImplementCountAll;
import io.trino.plugin.jdbc.aggregation.ImplementMinMax;
import io.trino.plugin.jdbc.aggregation.ImplementStddevPop;
import io.trino.plugin.jdbc.aggregation.ImplementStddevSamp;
import io.trino.plugin.jdbc.aggregation.ImplementSum;
import io.trino.plugin.jdbc.aggregation.ImplementVariancePop;
import io.trino.plugin.jdbc.aggregation.ImplementVarianceSamp;
import io.trino.plugin.jdbc.expression.JdbcConnectorExpressionRewriterBuilder;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.expression.RewriteLikeEscapeWithCaseSensitivity;
import io.trino.plugin.jdbc.expression.RewriteLikeWithCaseSensitivity;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnPosition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.JoinCondition;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.LongTimestampWithTimeZone;
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
import org.jdbi.v3.core.statement.UnableToExecuteStatementException;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.mysql.cj.exceptions.MysqlErrorNumbers.ER_NO_SUCH_TABLE;
import static com.mysql.cj.exceptions.MysqlErrorNumbers.ER_TABLE_EXISTS_ERROR;
import static com.mysql.cj.exceptions.MysqlErrorNumbers.ER_UNKNOWN_TABLE;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.base.util.JsonTypeUtil.jsonParse;
import static io.trino.plugin.jdbc.CaseSensitivity.CASE_INSENSITIVE;
import static io.trino.plugin.jdbc.CaseSensitivity.CASE_SENSITIVE;
import static io.trino.plugin.jdbc.DecimalConfig.DecimalMapping.ALLOW_OVERFLOW;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalDefaultScale;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRounding;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRoundingMode;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.JdbcJoinPushdownUtil.implementJoinCostAware;
import static io.trino.plugin.jdbc.JdbcMetadataSessionProperties.getDomainCompactionThreshold;
import static io.trino.plugin.jdbc.PredicatePushdownController.CASE_INSENSITIVE_CHARACTER_PUSHDOWN;
import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.PredicatePushdownController.FULL_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateReadFunctionUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.defaultVarcharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longTimestampWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timeReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timeWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
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
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.time.format.DateTimeFormatter.ISO_DATE;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class MySqlClient
        extends BaseJdbcClient
{
    private static final Logger log = Logger.get(MySqlClient.class);

    private static final int MAX_SUPPORTED_DATE_TIME_PRECISION = 6;
    // MySQL driver returns width of timestamp types instead of precision.
    // 19 characters are used for zero-precision timestamps while others
    // require 19 + precision + 1 characters with the additional character
    // required for the decimal separator.
    private static final int ZERO_PRECISION_TIMESTAMP_COLUMN_SIZE = 19;
    // MySQL driver returns width of time types instead of precision, same as the above timestamp type.
    private static final int ZERO_PRECISION_TIME_COLUMN_SIZE = 8;

    // An empty character means that the table doesn't have a comment in MySQL
    private static final String NO_COMMENT = "";

    private static final JsonCodec<ColumnHistogram> HISTOGRAM_CODEC = jsonCodec(ColumnHistogram.class);

    private final Type jsonType;
    private final boolean statisticsEnabled;
    private final ConnectorExpressionRewriter<ParameterizedExpression> connectorExpressionRewriter;
    private final AggregateFunctionRewriter<JdbcExpression, ?> aggregateFunctionRewriter;

    private static final PredicatePushdownController MYSQL_CHARACTER_PUSHDOWN = (session, domain) -> {
        if (domain.isNullableSingleValue()) {
            return FULL_PUSHDOWN.apply(session, domain);
        }

        Domain simplifiedDomain = domain.simplify(getDomainCompactionThreshold(session));
        if (!simplifiedDomain.getValues().isDiscreteSet()) {
            // Push down inequality predicate
            ValueSet complement = simplifiedDomain.getValues().complement();
            if (complement.isDiscreteSet()) {
                return FULL_PUSHDOWN.apply(session, simplifiedDomain);
            }
            // Domain#simplify can turn a discrete set into a range predicate
            // Push down of range predicate for varchar/char types could lead to incorrect results
            // when the remote database is case insensitive
            return DISABLE_PUSHDOWN.apply(session, domain);
        }
        return FULL_PUSHDOWN.apply(session, simplifiedDomain);
    };

    @Inject
    public MySqlClient(
            BaseJdbcConfig config,
            JdbcStatisticsConfig statisticsConfig,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            TypeManager typeManager,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier)
    {
        super("`", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, queryModifier, true);
        this.jsonType = typeManager.getType(new TypeSignature(StandardTypes.JSON));
        this.statisticsEnabled = statisticsConfig.isEnabled();

        this.connectorExpressionRewriter = JdbcConnectorExpressionRewriterBuilder.newBuilder()
                .addStandardRules(this::quoted)
                // No "real" on the list; pushdown on REAL is disabled also in toColumnMapping
                .withTypeClass("numeric_type", ImmutableSet.of("tinyint", "smallint", "integer", "bigint", "decimal", "double"))
                .map("$equal(left: numeric_type, right: numeric_type)").to("left = right")
                .map("$not_equal(left: numeric_type, right: numeric_type)").to("left <> right")
                // .map("$is_distinct_from(left: numeric_type, right: numeric_type)").to("left IS DISTINCT FROM right")
                .map("$less_than(left: numeric_type, right: numeric_type)").to("left < right")
                .map("$less_than_or_equal(left: numeric_type, right: numeric_type)").to("left <= right")
                .map("$greater_than(left: numeric_type, right: numeric_type)").to("left > right")
                .map("$greater_than_or_equal(left: numeric_type, right: numeric_type)").to("left >= right")
                .add(new RewriteLikeWithCaseSensitivity())
                .add(new RewriteLikeEscapeWithCaseSensitivity())
                .build();

        JdbcTypeHandle bigintTypeHandle = new JdbcTypeHandle(Types.BIGINT, Optional.of("bigint"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        this.aggregateFunctionRewriter = new AggregateFunctionRewriter<>(
                this.connectorExpressionRewriter,
                ImmutableSet.<AggregateFunctionRule<JdbcExpression, ParameterizedExpression>>builder()
                        .add(new ImplementCountAll(bigintTypeHandle))
                        .add(new ImplementCount(bigintTypeHandle))
                        .add(new ImplementMinMax(false))
                        .add(new ImplementSum(MySqlClient::toTypeHandle))
                        .add(new ImplementAvgFloatingPoint())
                        .add(new ImplementAvgDecimal())
                        .add(new ImplementAvgBigint())
                        .add(new ImplementStddevSamp())
                        .add(new ImplementStddevPop())
                        .add(new ImplementVarianceSamp())
                        .add(new ImplementVariancePop())
                        .build());
    }

    @Override
    public Optional<ParameterizedExpression> convertPredicate(ConnectorSession session, ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        return connectorExpressionRewriter.rewrite(session, expression, assignments);
    }

    @Override
    protected Map<String, CaseSensitivity> getCaseSensitivityForColumns(ConnectorSession session, Connection connection, SchemaTableName schemaTableName, RemoteTableName remoteTableName)
    {
        PreparedQuery preparedQuery = new PreparedQuery(format("SELECT * FROM %s", quoted(remoteTableName)), ImmutableList.of());

        try (PreparedStatement preparedStatement = queryBuilder.prepareStatement(this, session, connection, preparedQuery, Optional.empty())) {
            ResultSetMetaData metadata = preparedStatement.getMetaData();
            ImmutableMap.Builder<String, CaseSensitivity> columns = ImmutableMap.builder();
            for (int column = 1; column <= metadata.getColumnCount(); column++) {
                String name = metadata.getColumnName(column);
                columns.put(name, metadata.isCaseSensitive(column) ? CASE_SENSITIVE : CASE_INSENSITIVE);
            }
            return columns.buildOrThrow();
        }
        catch (SQLException e) {
            if (e.getErrorCode() == ER_NO_SUCH_TABLE) {
                throw new TableNotFoundException(schemaTableName);
            }
            throw new TrinoException(JDBC_ERROR, "Failed to get case sensitivity for columns. " + firstNonNull(e.getMessage(), e), e);
        }
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
        // Remote database can be case insensitive.
        return preventTextualTypeAggregationPushdown(groupingSets);
    }

    private static Optional<JdbcTypeHandle> toTypeHandle(DecimalType decimalType)
    {
        return Optional.of(new JdbcTypeHandle(Types.NUMERIC, Optional.of("decimal"), Optional.of(decimalType.getPrecision()), Optional.of(decimalType.getScale()), Optional.empty(), Optional.empty()));
    }

    @Override
    public Collection<String> listSchemas(Connection connection)
    {
        // for MySQL, we need to list catalogs instead of schemas
        try (ResultSet resultSet = connection.getMetaData().getCatalogs()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_CAT");
                // skip internal schemas
                if (filterSchema(schemaName)) {
                    schemaNames.add(schemaName);
                }
            }
            return schemaNames.build();
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected boolean filterSchema(String schemaName)
    {
        if (schemaName.equalsIgnoreCase("mysql")
                || schemaName.equalsIgnoreCase("sys")) {
            return false;
        }
        return super.filterSchema(schemaName);
    }

    @Override
    protected void dropSchema(ConnectorSession session, Connection connection, String remoteSchemaName, boolean cascade)
            throws SQLException
    {
        // MySQL always deletes all tables inside the database https://dev.mysql.com/doc/refman/8.0/en/drop-database.html
        if (!cascade) {
            try (ResultSet tables = getTables(connection, Optional.of(remoteSchemaName), Optional.empty())) {
                if (tables.next()) {
                    throw new TrinoException(SCHEMA_NOT_EMPTY, "Cannot drop non-empty schema '%s'".formatted(remoteSchemaName));
                }
            }
        }
        execute(session, connection, "DROP SCHEMA " + quoted(remoteSchemaName));
    }

    @Override
    public void abortReadConnection(Connection connection, ResultSet resultSet)
            throws SQLException
    {
        if (!resultSet.isAfterLast()) {
            // Abort connection before closing. Without this, the MySQL driver
            // attempts to drain the connection by reading all the results.
            connection.abort(directExecutor());
        }
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql, Optional<Integer> columnCount)
            throws SQLException
    {
        PreparedStatement statement = connection.prepareStatement(sql);
        if (statement.isWrapperFor(JdbcStatement.class)) {
            statement.unwrap(JdbcStatement.class).enableStreamingResults();
        }
        return statement;
    }

    @Override
    public ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        // MySQL maps their "database" to SQL catalogs and does not have schemas
        DatabaseMetaData metadata = connection.getMetaData();
        return metadata.getTables(
                schemaName.orElse(null),
                null,
                escapeObjectNameForMetadataQuery(tableName, metadata.getSearchStringEscape()).orElse(null),
                getTableTypes().map(types -> types.toArray(String[]::new)).orElse(null));
    }

    @Override
    protected ResultSet getAllTableColumns(Connection connection, Optional<String> remoteSchemaName)
            throws SQLException
    {
        // MySQL maps their "database" to SQL catalogs and does not have schemas
        DatabaseMetaData metadata = connection.getMetaData();
        return metadata.getColumns(
                remoteSchemaName.orElse(null),
                null,
                null,
                null);
    }

    @Override
    public Optional<String> getTableComment(ResultSet resultSet)
            throws SQLException
    {
        // Empty remarks means that the table doesn't have a comment in MySQL
        return Optional.ofNullable(emptyToNull(resultSet.getString("REMARKS")));
    }

    @Override
    public void setTableComment(ConnectorSession session, JdbcTableHandle handle, Optional<String> comment)
    {
        String sql = format(
                "ALTER TABLE %s COMMENT = %s",
                quoted(handle.asPlainTable().getRemoteTableName()),
                mysqlVarcharLiteral(comment.orElse(NO_COMMENT))); // An empty character removes the existing comment in MySQL
        execute(session, sql);
    }

    @Override
    protected String getTableSchemaName(ResultSet resultSet)
            throws SQLException
    {
        // MySQL uses catalogs instead of schemas
        return resultSet.getString("TABLE_CAT");
    }

    @Override
    protected List<String> createTableSqls(RemoteTableName remoteTableName, List<String> columns, ConnectorTableMetadata tableMetadata)
    {
        checkArgument(tableMetadata.getProperties().isEmpty(), "Unsupported table properties: %s", tableMetadata.getProperties());
        return ImmutableList.of(format("CREATE TABLE %s (%s) COMMENT %s", quoted(remoteTableName), join(", ", columns), mysqlVarcharLiteral(tableMetadata.getComment().orElse(NO_COMMENT))));
    }

    // This is overridden to pass NULL to MySQL for TIMESTAMP column types
    // Without it, an "Invalid default value" error is thrown
    @Override
    protected String getColumnDefinitionSql(ConnectorSession session, ColumnMetadata column, String columnName)
    {
        if (column.getComment() != null) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with column comment");
        }

        return "%s %s %s".formatted(
                quoted(columnName),
                toWriteMapping(session, column.getType()).getDataType(),
                column.isNullable() ? "NULL" : "NOT NULL");
    }

    private static String mysqlVarcharLiteral(String value)
    {
        requireNonNull(value, "value is null");
        return "'" + value.replace("'", "''").replace("\\", "\\\\") + "'";
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        String jdbcTypeName = typeHandle.jdbcTypeName()
                .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + typeHandle));

        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }

        switch (jdbcTypeName.toLowerCase(ENGLISH)) {
            case "tinyint unsigned":
                return Optional.of(smallintColumnMapping());
            case "smallint unsigned":
                return Optional.of(integerColumnMapping());
            case "int unsigned":
                return Optional.of(bigintColumnMapping());
            case "bigint unsigned":
                return Optional.of(decimalColumnMapping(createDecimalType(20)));
            case "json":
                return Optional.of(jsonColumnMapping());
            case "enum":
                return Optional.of(defaultVarcharColumnMapping(typeHandle.requiredColumnSize(), false));
            case "datetime":
                return mysqlDateTimeToTrinoTimestamp(typeHandle);
        }

        switch (typeHandle.jdbcType()) {
            case Types.BIT:
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
                // Disable pushdown because floating-point values are approximate and not stored as exact values,
                // attempts to treat them as exact in comparisons may lead to problems
                return Optional.of(ColumnMapping.longMapping(
                        REAL,
                        (resultSet, columnIndex) -> floatToRawIntBits(resultSet.getFloat(columnIndex)),
                        realWriteFunction(),
                        DISABLE_PUSHDOWN));

            case Types.DOUBLE:
                return Optional.of(doubleColumnMapping());

            case Types.NUMERIC:
            case Types.DECIMAL:
                int decimalDigits = typeHandle.decimalDigits().orElseThrow(() -> new IllegalStateException("decimal digits not present"));
                int precision = typeHandle.requiredColumnSize();
                if (getDecimalRounding(session) == ALLOW_OVERFLOW && precision > Decimals.MAX_PRECISION) {
                    int scale = min(decimalDigits, getDecimalDefaultScale(session));
                    return Optional.of(decimalColumnMapping(createDecimalType(Decimals.MAX_PRECISION, scale), getDecimalRoundingMode(session)));
                }
                // TODO does mysql support negative scale?
                precision = precision + max(-decimalDigits, 0); // Map decimal(p, -s) (negative scale) to decimal(p+s, 0).
                if (precision > Decimals.MAX_PRECISION) {
                    break;
                }
                return Optional.of(decimalColumnMapping(createDecimalType(precision, max(decimalDigits, 0))));

            case Types.CHAR:
                return Optional.of(mySqlDefaultCharColumnMapping(typeHandle.requiredColumnSize(), typeHandle.caseSensitivity()));

            // TODO not all these type constants are necessarily used by the JDBC driver
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                return Optional.of(mySqlDefaultVarcharColumnMapping(typeHandle.requiredColumnSize(), typeHandle.caseSensitivity()));

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return Optional.of(ColumnMapping.sliceMapping(VARBINARY, varbinaryReadFunction(), varbinaryWriteFunction(), FULL_PUSHDOWN));

            case Types.DATE:
                return Optional.of(ColumnMapping.longMapping(
                        DATE,
                        dateReadFunctionUsingLocalDate(),
                        mySqlDateWriteFunctionUsingLocalDate()));

            case Types.TIME:
                TimeType timeType = createTimeType(getTimePrecision(typeHandle.requiredColumnSize()));
                requireNonNull(timeType, "timeType is null");
                checkArgument(timeType.getPrecision() <= 9, "Unsupported type precision: %s", timeType);
                return Optional.of(ColumnMapping.longMapping(
                        timeType,
                        mySqlTimeReadFunction(timeType),
                        timeWriteFunction(timeType.getPrecision())));

            case Types.TIMESTAMP:
                return mysqlTimestampToTrinoTimestampWithTz(typeHandle);
        }

        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return mapToUnboundedVarchar(typeHandle);
        }
        return Optional.empty();
    }

    private static ColumnMapping mySqlDefaultVarcharColumnMapping(int columnSize, Optional<CaseSensitivity> caseSensitivity)
    {
        if (columnSize > VarcharType.MAX_LENGTH) {
            return mySqlVarcharColumnMapping(createUnboundedVarcharType(), caseSensitivity);
        }
        return mySqlVarcharColumnMapping(createVarcharType(columnSize), caseSensitivity);
    }

    private static ColumnMapping mySqlVarcharColumnMapping(VarcharType varcharType, Optional<CaseSensitivity> caseSensitivity)
    {
        PredicatePushdownController pushdownController = caseSensitivity.orElse(CASE_INSENSITIVE) == CASE_SENSITIVE
                ? MYSQL_CHARACTER_PUSHDOWN
                : CASE_INSENSITIVE_CHARACTER_PUSHDOWN;
        return ColumnMapping.sliceMapping(varcharType, varcharReadFunction(varcharType), varcharWriteFunction(), pushdownController);
    }

    private static ColumnMapping mySqlDefaultCharColumnMapping(int columnSize, Optional<CaseSensitivity> caseSensitivity)
    {
        if (columnSize > CharType.MAX_LENGTH) {
            return mySqlDefaultVarcharColumnMapping(columnSize, caseSensitivity);
        }
        return mySqlCharColumnMapping(createCharType(columnSize), caseSensitivity);
    }

    private static ColumnMapping mySqlCharColumnMapping(CharType charType, Optional<CaseSensitivity> caseSensitivity)
    {
        requireNonNull(charType, "charType is null");
        PredicatePushdownController pushdownController = caseSensitivity.orElse(CASE_INSENSITIVE) == CASE_SENSITIVE
                ? MYSQL_CHARACTER_PUSHDOWN
                : CASE_INSENSITIVE_CHARACTER_PUSHDOWN;
        return ColumnMapping.sliceMapping(charType, charReadFunction(charType), charWriteFunction(), pushdownController);
    }

    private Optional<ColumnMapping> mysqlDateTimeToTrinoTimestamp(JdbcTypeHandle typeHandle)
    {
        TimestampType timestampType = createTimestampType(getTimestampPrecision(typeHandle.requiredColumnSize()));
        checkArgument(timestampType.getPrecision() <= TimestampType.MAX_SHORT_PRECISION, "Precision is out of range: %s", timestampType.getPrecision());
        return Optional.of(ColumnMapping.longMapping(
                timestampType,
                mySqlTimestampReadFunction(timestampType),
                timestampWriteFunction(timestampType)));
    }

    private static Optional<ColumnMapping> mysqlTimestampToTrinoTimestampWithTz(JdbcTypeHandle typeHandle)
    {
        TimestampWithTimeZoneType trinoType = createTimestampWithTimeZoneType(getTimestampPrecision(typeHandle.requiredColumnSize()));
        if (trinoType.getPrecision() <= TimestampWithTimeZoneType.MAX_SHORT_PRECISION) {
            return Optional.of(ColumnMapping.longMapping(
                    trinoType,
                    shortTimestampWithTimeZoneReadFunction(),
                    shortTimestampWithTimeZoneWriteFunction()));
        }
        return Optional.of(ColumnMapping.objectMapping(
                trinoType,
                longTimestampWithTimeZoneReadFunction(),
                longTimestampWithTimeZoneWriteFunction()));
    }

    private static LongReadFunction shortTimestampWithTimeZoneReadFunction()
    {
        return (resultSet, columnIndex) -> {
            Timestamp timestamp = resultSet.getTimestamp(columnIndex);
            long millisUtc = timestamp.getTime();
            return packDateTimeWithZone(millisUtc, UTC_KEY);
        };
    }

    private static ObjectReadFunction longTimestampWithTimeZoneReadFunction()
    {
        return ObjectReadFunction.of(
                LongTimestampWithTimeZone.class,
                (resultSet, columnIndex) -> {
                    OffsetDateTime offsetDateTime = resultSet.getObject(columnIndex, OffsetDateTime.class);
                    return LongTimestampWithTimeZone.fromEpochSecondsAndFraction(
                            offsetDateTime.toEpochSecond(),
                            (long) offsetDateTime.getNano() * PICOSECONDS_PER_NANOSECOND,
                            UTC_KEY);
                });
    }

    private static LongWriteFunction shortTimestampWithTimeZoneWriteFunction()
    {
        return (statement, index, value) -> {
            Instant instantValue = Instant.ofEpochMilli(unpackMillisUtc(value));
            statement.setObject(index, instantValue);
        };
    }

    private static ObjectWriteFunction longTimestampWithTimeZoneWriteFunction()
    {
        return ObjectWriteFunction.of(
                LongTimestampWithTimeZone.class,
                (statement, index, value) -> {
                    long epochSeconds = floorDiv(value.getEpochMillis(), MILLISECONDS_PER_SECOND);
                    long nanosOfSecond = (long) floorMod(value.getEpochMillis(), MILLISECONDS_PER_SECOND) * NANOSECONDS_PER_MILLISECOND + value.getPicosOfMilli() / PICOSECONDS_PER_NANOSECOND;
                    Instant instantValue = Instant.ofEpochSecond(epochSeconds, nanosOfSecond);
                    statement.setObject(index, instantValue);
                });
    }

    private LongWriteFunction mySqlDateWriteFunctionUsingLocalDate()
    {
        return new LongWriteFunction()
        {
            @Override
            public String getBindExpression()
            {
                return "CAST(? AS DATE)";
            }

            @Override
            public void set(PreparedStatement statement, int index, long epochDay)
                    throws SQLException
            {
                statement.setString(index, LocalDate.ofEpochDay(epochDay).format(ISO_DATE));
            }
        };
    }

    private static LongReadFunction mySqlTimestampReadFunction(TimestampType timestampType)
    {
        return new LongReadFunction()
        {
            private final LongReadFunction delegate = timestampReadFunction(timestampType);

            @Override
            public boolean isNull(ResultSet resultSet, int columnIndex)
                    throws SQLException
            {
                // super calls ResultSet#getObject(), which for TIMESTAMP type returns java.sql.Timestamp, for which the conversion can fail if the value isn't a valid instant in server's time zone.
                resultSet.getObject(columnIndex, LocalDateTime.class);
                return resultSet.wasNull();
            }

            @Override
            public long readLong(ResultSet resultSet, int columnIndex)
                    throws SQLException
            {
                return delegate.readLong(resultSet, columnIndex);
            }
        };
    }

    private static LongReadFunction mySqlTimeReadFunction(TimeType timeType)
    {
        return new LongReadFunction()
        {
            private final LongReadFunction delegate = timeReadFunction(timeType);

            @Override
            public boolean isNull(ResultSet resultSet, int columnIndex)
                    throws SQLException
            {
                // super calls ResultSet#getObject(), which for TIME type returns java.sql.Time, for which the conversion can fail if the value isn't a valid instant in server's time zone.
                resultSet.getObject(columnIndex, String.class);
                return resultSet.wasNull();
            }

            @Override
            public long readLong(ResultSet resultSet, int columnIndex)
                    throws SQLException
            {
                return delegate.readLong(resultSet, columnIndex);
            }
        };
    }

    private static int getTimestampPrecision(int timestampColumnSize)
    {
        if (timestampColumnSize == ZERO_PRECISION_TIMESTAMP_COLUMN_SIZE) {
            return 0;
        }
        int timestampPrecision = timestampColumnSize - ZERO_PRECISION_TIMESTAMP_COLUMN_SIZE - 1;
        verify(1 <= timestampPrecision && timestampPrecision <= MAX_SUPPORTED_DATE_TIME_PRECISION, "Unexpected timestamp precision %s calculated from timestamp column size %s", timestampPrecision, timestampColumnSize);
        return timestampPrecision;
    }

    private static int getTimePrecision(int timeColumnSize)
    {
        if (timeColumnSize == ZERO_PRECISION_TIME_COLUMN_SIZE) {
            return 0;
        }
        int timePrecision = timeColumnSize - ZERO_PRECISION_TIME_COLUMN_SIZE - 1;
        verify(1 <= timePrecision && timePrecision <= MAX_SUPPORTED_DATE_TIME_PRECISION, "Unexpected time precision %s calculated from time column size %s", timePrecision, timeColumnSize);
        return timePrecision;
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
            return WriteMapping.doubleMapping("double precision", doubleWriteFunction());
        }

        if (type instanceof DecimalType decimalType) {
            String dataType = format("decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.objectMapping(dataType, longDecimalWriteFunction(decimalType));
        }

        if (type == DATE) {
            return WriteMapping.longMapping("date", mySqlDateWriteFunctionUsingLocalDate());
        }

        if (type instanceof TimeType timeType) {
            if (timeType.getPrecision() <= MAX_SUPPORTED_DATE_TIME_PRECISION) {
                return WriteMapping.longMapping(format("time(%s)", timeType.getPrecision()), timeWriteFunction(timeType.getPrecision()));
            }
            return WriteMapping.longMapping(format("time(%s)", MAX_SUPPORTED_DATE_TIME_PRECISION), timeWriteFunction(MAX_SUPPORTED_DATE_TIME_PRECISION));
        }

        if (type instanceof TimestampType timestampType) {
            if (timestampType.getPrecision() <= MAX_SUPPORTED_DATE_TIME_PRECISION) {
                verify(timestampType.getPrecision() <= TimestampType.MAX_SHORT_PRECISION);
                return WriteMapping.longMapping(format("datetime(%s)", timestampType.getPrecision()), timestampWriteFunction(timestampType));
            }
            return WriteMapping.objectMapping(format("datetime(%s)", MAX_SUPPORTED_DATE_TIME_PRECISION), longTimestampWriteFunction(timestampType, MAX_SUPPORTED_DATE_TIME_PRECISION));
        }

        if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
            if (timestampWithTimeZoneType.getPrecision() <= MAX_SUPPORTED_DATE_TIME_PRECISION) {
                String dataType = format("timestamp(%d)", timestampWithTimeZoneType.getPrecision());
                if (timestampWithTimeZoneType.getPrecision() <= TimestampWithTimeZoneType.MAX_SHORT_PRECISION) {
                    return WriteMapping.longMapping(dataType, shortTimestampWithTimeZoneWriteFunction());
                }
                return WriteMapping.objectMapping(dataType, longTimestampWithTimeZoneWriteFunction());
            }
            return WriteMapping.objectMapping(format("timestamp(%d)", MAX_SUPPORTED_DATE_TIME_PRECISION), longTimestampWithTimeZoneWriteFunction());
        }

        if (VARBINARY.equals(type)) {
            return WriteMapping.sliceMapping("mediumblob", varbinaryWriteFunction());
        }

        if (type instanceof CharType charType) {
            return WriteMapping.sliceMapping("char(" + charType.getLength() + ")", charWriteFunction());
        }

        if (type instanceof VarcharType varcharType) {
            String dataType;
            if (varcharType.isUnbounded()) {
                dataType = "longtext";
            }
            else if (varcharType.getBoundedLength() <= 255) {
                dataType = "tinytext";
            }
            else if (varcharType.getBoundedLength() <= 65535) {
                dataType = "text";
            }
            else if (varcharType.getBoundedLength() <= 16777215) {
                dataType = "mediumtext";
            }
            else {
                dataType = "longtext";
            }
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }

        if (type.equals(jsonType)) {
            return WriteMapping.sliceMapping("json", varcharWriteFunction());
        }

        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        try {
            createTable(session, tableMetadata, tableMetadata.getTable().getTableName());
        }
        catch (SQLException e) {
            if (e.getErrorCode() == ER_TABLE_EXISTS_ERROR) {
                throw new TrinoException(ALREADY_EXISTS, e);
            }
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column, ColumnPosition position)
    {
        verify(handle.getAuthorization().isEmpty(), "Unexpected authorization is required for table: %s", handle);

        RemoteTableName table = handle.asPlainTable().getRemoteTableName();

        switch (position) {
            case ColumnPosition.First _ -> addColumn(session, table, column, "FIRST");
            case ColumnPosition.After after -> addColumn(session, table, column, "AFTER " + quoted(after.columnName()));
            case ColumnPosition.Last _ -> addColumn(session, table, column, "");
        }
    }

    private void addColumn(ConnectorSession session, RemoteTableName table, ColumnMetadata column, String position)
    {
        if (column.getComment() != null) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support adding columns with comments");
        }

        try (Connection connection = connectionFactory.openConnection(session)) {
            verify(connection.getAutoCommit());
            String columnName = column.getName();
            verifyColumnName(connection.getMetaData(), columnName);
            String remoteColumnName = getIdentifierMapping().toRemoteColumnName(getRemoteIdentifiers(connection), columnName);
            String sql = format(
                    "ALTER TABLE %s ADD %s %s",
                    quoted(table),
                    getColumnDefinitionSql(session, column, remoteColumnName),
                    position);
            execute(session, connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected void renameColumn(ConnectorSession session, Connection connection, RemoteTableName remoteTableName, String remoteColumnName, String newRemoteColumnName)
            throws SQLException
    {
        String sql = format(
                "ALTER TABLE %s RENAME COLUMN %s TO %s",
                quoted(remoteTableName.getCatalogName().orElse(null), remoteTableName.getSchemaName().orElse(null), remoteTableName.getTableName()),
                quoted(remoteColumnName),
                quoted(newRemoteColumnName));
        execute(session, connection, sql);
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

    @Override
    public void renameSchema(ConnectorSession session, String schemaName, String newSchemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming schemas");
    }

    @Override
    protected void copyTableSchema(ConnectorSession session, Connection connection, String catalogName, String schemaName, String tableName, String newTableName, List<String> columnNames)
    {
        String tableCopyFormat = "CREATE TABLE %s AS SELECT * FROM %s WHERE 0 = 1";
        if (isGtidMode(connection)) {
            tableCopyFormat = "CREATE TABLE %s LIKE %s";
        }
        String sql = format(
                tableCopyFormat,
                quoted(catalogName, schemaName, newTableName),
                quoted(catalogName, schemaName, tableName));
        try {
            execute(session, connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void renameTable(ConnectorSession session, JdbcTableHandle handle, SchemaTableName newTableName)
    {
        // MySQL doesn't support specifying the catalog name in a rename. By setting the
        // catalogName parameter to null, it will be omitted in the ALTER TABLE statement.
        RemoteTableName remoteTableName = handle.asPlainTable().getRemoteTableName();
        verify(remoteTableName.getSchemaName().isEmpty());
        renameTable(session, null, remoteTableName.getCatalogName().orElse(null), remoteTableName.getTableName(), newTableName);
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
    public boolean supportsMerge()
    {
        return true;
    }

    @Override
    public boolean supportsTopN(ConnectorSession session, JdbcTableHandle handle, List<JdbcSortItem> sortOrder)
    {
        for (JdbcSortItem sortItem : sortOrder) {
            Type sortItemType = sortItem.column().getColumnType();
            if (sortItemType instanceof CharType || sortItemType instanceof VarcharType) {
                // Remote database can be case insensitive.
                return false;
            }
        }
        return true;
    }

    @Override
    protected Optional<TopNFunction> topNFunction()
    {
        return Optional.of((query, sortItems, limit) -> {
            String orderBy = sortItems.stream()
                    .flatMap(sortItem -> {
                        String ordering = sortItem.sortOrder().isAscending() ? "ASC" : "DESC";
                        String columnSorting = format("%s %s", quoted(sortItem.column().getColumnName()), ordering);

                        return switch (sortItem.sortOrder()) {
                            // In MySQL ASC implies NULLS FIRST, DESC implies NULLS LAST
                            case ASC_NULLS_FIRST, DESC_NULLS_LAST -> Stream.of(columnSorting);
                            case ASC_NULLS_LAST -> Stream.of(format("ISNULL(%s) ASC", quoted(sortItem.column().getColumnName())), columnSorting);
                            case DESC_NULLS_FIRST -> Stream.of(format("ISNULL(%s) DESC", quoted(sortItem.column().getColumnName())), columnSorting);
                        };
                    })
                    .collect(joining(", "));
            return format("%s ORDER BY %s LIMIT %s", query, orderBy, limit);
        });
    }

    @Override
    public boolean isTopNGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    public Optional<PreparedQuery> implementJoin(
            ConnectorSession session,
            JoinType joinType,
            PreparedQuery leftSource,
            Map<JdbcColumnHandle, String> leftProjections,
            PreparedQuery rightSource,
            Map<JdbcColumnHandle, String> rightProjections,
            List<ParameterizedExpression> joinConditions,
            JoinStatistics statistics)
    {
        if (joinType == JoinType.FULL_OUTER) {
            // Not supported in MySQL
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
    public Optional<PreparedQuery> legacyImplementJoin(
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
            // Not supported in MySQL
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
    protected boolean isSupportedJoinCondition(ConnectorSession session, JdbcJoinCondition joinCondition)
    {
        if (joinCondition.getOperator() == JoinCondition.Operator.IDENTICAL) {
            // Not supported in MySQL
            return false;
        }

        // Remote database can be case insensitive.
        return Stream.of(joinCondition.getLeftColumn(), joinCondition.getRightColumn())
                .map(JdbcColumnHandle::getColumnType)
                .noneMatch(type -> type instanceof CharType || type instanceof VarcharType);
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

        log.debug("Reading statistics for %s", table);
        try (Connection connection = connectionFactory.openConnection(session);
                Handle handle = Jdbi.open(connection)) {
            StatisticsDao statisticsDao = new StatisticsDao(handle);

            Long rowCount = statisticsDao.getRowCount(table);
            log.debug("Estimated row count of table %s is %s", table, rowCount);

            if (rowCount == null) {
                // Table not found, or is a view.
                return TableStatistics.empty();
            }

            TableStatistics.Builder tableStatistics = TableStatistics.builder();
            tableStatistics.setRowCount(Estimate.of(rowCount));

            Map<String, String> columnHistograms = statisticsDao.getColumnHistograms(table);
            Map<String, ColumnIndexStatistics> columnStatisticsFromIndexes = statisticsDao.getColumnIndexStatistics(table);

            if (columnHistograms.isEmpty() && columnStatisticsFromIndexes.isEmpty()) {
                log.debug("No column histograms and index statistics read");
                // No more information to work on
                return tableStatistics.build();
            }

            for (JdbcColumnHandle column : JdbcMetadata.getColumns(session, this, table)) {
                ColumnStatistics.Builder columnStatisticsBuilder = ColumnStatistics.builder();

                String columnName = column.getColumnName();
                Optional<ColumnHistogram> histogram = getColumnHistogram(columnHistograms, columnName);
                if (histogram.isPresent()) {
                    log.debug("Reading column statistics for %s, %s from histogram: %s", table, columnName, columnHistograms.get(columnName));
                    histogram.get().updateColumnStatistics(columnStatisticsBuilder);

                    // row count from INFORMATION_SCHEMA.TABLES is very inaccurate
                    rowCount = histogram.get().getUpdateRowCount(rowCount);
                }

                ColumnIndexStatistics columnIndexStatistics = columnStatisticsFromIndexes.get(columnName);
                if (columnIndexStatistics != null) {
                    log.debug("Reading column statistics for %s, %s from index statistics: %s", table, columnName, columnIndexStatistics);
                    updateColumnStatisticsFromIndexStatistics(table, columnName, columnStatisticsBuilder, columnIndexStatistics);

                    if (rowCount < columnIndexStatistics.cardinality()) {
                        // row count from INFORMATION_SCHEMA.TABLES is very inaccurate but rowCount already includes MAX(CARDINALITY) from indexes
                        // This can still happen if table's index statistics change concurrently
                        log.debug("Table %s rowCount calculated so far [%s] is less than index cardinality for %s: %s", table, rowCount, columnName, columnIndexStatistics);
                        rowCount = columnIndexStatistics.cardinality();
                    }
                }

                tableStatistics.setColumnStatistics(column, columnStatisticsBuilder.build());
            }

            tableStatistics.setRowCount(Estimate.of(rowCount));
            return tableStatistics.build();
        }
    }

    @Override
    public List<JdbcColumnHandle> getPrimaryKeys(ConnectorSession session, RemoteTableName remoteTableName)
    {
        SchemaTableName tableName = new SchemaTableName(remoteTableName.getCatalogName().orElse(null), remoteTableName.getTableName());
        List<JdbcColumnHandle> columns = getColumns(session, tableName, remoteTableName);
        try (Connection connection = connectionFactory.openConnection(session)) {
            DatabaseMetaData metaData = connection.getMetaData();

            ResultSet primaryKeys = metaData.getPrimaryKeys(remoteTableName.getCatalogName().orElse(null), remoteTableName.getSchemaName().orElse(null), remoteTableName.getTableName());

            Set<String> primaryKeyNames = new HashSet<>();
            while (primaryKeys.next()) {
                primaryKeyNames.add(primaryKeys.getString("COLUMN_NAME"));
            }
            if (primaryKeyNames.isEmpty()) {
                return ImmutableList.of();
            }
            ImmutableList.Builder<JdbcColumnHandle> primaryKeysBuilder = ImmutableList.builder();
            for (JdbcColumnHandle columnHandle : columns) {
                String name = columnHandle.getColumnName();
                if (!primaryKeyNames.contains(name)) {
                    continue;
                }
                JdbcTypeHandle handle = columnHandle.getJdbcTypeHandle();
                primaryKeysBuilder.add(new JdbcColumnHandle(
                        name,
                        // make sure the primary keys that are varchar/char relate types can be pushdown
                        new JdbcTypeHandle(handle.jdbcType(), handle.jdbcTypeName(), handle.columnSize(), handle.decimalDigits(), handle.arrayDimensions(), Optional.of(CASE_SENSITIVE)),
                        columnHandle.getColumnType()));
            }
            return primaryKeysBuilder.build();
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    private static Optional<ColumnHistogram> getColumnHistogram(Map<String, String> columnHistograms, String columnName)
    {
        return Optional.ofNullable(columnHistograms.get(columnName))
                .flatMap(histogramJson -> {
                    try {
                        return Optional.of(HISTOGRAM_CODEC.fromJson(histogramJson));
                    }
                    catch (RuntimeException e) {
                        log.warn(e, "Failed to parse column statistics histogram: %s", histogramJson);
                        return Optional.empty();
                    }
                });
    }

    private static void updateColumnStatisticsFromIndexStatistics(JdbcTableHandle table, String columnName, ColumnStatistics.Builder columnStatistics, ColumnIndexStatistics columnIndexStatistics)
    {
        // Prefer CARDINALITY from index statistics over NDV from a histogram.
        // Index column might be NULLABLE. Then CARDINALITY includes all
        columnStatistics.setDistinctValuesCount(Estimate.of(columnIndexStatistics.cardinality()));

        if (!columnIndexStatistics.nullable()) {
            double knownNullFraction = columnStatistics.build().getNullsFraction().getValue();
            if (knownNullFraction > 0) {
                log.warn("Inconsistent statistics, null fraction for a column %s, %s, that is not nullable according to index statistics: %s", table, columnName, knownNullFraction);
            }
            columnStatistics.setNullsFraction(Estimate.zero());
        }
    }

    private ColumnMapping jsonColumnMapping()
    {
        return ColumnMapping.sliceMapping(
                jsonType,
                (resultSet, columnIndex) -> jsonParse(utf8Slice(resultSet.getString(columnIndex))),
                varcharWriteFunction(),
                DISABLE_PUSHDOWN);
    }

    private static boolean isGtidMode(Connection connection)
    {
        try (java.sql.Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("SHOW VARIABLES LIKE 'gtid_mode'")) {
            if (resultSet.next()) {
                return !resultSet.getString("Value").equalsIgnoreCase("OFF");
            }

            return false;
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    private static class StatisticsDao
    {
        private final Handle handle;

        public StatisticsDao(Handle handle)
        {
            this.handle = requireNonNull(handle, "handle is null");
        }

        Long getRowCount(JdbcTableHandle table)
        {
            RemoteTableName remoteTableName = table.getRequiredNamedRelation().getRemoteTableName();
            return handle.createQuery(
                            """
                            SELECT max(row_count) FROM (
                                (SELECT TABLE_ROWS AS row_count FROM INFORMATION_SCHEMA.TABLES
                                WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table_name
                                AND TABLE_TYPE = 'BASE TABLE')
                                UNION ALL
                                (SELECT CARDINALITY AS row_count FROM INFORMATION_SCHEMA.STATISTICS
                                WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table_name
                                AND CARDINALITY IS NOT NULL)
                            ) t
                            """)
                    .bind("schema", remoteTableName.getCatalogName().orElse(null))
                    .bind("table_name", remoteTableName.getTableName())
                    .mapTo(Long.class)
                    .findOne()
                    .orElse(null);
        }

        Map<String, ColumnIndexStatistics> getColumnIndexStatistics(JdbcTableHandle table)
        {
            RemoteTableName remoteTableName = table.getRequiredNamedRelation().getRemoteTableName();
            return handle.createQuery(
                            """
                            SELECT
                                COLUMN_NAME,
                                MAX(NULLABLE) AS NULLABLE,
                                MAX(CARDINALITY) AS CARDINALITY
                            FROM INFORMATION_SCHEMA.STATISTICS
                            WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :table_name
                            AND SEQ_IN_INDEX = 1 -- first column in the index
                            AND SUB_PART IS NULL -- ignore cases where only a column prefix is indexed
                            AND CARDINALITY IS NOT NULL -- CARDINALITY might be null (https://stackoverflow.com/a/42242729/65458)
                            GROUP BY COLUMN_NAME -- there might be multiple indexes on a column
                            """)
                    .bind("schema", remoteTableName.getCatalogName().orElse(null))
                    .bind("table_name", remoteTableName.getTableName())
                    .map((rs, ctx) -> {
                        String columnName = rs.getString("COLUMN_NAME");

                        boolean nullable = rs.getString("NULLABLE").equalsIgnoreCase("YES");
                        checkState(!rs.wasNull(), "NULLABLE is null");

                        long cardinality = rs.getLong("CARDINALITY");
                        checkState(!rs.wasNull(), "CARDINALITY is null");

                        return new SimpleEntry<>(columnName, new ColumnIndexStatistics(nullable, cardinality));
                    })
                    .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        Map<String, String> getColumnHistograms(JdbcTableHandle table)
        {
            try {
                handle.execute("SELECT 1 FROM INFORMATION_SCHEMA.COLUMN_STATISTICS WHERE 0=1");
            }
            catch (UnableToExecuteStatementException e) {
                if (e.getCause() instanceof SQLSyntaxErrorException && ((SQLSyntaxErrorException) e.getCause()).getErrorCode() == ER_UNKNOWN_TABLE) {
                    // The table is available since MySQL 8
                    log.debug("INFORMATION_SCHEMA.COLUMN_STATISTICS table is not available: %s", e);
                    return ImmutableMap.of();
                }
            }

            RemoteTableName remoteTableName = table.getRequiredNamedRelation().getRemoteTableName();
            return handle.createQuery(
                            """
                            SELECT COLUMN_NAME, HISTOGRAM FROM INFORMATION_SCHEMA.COLUMN_STATISTICS
                            WHERE SCHEMA_NAME = :schema AND TABLE_NAME = :table_name
                            """)
                    .bind("schema", remoteTableName.getCatalogName().orElse(null))
                    .bind("table_name", remoteTableName.getTableName())
                    .map((rs, ctx) -> new SimpleEntry<>(rs.getString("COLUMN_NAME"), rs.getString("HISTOGRAM")))
                    .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        }
    }

    private record ColumnIndexStatistics(boolean nullable, long cardinality) { }

    // See https://dev.mysql.com/doc/refman/8.0/en/optimizer-statistics.html
    public static class ColumnHistogram
    {
        private final Optional<Double> nullFraction;
        private final Optional<String> histogramType;
        private final Optional<List<List<Object>>> buckets;

        @JsonCreator
        public ColumnHistogram(
                @JsonProperty("null-values") Optional<Double> nullFraction,
                @JsonProperty("histogram-type") Optional<String> histogramType,
                @JsonProperty("buckets") Optional<List<List<Object>>> buckets)
        {
            this.nullFraction = nullFraction;
            this.histogramType = histogramType;
            this.buckets = buckets;
        }

        public void updateColumnStatistics(ColumnStatistics.Builder columnStatistics)
        {
            nullFraction.map(Estimate::of).ifPresent(columnStatistics::setNullsFraction);
            getDistinctValuesCount().map(Estimate::of).ifPresent(columnStatistics::setDistinctValuesCount);
        }

        private Optional<Long> getDistinctValuesCount()
        {
            if (histogramType.isPresent() && buckets.isPresent()) {
                switch (histogramType.get()) {
                    case "singleton":
                        return Optional.of((long) buckets.get().size());

                    case "equi-height":
                        long distinctValues = 0;
                        for (List<?> bucket : buckets.get()) {
                            distinctValues += ((Number) bucket.get(3)).longValue();
                        }
                        return Optional.of(distinctValues);

                    default:
                        log.debug("Unsupported histogram type: %s", histogramType.get());
                }
            }
            else {
                log.debug("Unsupported histogram: type: %s, bucket count: %s", histogramType, buckets.map(List::size));
            }
            return Optional.empty();
        }

        public long getUpdateRowCount(long rowCount)
        {
            return getDistinctValuesCount()
                    .map(distinctValuesCount -> max(rowCount, distinctValuesCount))
                    .orElse(rowCount);
        }
    }
}
