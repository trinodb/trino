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
package io.trino.plugin.hsqldb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
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
import io.trino.plugin.jdbc.JdbcProcedureHandle;
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
import io.trino.plugin.jdbc.UnsupportedTypeHandling;
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
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.jdbc.CaseSensitivity.CASE_INSENSITIVE;
import static io.trino.plugin.jdbc.CaseSensitivity.CASE_SENSITIVE;
import static io.trino.plugin.jdbc.DecimalConfig.DecimalMapping.ALLOW_OVERFLOW;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalDefaultScale;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRounding;
import static io.trino.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRoundingMode;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
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
import static io.trino.plugin.jdbc.StandardColumnMappings.dateColumnMappingUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateWriteFunctionUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.defaultVarcharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timeColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.timeWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.IGNORE;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.packTimeWithTimeZone;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackOffsetMinutes;
import static io.trino.spi.type.DateTimeEncoding.unpackTimeNanos;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimeWithTimeZoneType.createTimeWithTimeZoneType;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.round;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.sql.DatabaseMetaData.columnNoNulls;
import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class HsqlDbClient
        extends BaseJdbcClient
{
    private static final Logger log = Logger.get(HsqlDbClient.class);

    private static final int MAX_SUPPORTED_DATE_TIME_PRECISION = 9;
    // HsqlDB driver returns width of time types instead of precision.
    private static final int ZERO_PRECISION_TIME_COLUMN_SIZE = 8;
    private static final int ZERO_PRECISION_TIME_WITH_TIME_ZONE_COLUMN_SIZE = 14;
    private static final int ZERO_PRECISION_TIMESTAMP_COLUMN_SIZE = 19;
    private static final int ZERO_PRECISION_TIMESTAMP_WITH_TIME_ZONE_COLUMN_SIZE = 25;

    private static final int DEFAULT_VARCHAR_LENGTH = 2_000_000_000;

    // MariaDB Error Codes https://mariadb.com/kb/en/mariadb-error-codes/
    private static final int PARSE_ERROR = 1064;

    private final boolean statisticsEnabled;
    private final ConnectorExpressionRewriter<ParameterizedExpression> connectorExpressionRewriter;
    private final AggregateFunctionRewriter<JdbcExpression, ?> aggregateFunctionRewriter;

    private static final PredicatePushdownController HSQLDB_CHARACTER_PUSHDOWN = (session, domain) -> {
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
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd");

    private static final DateTimeFormatter TIME_OFFSET_FORMATTER= new DateTimeFormatterBuilder()
            .appendPattern("HH:mm:ss")
            .appendFraction(NANO_OF_SECOND, 0, MAX_SUPPORTED_DATE_TIME_PRECISION, true)
            .appendPattern(" ")
            .appendZoneId()
            .toFormatter();

    private static final DateTimeFormatter DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss")
            .appendFraction(NANO_OF_SECOND, 0, MAX_SUPPORTED_DATE_TIME_PRECISION, true)
            .toFormatter();

    private static final DateTimeFormatter DATE_TIME_OFFSET_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss")
            .appendFraction(NANO_OF_SECOND, 0, MAX_SUPPORTED_DATE_TIME_PRECISION, true)
            .appendPattern(" ")
            .appendZoneId()
            .toFormatter();

    // Dates prior to the Gregorian calendar switch in 1582 can cause incorrect results when pushed down,
    // so we disable predicate push down when the domain contains values prior to 1583
    private static final Instant GREGORIAN_SWITCH_INSTANT = Instant.parse("1583-01-01T00:00:00Z");
    private static final OffsetDateTime GREGORIAN_SWITCH_DATETIMEOFFSET = OffsetDateTime.ofInstant(GREGORIAN_SWITCH_INSTANT, ZoneId.of("Z"));
    private static final LongTimestampWithTimeZone LONG_DATETIMEOFFSET_DISABLE_VALUE =
            LongTimestampWithTimeZone.fromEpochSecondsAndFraction(
                    GREGORIAN_SWITCH_INSTANT.getEpochSecond(),
                    (long) GREGORIAN_SWITCH_INSTANT.getNano() * PICOSECONDS_PER_NANOSECOND,
                    UTC_KEY);
    private static final long SHORT_DATETIMEOFFSET_DISABLE_VALUE = GREGORIAN_SWITCH_INSTANT.toEpochMilli();

    private static final PredicatePushdownController HSQLDB_DATE_TIME_PUSHDOWN = (session, domain) -> {
        Domain simplifiedDomain = domain.simplify(getDomainCompactionThreshold(session));
        for (Range range : simplifiedDomain.getValues().getRanges().getOrderedRanges()) {
            Range disableRange = range.getType().getJavaType().equals(LongTimestampWithTimeZone.class)
                    ? Range.lessThan(range.getType(), LONG_DATETIMEOFFSET_DISABLE_VALUE)
                    : Range.lessThan(range.getType(), SHORT_DATETIMEOFFSET_DISABLE_VALUE);

            // If there is any overlap of any predicate range and (-inf, 1583), disable push down
            if (range.overlaps(disableRange)) {
                return DISABLE_PUSHDOWN.apply(session, domain);
            }
        }

        return FULL_PUSHDOWN.apply(session, domain);
    };

    @Inject
    public HsqlDbClient(
            BaseJdbcConfig config,
            JdbcStatisticsConfig statisticsConfig,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier)
    {
        super("\"", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, queryModifier, true);

        JdbcTypeHandle bigintTypeHandle = new JdbcTypeHandle(Types.BIGINT, Optional.of("bigint"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
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
                .build();
        this.statisticsEnabled = statisticsConfig.isEnabled();
        this.aggregateFunctionRewriter = new AggregateFunctionRewriter<>(
                connectorExpressionRewriter,
                ImmutableSet.<AggregateFunctionRule<JdbcExpression, ParameterizedExpression>>builder()
                        .add(new ImplementCountAll(bigintTypeHandle))
                        .add(new ImplementCount(bigintTypeHandle))
                        .add(new ImplementMinMax(false))
                        .add(new ImplementSum(HsqlDbClient::toTypeHandle))
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
    protected Map<String, CaseSensitivity> getCaseSensitivityForColumns(ConnectorSession session, Connection connection, JdbcTableHandle tableHandle)
    {
        if (tableHandle.isSynthetic()) {
            return ImmutableMap.of();
        }
        PreparedQuery preparedQuery = new PreparedQuery(format("SELECT * FROM %s", quoted(tableHandle.asPlainTable().getRemoteTableName())), ImmutableList.of());

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
            if (e.getErrorCode() == 1004) {
                throw new TableNotFoundException(tableHandle.asPlainTable().getSchemaTableName());
            }
            throw new TrinoException(JDBC_ERROR, "Failed to get case sensitivity for columns. " + firstNonNull(e.getMessage(), e), e);
        }
    }

    @Override
    public void setColumnComment(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Optional<String> comment)
    {
        // Oracle doesn't support prepared statement for COMMENT statement
        String sql = format(
                "COMMENT ON COLUMN %s.%s IS %s",
                quoted(handle.asPlainTable().getRemoteTableName()),
                quoted(column.getColumnName()),
                comment.map(BaseJdbcClient::varcharLiteral).orElse("NULL"));
        execute(session, sql);
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
    protected ResultSet getAllTableColumns(Connection connection, Optional<String> remoteSchemaName)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        return metadata.getColumns(
                metadata.getConnection().getCatalog(),
                remoteSchemaName.orElse(null),
                null,
                null);
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
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }

        switch (typeHandle.jdbcType()) {
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

            case Types.DOUBLE:
                return Optional.of(doubleColumnMapping());
            case Types.NUMERIC:
            case Types.DECIMAL:
                int decimalDigits = typeHandle.requiredDecimalDigits();
                int decimalPrecision = typeHandle.requiredColumnSize();
                if (getDecimalRounding(session) == ALLOW_OVERFLOW && decimalPrecision > Decimals.MAX_PRECISION) {
                    int scale = min(decimalDigits, getDecimalDefaultScale(session));
                    return Optional.of(decimalColumnMapping(createDecimalType(Decimals.MAX_PRECISION, scale), getDecimalRoundingMode(session)));
                }
                decimalPrecision = decimalPrecision + max(-decimalDigits, 0); // Map decimal(p, -s) (negative scale) to decimal(p+s, 0).
                if (decimalPrecision > Decimals.MAX_PRECISION) {
                    break;
                }
                return Optional.of(decimalColumnMapping(createDecimalType(decimalPrecision, max(decimalDigits, 0))));

            case Types.CHAR:
                return Optional.of(charColumnMapping(typeHandle.requiredColumnSize(), typeHandle.caseSensitivity()));
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                // varchar columns get created as varchar(default_length) in HsqlDB
                if (typeHandle.requiredColumnSize() == DEFAULT_VARCHAR_LENGTH) {
                    return Optional.of(varcharColumnMapping(createUnboundedVarcharType(), typeHandle.caseSensitivity()));
                }
                return Optional.of(defaultVarcharColumnMapping(typeHandle.requiredColumnSize(), true));

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return Optional.of(varbinaryColumnMapping());

            case Types.DATE:
                return Optional.of(dateColumnMappingUsingLocalDate());

            case Types.TIME:
                TimeType timeType = createTimeType(getTimePrecision(typeHandle.requiredColumnSize()));
                return Optional.of(timeColumnMapping(timeType));
            case Types.TIME_WITH_TIMEZONE:
                int timePrecision = getTimeWithTimeZonePrecision(typeHandle.requiredColumnSize());
                return Optional.of(timeWithTimeZoneColumnMapping(timePrecision));

            case Types.TIMESTAMP:
                TimestampType timestampType = createTimestampType(getTimestampPrecision(typeHandle.requiredColumnSize()));
                return Optional.of(timestampColumnMapping(timestampType));
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return Optional.of(timestampWithTimeZoneColumnMapping(getTimestampWithTimeZonePrecision(typeHandle.requiredColumnSize())));
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
            String dataType = format("char(%s)", charType.getLength());
            return WriteMapping.sliceMapping(dataType, charWriteFunction());
        }
        if (type instanceof VarcharType varcharType) {
            String dataType = varcharType.isUnbounded() ? "varchar(32768)" : format("varchar(%s)", varcharType.getBoundedLength());
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }
        if (type == VARBINARY) {
            return WriteMapping.sliceMapping("varbinary", varbinaryWriteFunction());
        }
        if (type == DATE) {
            return WriteMapping.longMapping("date", dateWriteFunctionUsingLocalDate());
        }

        if (type instanceof TimeType timeType) {
            if (timeType.getPrecision() <= MAX_SUPPORTED_DATE_TIME_PRECISION) {
                return WriteMapping.longMapping(format("time(%s)", timeType.getPrecision()), timeWriteFunction(timeType.getPrecision()));
            }
            return WriteMapping.longMapping(format("time(%s)", MAX_SUPPORTED_DATE_TIME_PRECISION), timeWriteFunction(MAX_SUPPORTED_DATE_TIME_PRECISION));
        }
        if (type instanceof TimeWithTimeZoneType timeWithZoneType) {
            if (timeWithZoneType.getPrecision() <= MAX_SUPPORTED_DATE_TIME_PRECISION) {
                return WriteMapping.longMapping(format("time(%s) with time zone", timeWithZoneType.getPrecision()), timeWithTimeZoneWriteFunction());
            }
            return WriteMapping.longMapping(format("time(%s) with time zone", MAX_SUPPORTED_DATE_TIME_PRECISION), timeWithTimeZoneWriteFunction());
        }

        if (type instanceof TimestampType timestampType) {
            int timestampPrecision = min(timestampType.getPrecision(), MAX_SUPPORTED_DATE_TIME_PRECISION);
            String dataType = format("timestamp(%s)", timestampPrecision);
            if (timestampType.getPrecision() <= TimestampType.MAX_SHORT_PRECISION) {
                return WriteMapping.longMapping(dataType, timestampWriteFunction(timestampType));
            }
            return WriteMapping.objectMapping(dataType, longTimestampWriteFunction(timestampType, timestampPrecision));
        }
        if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
            int timestampPrecision = min(timestampWithTimeZoneType.getPrecision(), MAX_SUPPORTED_DATE_TIME_PRECISION);
            String dataType = format("timestamp(%d) with time zone", timestampPrecision);
            if (timestampPrecision <= TimestampWithTimeZoneType.MAX_SHORT_PRECISION) {
                return WriteMapping.longMapping(dataType, shortTimestampWithTimeZoneWriteFunction());
            }
            return WriteMapping.objectMapping(dataType, longTimestampWithTimeZoneWriteFunction());
        }

        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    private static ColumnMapping charColumnMapping(int columnSize, Optional<CaseSensitivity> caseSensitivity)
    {
        if (columnSize > CharType.MAX_LENGTH) {
            return varcharColumnMapping(columnSize, caseSensitivity);
        }
        return charColumnMapping(createCharType(columnSize), caseSensitivity);
    }

    private static ColumnMapping timestampColumnMapping(TimestampType timestampType)
    {
        if (timestampType.getPrecision() <= TimestampType.MAX_SHORT_PRECISION) {
            return ColumnMapping.longMapping(
                    timestampType,
                    timestampReadFunction(timestampType),
                    timestampWriteFunction(timestampType));
        }
        checkArgument(timestampType.getPrecision() <= MAX_SUPPORTED_DATE_TIME_PRECISION, "Precision is out of range: %s", timestampType.getPrecision());
        return ColumnMapping.objectMapping(
                timestampType,
                longTimestampReadFunction(timestampType),
                longTimestampWriteFunction(timestampType, timestampType.getPrecision()));
    }

    private static LongReadFunction timestampReadFunction(TimestampType timestampType)
    {
        return (resultSet, columnIndex) -> {
            LocalDateTime localDateTime = resultSet.getObject(columnIndex, LocalDateTime.class);
            return toTrinoTimestamp(timestampType, localDateTime);
        };
    }

    public static long toTrinoTimestamp(TimestampType timestampType, LocalDateTime localDateTime)
    {
        long precision = timestampType.getPrecision();
        checkArgument(precision <= TimestampType.MAX_SHORT_PRECISION, "Precision is out of range: %s", precision);
        long epochMicros = localDateTime.toEpochSecond(UTC) * MICROSECONDS_PER_SECOND
                + localDateTime.getNano() / NANOSECONDS_PER_MICROSECOND;
        verify(
                epochMicros == round(epochMicros, TimestampType.MAX_SHORT_PRECISION - timestampType.getPrecision()),
                "Invalid value of epochMicros for precision %s: %s",
                precision,
                epochMicros);
        return epochMicros;
    }

    private static LongWriteFunction timestampWriteFunction(TimestampType timestampType)
    {
        checkArgument(timestampType.getPrecision() <= TimestampType.MAX_SHORT_PRECISION, "Precision is out of range: %s", timestampType.getPrecision());
        return LongWriteFunction.of(
                Types.TIMESTAMP,
                (statement, index, value) -> statement.setObject(index, fromTrinoTimestamp(value)));
    }

    private static LocalDateTime fromTrinoTimestamp(long epochMicros)
    {
        long epochSecond = floorDiv(epochMicros, MICROSECONDS_PER_SECOND);
        int nanoFraction = floorMod(epochMicros, MICROSECONDS_PER_SECOND) * NANOSECONDS_PER_MICROSECOND;
        return LocalDateTime.ofEpochSecond(epochSecond, nanoFraction, UTC);
    }

    private static ObjectReadFunction longTimestampReadFunction(TimestampType timestampType)
    {
        return ObjectReadFunction.of(
                LongTimestamp.class,
                (resultSet, columnIndex) -> {
                    LocalDateTime localDateTime = resultSet.getObject(columnIndex, LocalDateTime.class);
                    return toLongTrinoTimestamp(timestampType, localDateTime);
                });
    }

    public static LongTimestamp toLongTrinoTimestamp(TimestampType timestampType, LocalDateTime localDateTime)
    {
        long precision = timestampType.getPrecision();
        checkArgument(precision > TimestampType.MAX_SHORT_PRECISION, "Precision is out of range: %s", precision);
        long epochMicros = localDateTime.toEpochSecond(UTC) * MICROSECONDS_PER_SECOND
                + localDateTime.getNano() / NANOSECONDS_PER_MICROSECOND;
        int picosOfMicro = (localDateTime.getNano() % NANOSECONDS_PER_MICROSECOND) * PICOSECONDS_PER_NANOSECOND;
        verify(
                picosOfMicro == round(picosOfMicro, TimestampType.MAX_PRECISION - timestampType.getPrecision()),
                "Invalid value of picosOfMicro for precision %s: %s",
                precision,
                picosOfMicro);
        return new LongTimestamp(epochMicros, picosOfMicro);
    }

    private static ObjectWriteFunction longTimestampWriteFunction(TimestampType timestampType, int roundToPrecision)
    {
        checkArgument(timestampType.getPrecision() > TimestampType.MAX_SHORT_PRECISION, "Precision is out of range: %s", timestampType.getPrecision());
        checkArgument(
                6 <= roundToPrecision && roundToPrecision <= 9 && roundToPrecision <= timestampType.getPrecision(),
                "Invalid roundToPrecision for %s: %s",
                timestampType,
                roundToPrecision);

        return ObjectWriteFunction.of(
                LongTimestamp.class,
                (statement, index, value) -> statement.setObject(index, fromLongTrinoTimestamp(value, roundToPrecision)));
    }

    private static LocalDateTime fromLongTrinoTimestamp(LongTimestamp timestamp, int precision)
    {
        // The code below assumes precision is not less than microseconds and not more than picoseconds.
        checkArgument(6 <= precision && precision <= 9, "Unsupported precision: %s", precision);
        long epochSeconds = floorDiv(timestamp.getEpochMicros(), MICROSECONDS_PER_SECOND);
        int microsOfSecond = floorMod(timestamp.getEpochMicros(), MICROSECONDS_PER_SECOND);
        long picosOfMicro = round(timestamp.getPicosOfMicro(), TimestampType.MAX_PRECISION - precision);
        int nanosOfSecond = (microsOfSecond * NANOSECONDS_PER_MICROSECOND) + toIntExact(picosOfMicro / PICOSECONDS_PER_NANOSECOND);
        return LocalDateTime.ofEpochSecond(epochSeconds, nanosOfSecond, UTC);
    }

    public static ColumnMapping timestampWithTimeZoneColumnMapping(int precision)
    {
        checkArgument(precision <= MAX_SUPPORTED_DATE_TIME_PRECISION, "Precision is out of range: %s", precision);
        if (precision <= TimestampWithTimeZoneType.MAX_SHORT_PRECISION) {
            return ColumnMapping.longMapping(
                    createTimestampWithTimeZoneType(precision),
                    shortTimestampWithTimeZoneReadFunction(),
                    shortTimestampWithTimeZoneWriteFunction());
        }
        return ColumnMapping.objectMapping(
                createTimestampWithTimeZoneType(precision),
                longTimestampWithTimeZoneReadFunction(),
                longTimestampWithTimeZoneWriteFunction());
    }

    private static LongReadFunction shortTimestampWithTimeZoneReadFunction()
    {
        return (resultSet, columnIndex) -> {
            OffsetDateTime offsetDateTime = resultSet.getObject(columnIndex, OffsetDateTime.class);
            ZonedDateTime zonedDateTime = offsetDateTime.toZonedDateTime();
            return packDateTimeWithZone(zonedDateTime.toInstant().toEpochMilli(), zonedDateTime.getZone().getId());
        };
    }

    private static LongWriteFunction shortTimestampWithTimeZoneWriteFunction()
    {
        return (statement, index, value) -> {
            long millisUtc = unpackMillisUtc(value);
            TimeZoneKey timeZoneKey = unpackZoneKey(value);
            statement.setObject(index, OffsetDateTime.ofInstant(Instant.ofEpochMilli(millisUtc), timeZoneKey.getZoneId()));
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
                            TimeZoneKey.getTimeZoneKey(offsetDateTime.toZonedDateTime().getZone().getId()));
                });
    }

    private static ObjectWriteFunction longTimestampWithTimeZoneWriteFunction()
    {
        return ObjectWriteFunction.of(
                LongTimestampWithTimeZone.class,
                (statement, index, value) -> {
                    long epochMillis = value.getEpochMillis();
                    long epochSeconds = floorDiv(epochMillis, MILLISECONDS_PER_SECOND);
                    int nanoAdjustment = floorMod(epochMillis, MILLISECONDS_PER_SECOND) * NANOSECONDS_PER_MILLISECOND + value.getPicosOfMilli() / PICOSECONDS_PER_NANOSECOND;
                    ZoneId zoneId = getTimeZoneKey(value.getTimeZoneKey()).getZoneId();
                    Instant instant = Instant.ofEpochSecond(epochSeconds, nanoAdjustment);
                    statement.setObject(index, OffsetDateTime.ofInstant(instant, zoneId));
                });
    }

    private static ColumnMapping charColumnMapping(CharType charType, Optional<CaseSensitivity> caseSensitivity)
    {
        requireNonNull(charType, "charType is null");
        PredicatePushdownController pushdownController = caseSensitivity.orElse(CASE_INSENSITIVE) == CASE_SENSITIVE
                ? HSQLDB_CHARACTER_PUSHDOWN
                : CASE_INSENSITIVE_CHARACTER_PUSHDOWN;
        return ColumnMapping.sliceMapping(charType, charReadFunction(charType), charWriteFunction(), pushdownController);
    }

    private static ColumnMapping varcharColumnMapping(int columnSize, Optional<CaseSensitivity> caseSensitivity)
    {
        if (columnSize > VarcharType.MAX_LENGTH) {
            return varcharColumnMapping(createUnboundedVarcharType(), caseSensitivity);
        }
        return varcharColumnMapping(createVarcharType(columnSize), caseSensitivity);
    }

    private static ColumnMapping varcharColumnMapping(VarcharType varcharType, Optional<CaseSensitivity> caseSensitivity)
    {
        PredicatePushdownController pushdownController = caseSensitivity.orElse(CASE_INSENSITIVE) == CASE_SENSITIVE
                ? HSQLDB_CHARACTER_PUSHDOWN
                : CASE_INSENSITIVE_CHARACTER_PUSHDOWN;
        return ColumnMapping.sliceMapping(varcharType, varcharReadFunction(varcharType), varcharWriteFunction(), pushdownController);
    }

    @Override
    protected void renameColumn(ConnectorSession session, Connection connection, RemoteTableName remoteTableName, String remoteColumnName, String newRemoteColumnName)
            throws SQLException
    {
        try {
            String sql = format(
                    "ALTER TABLE %s ALTER COLUMN %s RENAME TO %s",
                    quoted(remoteTableName.getCatalogName().orElse(null), remoteTableName.getSchemaName().orElse(null), remoteTableName.getTableName()),
                    quoted(remoteColumnName),
                    quoted(newRemoteColumnName));
            execute(session, connection, sql);
        }
        catch (SQLSyntaxErrorException syntaxError) {
            throw syntaxError;
        }
    }

    @Override
    public void setColumnType(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Type type)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support setting column types");
    }

    @Override
    protected void copyTableSchema(ConnectorSession session, Connection connection, String catalogName, String schemaName, String tableName, String newTableName, List<String> columnNames)
    {
        // Copy all columns for enforcing NOT NULL option in the temp table
        String tableCopyFormat = "CREATE TABLE %s AS (SELECT %s FROM %s) WITH NO DATA";
        String sql = format(
                tableCopyFormat,
                quoted(catalogName, schemaName, newTableName),
                columnNames.stream()
                        .map(this::quoted)
                        .collect(joining(", ")),
                quoted(catalogName, schemaName, tableName));
        try {
            execute(session, connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
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
    protected void renameTable(ConnectorSession session, String catalogName, String schemaName, String tableName, SchemaTableName newTable)
    {
        if (!schemaName.equalsIgnoreCase(newTable.getSchemaName())) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming tables across schemas");
        }
        super.renameTable(session, catalogName, schemaName, tableName, newTable);
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
                            // In MariaDB ASC implies NULLS FIRST, DESC implies NULLS LAST
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
            // Not supported in MariaDB
            return Optional.empty();
        }
        return super.implementJoin(session, joinType, leftSource, leftProjections, rightSource, rightProjections, joinConditions, statistics);
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
            // Not supported in MariaDB
            return Optional.empty();
        }
        return super.legacyImplementJoin(session, joinType, leftSource, rightSource, joinConditions, rightAssignments, leftAssignments, statistics);
    }

    @Override
    protected boolean isSupportedJoinCondition(ConnectorSession session, JdbcJoinCondition joinCondition)
    {
        // Remote database can be case insensitive.
        return Stream.of(joinCondition.getLeftColumn(), joinCondition.getRightColumn())
                .map(JdbcColumnHandle::getColumnType)
                .noneMatch(type -> type instanceof CharType || type instanceof VarcharType);
    }

    private static int getTimePrecision(int timeColumnSize)
    {
        return getTimePrecision(timeColumnSize, ZERO_PRECISION_TIME_COLUMN_SIZE);
    }

    private static int getTimeWithTimeZonePrecision(int timeColumnSize)
    {
        return getTimePrecision(timeColumnSize, ZERO_PRECISION_TIME_WITH_TIME_ZONE_COLUMN_SIZE) ;
    }

    private static int getTimestampPrecision(int timeColumnSize)
    {
        return getTimePrecision(timeColumnSize, ZERO_PRECISION_TIMESTAMP_COLUMN_SIZE);
    }

    private static int getTimestampWithTimeZonePrecision(int timeColumnSize)
    {
        return getTimePrecision(timeColumnSize, ZERO_PRECISION_TIMESTAMP_WITH_TIME_ZONE_COLUMN_SIZE) ;
    }

    private static int getTimePrecision(int timeColumnSize, int zeroPrecisionColumnSize)
    {
        if (timeColumnSize == zeroPrecisionColumnSize) {
            return 0;
        }
        int timePrecision = timeColumnSize - zeroPrecisionColumnSize - 1;
        verify(1 <= timePrecision && timePrecision <= MAX_SUPPORTED_DATE_TIME_PRECISION, "Unexpected time precision %s calculated from time column size %s", timePrecision, timeColumnSize);
        return timePrecision;
    }

    @Override
    public JdbcProcedureHandle getProcedureHandle(ConnectorSession session, JdbcProcedureHandle.ProcedureQuery procedureQuery)

    {
        try (Connection connection = connectionFactory.openConnection(session);
                CallableStatement statement = queryBuilder.callProcedure(this, session, connection, procedureQuery);
                ResultSet resultSet = statement.executeQuery()) {
            ResultSetMetaData metadata = resultSet.getMetaData();
            if (metadata == null) {
                throw new TrinoException(NOT_SUPPORTED, "Procedure not supported: ResultSetMetaData not available for query: " + procedureQuery.query());
            }
            JdbcProcedureHandle procedureHandle = new JdbcProcedureHandle(procedureQuery, getColumns(session, connection, metadata));
            if (statement.getMoreResults()) {
                throw new TrinoException(NOT_SUPPORTED, "Procedure has multiple ResultSets for query: " + procedureQuery.query());
            }
            return procedureHandle;
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, "Failed to get table handle for procedure query. " + firstNonNull(e.getMessage(), e), e);
        }
    }

    private static ColumnMapping timeWithTimeZoneColumnMapping(int precision)
    {
        // HsqlDB supports timestamp with time zone precision up to nanoseconds
        checkArgument(precision <= MAX_SUPPORTED_DATE_TIME_PRECISION, "unsupported precision value %s", precision);
        return ColumnMapping.longMapping(
                createTimeWithTimeZoneType(precision),
                timeWithTimeZoneReadFunction(),
                timeWithTimeZoneWriteFunction());
    }

    public static LongReadFunction timeWithTimeZoneReadFunction()
    {
        return (resultSet, columnIndex) -> {
            OffsetTime time = resultSet.getObject(columnIndex, OffsetTime.class);
            long nanosOfDay = time.toLocalTime().toNanoOfDay();
            verify(nanosOfDay < NANOSECONDS_PER_DAY, "Invalid value of nanosOfDay: %s", nanosOfDay);
            int offset = time.getOffset().getTotalSeconds() / 60;
            return packTimeWithTimeZone(nanosOfDay, offset);
        };
    }

    public static LongWriteFunction timeWithTimeZoneWriteFunction()
    {
        return LongWriteFunction.of(Types.TIME_WITH_TIMEZONE, (statement, index, packedTime) -> {
            long nanosOfDay = unpackTimeNanos(packedTime);
            verify(nanosOfDay < NANOSECONDS_PER_DAY, "Invalid value of nanosOfDay: %s", nanosOfDay);
            ZoneOffset offset = ZoneOffset.ofTotalSeconds(unpackOffsetMinutes(packedTime) * 60);
            statement.setObject(index, OffsetTime.of(LocalTime.ofNanoOfDay(nanosOfDay), offset));
        });
    }

    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        if (tableHandle.getColumns().isPresent()) {
            return tableHandle.getColumns().get();
        }
        checkArgument(tableHandle.isNamedRelation(), "Cannot get columns for %s", tableHandle);
        SchemaTableName schemaTableName = tableHandle.getRequiredNamedRelation().getSchemaTableName();
        RemoteTableName remoteTableName = tableHandle.getRequiredNamedRelation().getRemoteTableName();
        String table = remoteTableName.getTableName();

        try (Connection connection = connectionFactory.openConnection(session)) {
            try (ResultSet resultSet = getColumns(tableHandle, connection.getMetaData())) {
                int allColumns = 0;
                List<JdbcColumnHandle> columns = new ArrayList<>();
                while (resultSet.next()) {
                    allColumns++;
                    String columnName = resultSet.getString("COLUMN_NAME");
                    int dataType = getInteger(resultSet, "DATA_TYPE").orElseThrow(() -> new IllegalStateException("DATA_TYPE is null"));
                    String typeName = resultSet.getString("TYPE_NAME");
                    Optional<Integer> columnSize = getInteger(resultSet, "COLUMN_SIZE");
                    log.info("Mapping data type of Table: %s on Column: '%s' Column  size: %s *****************************************************************", table, columnName, columnSize.orElse(0));
                    Optional<Integer> decimalDigits = getInteger(resultSet, "DECIMAL_DIGITS");
                    JdbcTypeHandle typeHandle = new JdbcTypeHandle(
                            dataType,
                            Optional.of(typeName),
                            columnSize,
                            decimalDigits,
                            Optional.empty(),
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

}
