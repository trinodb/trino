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
package io.trino.plugin.cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.jdbc.Row;
import io.trino.jdbc.TrinoIntervalDayTime;
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
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.plugin.jdbc.PredicatePushdownController;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.ReadFunction;
import io.trino.plugin.jdbc.SliceWriteFunction;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.ObjectReadFunction;
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
import io.trino.plugin.jdbc.expression.RewriteIn;
import io.trino.plugin.jdbc.expression.RewriteLikeEscapeWithCaseSensitivity;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlock;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.JoinCondition;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.LongTimeWithTimeZone;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SqlTimeWithTimeZone;
import io.trino.spi.type.SqlTimestampWithTimeZone;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeDescriptor;
import io.trino.spi.type.VarcharType;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.UnableToExecuteStatementException;

import java.sql.Array;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.time.Instant;
import java.time.LocalDate;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
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
import static io.trino.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateColumnMappingUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateWriteFunctionUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.defaultCharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.defaultVarcharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longTimestampWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.realColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timeColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.timeWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.block.RowValueBuilder.buildRowValue;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackOffsetMinutes;
import static io.trino.spi.type.DateTimeEncoding.unpackTimeNanos;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.StandardTypes.INTERVAL_DAY_TO_SECOND;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimestampType.createTimestampType;
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
import static java.sql.Types.JAVA_OBJECT;
import static java.sql.Types.TIMESTAMP_WITH_TIMEZONE;
import static java.sql.Types.TIME_WITH_TIMEZONE;
import static java.lang.String.join;
import static java.time.format.DateTimeFormatter.ISO_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class ClusterClient
        extends BaseJdbcClient
{
    private static final Logger log = Logger.get(ClusterClient.class);

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

    private final TypeManager typeManager;
    private final Type jsonType;
    private final Type intervalDayTimeType;
    private final boolean statisticsEnabled;
    private final ConnectorExpressionRewriter<ParameterizedExpression> connectorExpressionRewriter;
    private final AggregateFunctionRewriter<JdbcExpression, ?> aggregateFunctionRewriter;

    private static final Pattern NESTED_DATA_TYPE_METADATA_ESCAPE_PATTERN = Pattern.compile("(\\s*)(?<fieldname>(\\w(\\.\\w)*)+)(?=(\\s+\\w))");

    private static final PredicatePushdownController CLUSTER_CHARACTER_PUSHDOWN = (session, domain) -> {
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
    public ClusterClient(
            BaseJdbcConfig config,
            JdbcStatisticsConfig statisticsConfig,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            TypeManager typeManager,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier)
    {
        super("\"", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, queryModifier, true);
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.jsonType = typeManager.getType(new TypeDescriptor(StandardTypes.JSON));
        this.intervalDayTimeType = typeManager.getType(new TypeDescriptor(INTERVAL_DAY_TO_SECOND));
        this.statisticsEnabled = statisticsConfig.isEnabled();

        this.connectorExpressionRewriter = JdbcConnectorExpressionRewriterBuilder.newBuilder()
                .addStandardRules(this::quoted)
                // No "real" on the list; pushdown on REAL is disabled also in toColumnMapping
                .add(new RewriteIn())
                .withTypeClass("integer_type", ImmutableSet.of("tinyint", "smallint", "integer", "bigint"))
                .withTypeClass("numeric_type", ImmutableSet.of("tinyint", "smallint", "integer", "bigint", "decimal", "real", "double"))
                .map("$equal(left, right)").to("left = right")
                .map("$not_equal(left, right)").to("left <> right")
                .map("$is_distinct_from(left, right)").to("left IS DISTINCT FROM right")
                .map("$less_than(left, right)").to("left < right")
                .map("$less_than_or_equal(left, right)").to("left <= right")
                .map("$greater_than(left, right)").to("left > right")
                .map("$greater_than_or_equal(left, right)").to("left >= right")
                .map("$add(left, right)").to("left + right")
                .map("$subtract(left, right)").to("left - right")
                .map("$multiply(left, right)").to("left * right")
                .map("$divide(left, right)").to("left / right")
                .map("$modulus(left, right)").to("left % right")
                .map("$negate(value)").to("-value")
                .map("$like(value: varchar, pattern: varchar): boolean").to("value LIKE pattern")
                .map("$like(value: varchar, pattern: varchar, escape: varchar(1)): boolean").to("value LIKE pattern ESCAPE escape")
                .map("$not($is_null(value))").to("value IS NOT NULL")
                .map("$not(value: boolean)").to("NOT value")
                .map("$is_null(value)").to("value IS NULL")
                .map("$nullif(first, second)").to("NULLIF(first, second)")
                .add(new ClusterRewriteLikeWithCaseSensitivity())
                .add(new RewriteLikeEscapeWithCaseSensitivity())
                .build();

        JdbcTypeHandle bigintTypeHandle = new JdbcTypeHandle(Types.BIGINT, Optional.of("bigint"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        this.aggregateFunctionRewriter = new AggregateFunctionRewriter<>(
                this.connectorExpressionRewriter,
                ImmutableSet.<AggregateFunctionRule<JdbcExpression, ParameterizedExpression>>builder()
                        .add(new ImplementCountAll(bigintTypeHandle))
                        .add(new ImplementCount(bigintTypeHandle))
                        .add(new ImplementMinMax(false))
                        .add(new ImplementCountDistinct(bigintTypeHandle, false))
                        .add(new ImplementSum(ClusterClient::convertType))
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
    public Optional<ParameterizedExpression> convertPredicate(ConnectorSession session, ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        return connectorExpressionRewriter.rewrite(session, expression, assignments);
    }

    private static Optional<JdbcTypeHandle> convertType(Type type)
    {
        requireNonNull(type, "type is null");
        String typeName = type.getBaseName();
        if (typeName.equals(StandardTypes.JSON)) {
            return Optional.of(getJDBCTypeHandle(Types.JAVA_OBJECT, Optional.of(typeName), Optional.empty(), Optional.empty(), Optional.empty()));
        }
        return switch (typeName) {
            case StandardTypes.BOOLEAN -> Optional.of(getJDBCTypeHandle(Types.BOOLEAN, Optional.of(typeName)));
            case StandardTypes.TINYINT -> Optional.of(getJDBCTypeHandle(Types.TINYINT, Optional.of(typeName)));
            case StandardTypes.SMALLINT -> Optional.of(getJDBCTypeHandle(Types.SMALLINT, Optional.of(typeName)));
            case StandardTypes.INTEGER -> Optional.of(getJDBCTypeHandle(Types.INTEGER, Optional.of(typeName)));
            case StandardTypes.BIGINT -> Optional.of(getJDBCTypeHandle(Types.BIGINT, Optional.of(typeName)));
            case StandardTypes.REAL -> Optional.of(getJDBCTypeHandle(Types.REAL, Optional.of(typeName)));
            case StandardTypes.DOUBLE -> Optional.of(getJDBCTypeHandle(Types.DOUBLE, Optional.of(typeName)));
            case StandardTypes.DECIMAL -> Optional.of(getJDBCTypeHandle(Types.NUMERIC, Optional.of(typeName), Optional.of(((DecimalType) type).getPrecision()), Optional.of(((DecimalType) type).getScale()), Optional.empty()));
            case StandardTypes.CHAR -> Optional.of(getJDBCTypeHandle(Types.CHAR, Optional.of(typeName), Optional.of(((CharType) type).getLength()), Optional.empty(), Optional.empty()));
            case StandardTypes.VARCHAR -> Optional.of(getJDBCTypeHandle(Types.VARCHAR, Optional.of(typeName), Optional.of(((VarcharType) type).getLength().orElse(Integer.MAX_VALUE)), Optional.empty(), Optional.empty()));
            case StandardTypes.VARBINARY -> Optional.of(getJDBCTypeHandle(Types.VARBINARY, Optional.of(typeName)));
            case StandardTypes.DATE -> Optional.of(getJDBCTypeHandle(Types.DATE, Optional.of(typeName)));
            case StandardTypes.TIME -> Optional.of(getJDBCTypeHandle(Types.TIME, Optional.of(typeName), Optional.of(((TimeType) type).getPrecision()), Optional.empty(), Optional.empty()));
            case StandardTypes.TIME_WITH_TIME_ZONE -> Optional.of(getJDBCTypeHandle(Types.TIME_WITH_TIMEZONE, Optional.of(typeName), Optional.of(((TimeWithTimeZoneType) type).getPrecision()), Optional.empty(), Optional.empty()));
            case StandardTypes.TIMESTAMP -> Optional.of(getJDBCTypeHandle(Types.TIMESTAMP, Optional.of(typeName), Optional.of(((TimestampType) type).getPrecision()), Optional.empty(), Optional.empty()));
            case StandardTypes.TIMESTAMP_WITH_TIME_ZONE -> Optional.of(getJDBCTypeHandle(Types.TIMESTAMP_WITH_TIMEZONE, Optional.of(typeName), Optional.of(((TimestampWithTimeZoneType) type).getPrecision()), Optional.empty(), Optional.empty()));
            case StandardTypes.ARRAY -> Optional.of(getJDBCTypeHandle(Types.ARRAY, Optional.of(((ArrayType) type).getElementType().getDisplayName()), Optional.empty(), Optional.empty(), Optional.empty()));
            default -> Optional.empty();
        };
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
        return super.legacyImplementJoin(session, joinType, leftSource, rightSource, joinConditions, rightAssignments, leftAssignments, statistics);
    }

    @Override
    protected boolean isSupportedJoinCondition(ConnectorSession session, JdbcJoinCondition joinCondition)
    {
        return true;
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

    public Collection<String> listCatalogs(Connection connection)
    {
        // for Cluster, we need to list catalogs instead of schemas
        try (ResultSet resultSet = connection.getMetaData().getCatalogs()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_CAT");
                // skip internal schemas
                if (filterRemoteSchema(schemaName)) {
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
    public Collection<String> listSchemas(Connection connection)
    {
        // for trino cluster, list schemas from the current catalog
        try (ResultSet resultSet = connection.getMetaData().getSchemas(connection.getCatalog(), null)) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String tmpSchemaName = resultSet.getString("TABLE_SCHEM");
                // skip internal schemas
                if (filterRemoteSchema(tmpSchemaName)) {
                    schemaNames.add(tmpSchemaName);
                }
            }
            return schemaNames.build();
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    public Collection<String> listSchemas(Connection connection, String catalogName)
    {
        // for trino cluster, we need to list catalogs instead of schemas
        try (ResultSet resultSet = connection.getMetaData().getSchemas()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            String[] catalogNames = catalogName.split("\\.");

            while (resultSet.next()) {
                String tmpSchemaName = resultSet.getString("TABLE_SCHEM");
                String tmpCatalogName = resultSet.getString("TABLE_CATALOG");
                // skip internal schemas
                if (filterRemoteSchema(tmpSchemaName) && tmpCatalogName.equals(catalogNames[1])) {
                    schemaNames.add(tmpSchemaName);
                }
            }
            return schemaNames.build();
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected void dropSchema(ConnectorSession session, Connection connection, String remoteSchemaName, boolean cascade)
            throws SQLException
    {
        try (Statement statement = connection.createStatement()) {
            verify(connection.getAutoCommit());
            String fullCatalogName = session.getCatalog().get();
            String[] catalogNames = fullCatalogName.split("\\.");
            statement.execute(String.format("DROP SCHEMA IF EXISTS %s.%s", catalogNames[1], remoteSchemaName));
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
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
        return statement;
    }

    @Override
    public List<SchemaTableName> getTableNames(ConnectorSession session, Optional<String> schema)
    {
        log.debug("trace cluster getTableNames schema=%s", schema.get());
        try (Connection connection = connectionFactory.openConnection(session)) {
            String fullCatalogName = session.getCatalog().get();
            String[] catalogNames = fullCatalogName.split("\\.");
            PreparedStatement statement = connection.prepareStatement(String.format("show tables from %s.%s", catalogNames[1], schema.get()));

            try (ResultSet resultSet = statement.executeQuery();) {
                ImmutableList.Builder<SchemaTableName> list = ImmutableList.builder();
                while (resultSet.next()) {
                    String tableName = resultSet.getObject("Table").toString();
                    if (filterRemoteSchema(schema.get())) {
                        list.add(new SchemaTableName(schema.get(), tableName));
                    }
                }
                return list.build();
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
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
                clusterVarcharLiteral(comment.orElse(NO_COMMENT))); // An empty character removes the existing comment in MySQL
        execute(session, sql);
    }

    @Override
    protected String getTableRemoteSchemaName(ResultSet resultSet)
            throws SQLException
    {
        // MySQL uses catalogs instead of schemas
        return resultSet.getString("TABLE_CAT");
    }

    @Override
    protected List<String> createTableSqls(RemoteTableName remoteTableName, List<String> columns, ConnectorTableMetadata tableMetadata)
    {
        checkArgument(tableMetadata.getProperties().isEmpty(), "Unsupported table properties: %s", tableMetadata.getProperties());
        return ImmutableList.of(format("CREATE TABLE %s (%s) COMMENT %s", quoted(remoteTableName), join(", ", columns), clusterVarcharLiteral(tableMetadata.getComment().orElse(NO_COMMENT))));
    }

    private static String clusterVarcharLiteral(String value)
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
        if (typeHandle.jdbcTypeName().equals("json")) {
            return Optional.of(jsonColumnMapping());
        }
        switch (typeHandle.jdbcType()) {
            case Types.BIT, Types.BOOLEAN: return Optional.of(booleanColumnMapping());
            case Types.TINYINT: return Optional.of(tinyintColumnMapping());
            case Types.SMALLINT: return Optional.of(smallintColumnMapping());
            case Types.INTEGER: return Optional.of(integerColumnMapping());
            case Types.BIGINT: return Optional.of(bigintColumnMapping());
            case Types.REAL: return Optional.of(realColumnMapping());
            case Types.DOUBLE: return Optional.of(doubleColumnMapping());
            case Types.NUMERIC, Types.DECIMAL: {
                int decimalDigits = typeHandle.requiredDecimalDigits();
                int precision = typeHandle.requiredColumnSize();
                if (precision > Decimals.MAX_PRECISION) {
                    return Optional.empty();
                }
                return Optional.of(decimalColumnMapping(createDecimalType(precision, max(decimalDigits, 0))));
            }
            case Types.CHAR, Types.NCHAR: return Optional.of(defaultCharColumnMapping(typeHandle.requiredColumnSize(), true));
            case Types.VARCHAR, Types.NVARCHAR, Types.LONGVARCHAR, Types.LONGNVARCHAR: return Optional.of(defaultVarcharColumnMapping(typeHandle.requiredColumnSize(), true));
            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY: return Optional.of(varbinaryColumnMapping());

            case Types.DATE: return Optional.of(dateColumnMappingUsingLocalDate());
            case Types.TIME: return Optional.of(timeColumnMapping(createTimeType(typeHandle.requiredDecimalDigits())));
            case Types.TIMESTAMP: return Optional.of(timestampColumnMapping(createTimestampType(typeHandle.requiredDecimalDigits())));
            case TIME_WITH_TIMEZONE:
            case TIMESTAMP_WITH_TIMEZONE:
                if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
                    return mapToUnboundedVarchar(typeHandle);
                }
                return Optional.empty();
            case Types.ARRAY: {
                JdbcTypeHandle baseElementTypeHandle = getArrayElementTypeHandle(connection, typeHandle);
                String baseElementTypeName = baseElementTypeHandle.jdbcTypeName()
                        .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Array baseelement type name is missing: " + baseElementTypeHandle));
                return toColumnMapping(session, connection, baseElementTypeHandle)
                        .map(elementMapping -> {
                            ArrayType trinoArrayType = new ArrayType(elementMapping.getType());
                            return arrayColumnMapping(session, trinoArrayType, elementMapping, baseElementTypeName);
                        });
            }
            case Types.JAVA_OBJECT: return objectToTrinoType(session, jdbcTypeName);
            case Types.STRUCT: return rowToTrinoType(session, jdbcTypeName);
            default: {
                if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
                    return mapToUnboundedVarchar(typeHandle);
                }
                else {
                    log.debug("Trino-to-Trino Unsupported type: %s", typeHandle);
                    return Optional.empty();
                }
            }
        }
    }

    private JdbcTypeHandle getArrayElementTypeHandle(Connection connection, JdbcTypeHandle arrayTypeHandle)
    {
        String arrayTypeName = arrayTypeHandle.jdbcTypeName()
                .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing for jdbc type: " + JDBCType.valueOf(arrayTypeHandle.jdbcType())));
        // Trino Array elements are like this array(row(column1 varchar, column2 varchar)) - we need to extract the base element type - row(column1 varchar, column2 varchar)
        arrayTypeName = arrayTypeName.substring("array(".length(), arrayTypeName.length() - 1); // removing 'array(' and ')' and capture what is inside as the datatype
        arrayTypeName = escapeSpecialCharactersInJDBCTypeName(arrayTypeName); //
        return prepareJdbcTypeHandle(arrayTypeHandle, typeManager.fromSqlType(arrayTypeName).getBaseName(), arrayTypeName);
    }

    private Optional<ColumnMapping> objectToTrinoType(
            ConnectorSession session,
            String jdbcTypeName)
    {
        String quotedJdbcTypeName = escapeSpecialCharactersInJDBCTypeName(jdbcTypeName);
        Object objType = typeManager.fromSqlType(quotedJdbcTypeName);
        if (objType instanceof MapType) {
            return Optional.of(mapColumnMapping(session, (MapType) objType, quotedJdbcTypeName));
        }
        // By default, we are assuming the Objects are ROW types.
        return Optional.of(rowColumnMapping(session, (RowType) objType, quotedJdbcTypeName));
    }

    private String escapeSpecialCharactersInJDBCTypeName(String jdbcTypeName)
    {
        String quotedJdbcTypeName = replaceTokens(
                jdbcTypeName,
                NESTED_DATA_TYPE_METADATA_ESCAPE_PATTERN,
                match -> '"' + match.group("fieldname") + '"');
        return quotedJdbcTypeName;
    }

    private Optional<ColumnMapping> rowToTrinoType(
            ConnectorSession session,
            String jdbcTypeName)
    {
        String quotedJdbcTypeName = escapeSpecialCharactersInJDBCTypeName(jdbcTypeName);
        RowType rowType = (RowType) typeManager.fromSqlType(quotedJdbcTypeName);
        return Optional.of(rowColumnMapping(session, rowType, quotedJdbcTypeName));
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
            Object[] objectArray = TypeUtils.toBoxedArray(resultSet.getArray(columnIndex).getArray());
            return TypeUtils.jdbcObjectArrayToBlock(elementType, objectArray);
        });
    }

    private static ObjectWriteFunction arrayWriteFunction(ConnectorSession session, Type elementType, String elementJdbcTypeName)
    {
        return ObjectWriteFunction.of(Block.class, (statement, index, block) -> {
            Array jdbcArray = statement.getConnection().createArrayOf(elementJdbcTypeName, TypeUtils.getJdbcObjectArray(session, elementType, block));
            statement.setArray(index, jdbcArray);
        });
    }

    private static ColumnMapping mapColumnMapping(ConnectorSession session, MapType mapType, String baseElementJdbcTypeName)
    {
        return ColumnMapping.objectMapping(
                mapType,
                mapReadFunction(session, mapType),
                mapWriteFunction(session, mapType, baseElementJdbcTypeName));
    }

    private static ObjectReadFunction mapReadFunction(ConnectorSession session, MapType mapType)
    {
        return ObjectReadFunction.of(SqlMap.class, (resultSet, columnIndex) -> {
            Map<?, Object> mapData = (Map<?, Object>) resultSet.getObject(columnIndex);
            BlockBuilder keyBlockBuilder = mapType.getKeyType().createBlockBuilder(null, mapData.size());
            BlockBuilder valueBlockBuilder = mapType.getValueType().createBlockBuilder(null, mapData.size());

            for (Map.Entry<?, ?> entry : ((Map<?, ?>) mapData).entrySet()) {
                if (entry.getKey() == null) {
                    throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Map record key is null");
                }
                TypeUtils.appendToBlockBuilder(mapType.getKeyType(), entry.getKey(), keyBlockBuilder);
                if (entry.getValue() == null) {
                    valueBlockBuilder.appendNull();
                }
                else {
                    TypeUtils.appendToBlockBuilder(mapType.getValueType(), entry.getValue(), valueBlockBuilder);
                }
            }
            MapBlock mapBlock = mapType.createBlockFromKeyValue(
                    Optional.empty(),
                    new int[] {0, mapData.size()},
                    keyBlockBuilder.build(),
                    valueBlockBuilder.build());
            return mapType.getObject(mapBlock, 0);
        });
    }

    private static ObjectWriteFunction mapWriteFunction(ConnectorSession session, MapType elementType, String elementJdbcTypeName)
    {
        return ObjectWriteFunction.of(SqlMap.class, (statement, index, sqlMap) -> {
            int rawOffset = sqlMap.getRawOffset();
            Block rawKeyBlock = sqlMap.getRawKeyBlock();
            Block rawValueBlock = sqlMap.getRawValueBlock();

            Type keyType = elementType.getKeyType();
            Type valueType = elementType.getValueType();
            Map<Object, Object> map = new HashMap<>();

            for (int i = 0; i < sqlMap.getSize(); i++) {
                map.put(keyType.getObjectValue(rawKeyBlock, rawOffset + i), valueType.getObjectValue(rawValueBlock, rawOffset + i));
            }
            statement.setObject(index, Collections.unmodifiableMap(map));
        });
    }

    private static ColumnMapping rowColumnMapping(ConnectorSession session, RowType rowType, String baseElementJdbcTypeName)
    {
        return ColumnMapping.objectMapping(
                rowType,
                rowReadFunction(session, rowType),
                rowWriteFunction(session, rowType, baseElementJdbcTypeName));
    }

    private static ObjectReadFunction rowReadFunction(ConnectorSession session, RowType rowType)
    {
        return ObjectReadFunction.of(SqlRow.class, (resultSet, columnIndex) -> {
            Row rowData = (Row) resultSet.getObject(columnIndex);
            Map<?, Object> mapRowData = rowData.getFields().stream()
                    .collect(HashMap::new, (m, v) -> m.put(v.getName().get(), v.getValue()), HashMap::putAll);
            verify(rowData.getFields().size() == rowType.getFields().size(), "Type mismatch: %s, %s", rowData, rowType);

            return buildRowValue(rowType, fieldBuilders -> {
                int fieldPosition = 0;
                for (RowType.Field field : rowType.getFields()) {
                    BlockBuilder fieldBuilder = fieldBuilders.get(fieldPosition);
                    Object fieldData = mapRowData.get(field.getName().get());
                    TypeUtils.appendToBlockBuilder(field.getType(), fieldData, fieldBuilder);
                    fieldPosition++;
                }
            });
        });
    }

    private static ObjectWriteFunction rowWriteFunction(ConnectorSession session, Type elementType, String elementJdbcTypeName)
    {
        return ObjectWriteFunction.of(SqlRow.class, (statement, index, sqlRow) -> {
            List<Object> values = new ArrayList<>(sqlRow.getFieldCount());
            RowType rowType = (RowType) elementType;
            int rawIndex = sqlRow.getRawIndex();
            for (int fieldIndex = 0; fieldIndex < sqlRow.getFieldCount(); fieldIndex++) {
                Type fieldType = rowType.getTypeParameters().get(fieldIndex);
                values.add(fieldType.getObjectValue(sqlRow.getRawFieldBlock(fieldIndex), rawIndex));
            }
            //TODO: The below code may not work - we need to write the method that will write data as a STRUCT
            Object jdbcObject = statement.getConnection().createStruct(elementJdbcTypeName, values.toArray());
            statement.setObject(index, jdbcObject);
        });
    }

    private ColumnMapping jsonColumnMapping()
    {
        return ColumnMapping.sliceMapping(
                jsonType,
                (resultSet, columnIndex) -> jsonParse(utf8Slice(resultSet.getString(columnIndex))),
                typedVarcharWriteFunction(),
                DISABLE_PUSHDOWN);
    }

    private static JdbcTypeHandle prepareJdbcTypeHandle(JdbcTypeHandle original, String typeName, String elementTypeName)
    {
        return switch (typeName) {
            case "boolean" -> getJDBCTypeHandle(Types.BOOLEAN, Optional.ofNullable(typeName), original.arrayDimensions());
            case "bigint" -> getJDBCTypeHandle(Types.BIGINT, Optional.ofNullable(typeName), original.arrayDimensions());
            case "integer" -> getJDBCTypeHandle(Types.INTEGER, Optional.ofNullable(typeName), original.arrayDimensions());
            case "smallint" -> getJDBCTypeHandle(Types.SMALLINT, Optional.ofNullable(typeName), original.arrayDimensions());
            case "tinyint" -> getJDBCTypeHandle(Types.TINYINT, Optional.ofNullable(typeName), original.arrayDimensions());
            case "real" -> getJDBCTypeHandle(Types.REAL, Optional.ofNullable(typeName), original.arrayDimensions());
            case "double" -> getJDBCTypeHandle(Types.DOUBLE, Optional.ofNullable(typeName), original.arrayDimensions());
            case "varchar" -> getJDBCTypeHandle(Types.VARCHAR, Optional.ofNullable(typeName), Optional.of(original.columnSize().orElse(VarcharType.MAX_LENGTH)), Optional.empty(), original.arrayDimensions());
            case "char" -> getJDBCTypeHandle(Types.CHAR, Optional.ofNullable(typeName), Optional.of(original.columnSize().orElse(VarcharType.MAX_LENGTH)), Optional.empty(), original.arrayDimensions());
            case "varbinary" -> getJDBCTypeHandle(Types.VARBINARY, Optional.ofNullable(typeName), original.arrayDimensions());
            case "time" -> getJDBCTypeHandle(Types.TIME, Optional.ofNullable(typeName), Optional.empty(), Optional.of(original.requiredDecimalDigits()), original.arrayDimensions());
            case "time with time zone" -> getJDBCTypeHandle(Types.TIME_WITH_TIMEZONE, Optional.ofNullable(typeName), Optional.empty(), Optional.of(original.requiredDecimalDigits()), original.arrayDimensions());
            case "timestamp" -> getJDBCTypeHandle(Types.TIMESTAMP, Optional.ofNullable(typeName), Optional.empty(), Optional.of(original.requiredDecimalDigits()), original.arrayDimensions());
            case "timestamp with time zone" -> getJDBCTypeHandle(Types.TIMESTAMP_WITH_TIMEZONE, Optional.ofNullable(typeName), Optional.empty(), Optional.of(original.requiredDecimalDigits()), original.arrayDimensions());
            case "unknown" -> getJDBCTypeHandle(Types.NULL, Optional.ofNullable(typeName), original.arrayDimensions());
            case "json" -> getJDBCTypeHandle(Types.JAVA_OBJECT, Optional.ofNullable(typeName), original.arrayDimensions());
            case "date" -> getJDBCTypeHandle(Types.DATE, Optional.ofNullable(typeName), original.arrayDimensions());
            case "decimal" -> getJDBCTypeHandle(Types.DECIMAL, Optional.ofNullable(typeName), Optional.of(original.requiredColumnSize()), Optional.of(original.requiredDecimalDigits()), original.arrayDimensions());
            case "array" -> getJDBCTypeHandle(Types.ARRAY, Optional.of(elementTypeName), original.columnSize(), original.decimalDigits(), original.arrayDimensions());
            case "row" -> getJDBCTypeHandle(Types.STRUCT, Optional.of(elementTypeName), original.columnSize(), original.decimalDigits(), original.arrayDimensions());
            case "map" -> getJDBCTypeHandle(Types.JAVA_OBJECT, Optional.of(elementTypeName), original.columnSize(), original.decimalDigits(), original.arrayDimensions());
            default -> getJDBCTypeHandle(Types.JAVA_OBJECT, Optional.of(elementTypeName), original.columnSize(), original.decimalDigits(), original.arrayDimensions());
        };
    }

    private static JdbcTypeHandle getJDBCTypeHandle(int jdbcType, Optional<String> typeName)
    {
        return getJDBCTypeHandle(jdbcType, typeName, Optional.empty(), Optional.empty(), Optional.empty());
    }

    private static JdbcTypeHandle getJDBCTypeHandle(int jdbcType, Optional<String> typeName, Optional<Integer> arraySize)
    {
        return getJDBCTypeHandle(jdbcType, typeName, Optional.empty(), Optional.empty(), arraySize);
    }

    private static JdbcTypeHandle getJDBCTypeHandle(int jdbcType, Optional<String> typeName, Optional<Integer> columnSize, Optional<Integer> decimalDigits, Optional<Integer> arraySize)
    {
        return new JdbcTypeHandle(jdbcType, typeName, columnSize, decimalDigits, arraySize, Optional.empty());
    }

    private static SliceWriteFunction typedVarcharWriteFunction()
    {
        return new SliceWriteFunction()
        {
            @Override
            public String getBindExpression()
            {
                return "json_parse(?)";
            }

            @Override
            public void set(PreparedStatement statement, int index, Slice value)
                    throws SQLException
            {
                statement.setString(index, value.toStringUtf8());
            }
        };
    }

    /**
     * Replace all the tokens in an input using the algorithm provided for each
     *
     * @param original original string
     * @param tokenPattern the pattern to match with
     * @param converter the conversion to apply
     * @return the substituted string
     */
    private static String replaceTokens(String original, Pattern tokenPattern, Function<Matcher, String> converter)
    {
        int lastIndex = 0;
        StringBuilder output = new StringBuilder();
        Matcher matcher = tokenPattern.matcher(original);
        while (matcher.find()) {
            output.append(original, lastIndex, matcher.start())
                    .append(converter.apply(matcher));

            lastIndex = matcher.end();
        }
        if (lastIndex < original.length()) {
            output.append(original, lastIndex, original.length());
        }
        return output.toString();
    }

    public static LongWriteFunction trinoIntervalWriteMapping()
    {
        return LongWriteFunction.of(JAVA_OBJECT, (statement, index, intervalData) -> {
            statement.setObject(index, new TrinoIntervalDayTime(intervalData));
        });
    }

    public static WriteMapping trinoTimeWriteMapping(TimeType timeType)
    {
        int precision = timeType.getPrecision();
        return WriteMapping.longMapping(String.format("time(%s)", precision), timeWriteFunction(precision));
    }

    public static WriteMapping trinoTimeWithTimeZoneWriteMapping(TimeWithTimeZoneType timeWithTimeZoneType)
    {
        int precision = timeWithTimeZoneType.getPrecision();
        String dataType = String.format("time(%s) with time zone", precision);
        if (timeWithTimeZoneType.isShort()) {
            return WriteMapping.longMapping(dataType, shortTimeWithTimeZoneWriteFunction(precision));
        }
        return WriteMapping.objectMapping(dataType, longTimeWithTimeZoneWriteFunction(precision));
    }

    private static LongWriteFunction shortTimeWithTimeZoneWriteFunction(int timePrecision)
    {
        return (statement, index, value) -> {
            String formatted = SqlTimeWithTimeZone.newInstance(timePrecision, unpackTimeNanos(value) * 1000L, unpackOffsetMinutes(value)).toString();
            statement.setObject(index, formatted, TIME_WITH_TIMEZONE);
        };
    }

    private static ObjectWriteFunction longTimeWithTimeZoneWriteFunction(int timePrecision)
    {
        return ObjectWriteFunction.of(LongTimeWithTimeZone.class, (statement, index, value) -> {
            String formatted = SqlTimeWithTimeZone.newInstance(timePrecision, value.getPicoseconds(), value.getOffsetMinutes()).toString();
            statement.setObject(index, formatted, TIME_WITH_TIMEZONE);
        });
    }

    public static WriteMapping trinoTimestampWriteMapping(TimestampType timestampType)
    {
        int precision = timestampType.getPrecision();
        String dataType = String.format("timestamp(%s)", precision);
        if (timestampType.isShort()) {
            return WriteMapping.longMapping(dataType, timestampWriteFunction(timestampType));
        }
        return WriteMapping.objectMapping(dataType, longTimestampWriteFunction(timestampType, precision));
    }

    public static WriteMapping trinoTimestampWithTimeZoneWriteMapping(TimestampWithTimeZoneType timestampWithTimeZoneType)
    {
        int precision = timestampWithTimeZoneType.getPrecision();
        String dataType = String.format("timestamp(%s) with time zone", precision);
        if (timestampWithTimeZoneType.isShort()) {
            return WriteMapping.longMapping(dataType, shortTimestampWithTimeZoneWriteFunction(precision));
        }
        return WriteMapping.objectMapping(dataType, longTimestampWithTimeZoneWriteFunction(precision));
    }

    private static LongWriteFunction shortTimestampWithTimeZoneWriteFunction(int timeStampPrecision)
    {
        return (statement, index, value) -> {
            String formatted = SqlTimestampWithTimeZone.newInstance(timeStampPrecision, unpackMillisUtc(value), 0, unpackZoneKey(value)).toString();
            statement.setObject(index, formatted, TIMESTAMP_WITH_TIMEZONE);
        };
    }

    private static ObjectWriteFunction longTimestampWithTimeZoneWriteFunction(int timeStampPrecision)
    {
        return ObjectWriteFunction.of(LongTimestampWithTimeZone.class, (statement, index, value) -> {
            String formatted = SqlTimestampWithTimeZone.newInstance(timeStampPrecision, value.getEpochMillis(), value.getPicosOfMilli(), getTimeZoneKey(value.getTimeZoneKey())).toString();
            statement.setObject(index, formatted, TIMESTAMP_WITH_TIMEZONE);
        });
    }

    public static LongReadFunction clusterDateReadFunctionUsingLocalDate()
    {
        return new LongReadFunction() {
            @Override
            public boolean isNull(ResultSet resultSet, int columnIndex)
                    throws SQLException
            {
                // 'ResultSet.getObject' without class name may throw an exception
                Object value = resultSet.getObject(columnIndex);
                if (null == value) {
                    return true;
                }

                return value.toString().isEmpty();
            }

            @Override
            public long readLong(ResultSet resultSet, int columnIndex)
                    throws SQLException
            {
                // LocalDate value = resultSet.getObject(columnIndex, LocalDate.class);
                LocalDate value = LocalDate.parse(resultSet.getObject(columnIndex).toString(), ISO_LOCAL_DATE);
                // Some drivers (e.g. MemSQL's) return null LocalDate even though the value isn't null
                if (value == null) {
                    throw new TrinoException(JDBC_ERROR, "Driver returned null LocalDate for a non-null value");
                }

                return value.toEpochDay();
            }
        };
    }

    private LongWriteFunction clusterDateWriteFunctionUsingLocalDate()
    {
        return new LongWriteFunction() {
            @Override
            public String getBindExpression()
            {
                return "CAST(? AS DATE)";
            }

            @Override
            public void set(PreparedStatement statement, int index, long epochDay)
                    throws SQLException
            {
                statement.setString(index, LocalDate.ofEpochDay(epochDay).format(ISO_LOCAL_DATE));
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
        if (type.equals(this.jsonType)) {
            return WriteMapping.sliceMapping(StandardTypes.JSON, typedVarcharWriteFunction());
        }
        else if (type.equals(this.intervalDayTimeType)) {
            return WriteMapping.longMapping(INTERVAL_DAY_TO_SECOND, trinoIntervalWriteMapping());
        }
        String typeName = type.getBaseName();
        return switch (typeName) {
            case StandardTypes.BOOLEAN -> WriteMapping.booleanMapping("boolean", booleanWriteFunction());
            case StandardTypes.TINYINT -> WriteMapping.longMapping("tinyint", tinyintWriteFunction());
            case StandardTypes.SMALLINT -> WriteMapping.longMapping("smallint", smallintWriteFunction());
            case StandardTypes.INTEGER -> WriteMapping.longMapping("integer", integerWriteFunction());
            case StandardTypes.BIGINT -> WriteMapping.longMapping("bigint", bigintWriteFunction());
            case StandardTypes.REAL -> WriteMapping.longMapping("float", realWriteFunction());
            case StandardTypes.DOUBLE -> WriteMapping.doubleMapping("double", doubleWriteFunction());
            case StandardTypes.DECIMAL -> {
                DecimalType decimalType = (DecimalType) type;
                String dataType = format("decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
                if (decimalType.isShort()) {
                    yield WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
                }
                yield WriteMapping.objectMapping(dataType, longDecimalWriteFunction(decimalType));
            }
            case StandardTypes.CHAR -> WriteMapping.sliceMapping(format("char(%s)", ((CharType) type).getLength()), charWriteFunction());
            case StandardTypes.VARCHAR -> {
                String dataType;
                if (((VarcharType) type).isUnbounded()) {
                    dataType = "varchar";
                }
                else {
                    dataType = format("varchar(%s)", ((VarcharType) type).getBoundedLength());
                }
                yield WriteMapping.sliceMapping(dataType, varcharWriteFunction());
            }
            case StandardTypes.VARBINARY -> WriteMapping.sliceMapping("varbinary", varbinaryWriteFunction());
            case StandardTypes.DATE -> WriteMapping.longMapping("date", dateWriteFunctionUsingLocalDate());
            case StandardTypes.TIME -> trinoTimeWriteMapping((TimeType) type);
            case StandardTypes.TIME_WITH_TIME_ZONE -> trinoTimeWithTimeZoneWriteMapping((TimeWithTimeZoneType) type);
            case StandardTypes.TIMESTAMP -> trinoTimestampWriteMapping((TimestampType) type);
            case StandardTypes.TIMESTAMP_WITH_TIME_ZONE -> trinoTimestampWithTimeZoneWriteMapping((TimestampWithTimeZoneType) type);
            case StandardTypes.ARRAY -> {
                Type elementType = ((ArrayType) type).getElementType();
                String elementDataType = toWriteMapping(session, elementType).getDataType();
                yield WriteMapping.objectMapping(elementDataType + "[]", arrayWriteFunction(session, elementType, elementType.getDisplayName()));
            }
            default -> throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
        };
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
        try (Connection connection = connectionFactory.openConnection(session);
                Statement statement = connection.createStatement()) {
            verify(connection.getAutoCommit());
            String fullCatalogName = session.getCatalog().get();
            String[] catalogNames = fullCatalogName.split("\\.");
            statement.execute(String.format("ALTER SCHEMA name %s.%s TO %s.%s",
                    catalogNames[1],
                    schemaName,
                    catalogNames[1],
                    newSchemaName));
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected void copyTableSchema(ConnectorSession session, Connection connection, String catalogName, String schemaName, String tableName, String newTableName, List<String> columnNames)
    {
        String fullCatalogName = session.getCatalog().get();
        String[] catalogNames = fullCatalogName.split("\\.");
        String tableCopyFormat = "CREATE TABLE %s AS SELECT * FROM %s WHERE 0 = 1";
        if (isGtidMode(connection)) {
            tableCopyFormat = "CREATE TABLE %s LIKE %s";
        }
        String sql = format(
                tableCopyFormat,
                quoted(catalogNames[1], schemaName, newTableName),
                quoted(catalogNames[1], schemaName, tableName));
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
    public boolean supportsTopN(ConnectorSession session, JdbcTableHandle handle, List<JdbcSortItem> sortOrder)
    {
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
                        rowCount = max(rowCount, columnIndexStatistics.cardinality());
                    }
                }

                tableStatistics.setColumnStatistics(column, columnStatisticsBuilder.build());
            }

            tableStatistics.setRowCount(Estimate.of(rowCount));
            return tableStatistics.build();
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
            return handle.createQuery("""
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
            return handle.createQuery("""
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
                if (e.getCause() instanceof SQLSyntaxErrorException) {
                    log.debug("INFORMATION_SCHEMA.COLUMN_STATISTICS table is not available: %s", e);
                    return ImmutableMap.of();
                }
            }

            RemoteTableName remoteTableName = table.getRequiredNamedRelation().getRemoteTableName();
            return handle.createQuery("""
                        SELECT COLUMN_NAME, HISTOGRAM FROM INFORMATION_SCHEMA.COLUMN_STATISTICS
                        WHERE SCHEMA_NAME = :schema AND TABLE_NAME = :table_name
                        """)
                    .bind("schema", remoteTableName.getCatalogName().orElse(null))
                    .bind("table_name", remoteTableName.getTableName())
                    .map((rs, ctx) -> new SimpleEntry<>(rs.getString("COLUMN_NAME"), rs.getString("HISTOGRAM")))
                    .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        }
    }

    private record ColumnIndexStatistics(boolean nullable, long cardinality) {}

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
