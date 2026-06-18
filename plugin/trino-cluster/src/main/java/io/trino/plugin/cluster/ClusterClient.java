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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.trino.plugin.base.aggregation.AggregateFunctionRewriter;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.CaseSensitivity;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcJoinCondition;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcSortItem;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongReadFunction;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.PredicatePushdownController;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
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
import io.trino.plugin.jdbc.expression.RewriteIn;
import io.trino.plugin.jdbc.expression.RewriteLikeEscapeWithCaseSensitivity;
import io.trino.plugin.jdbc.expression.RewriteLikeWithCaseSensitivity;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.base.mapping.RemoteIdentifiers;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.JoinCondition;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.VarcharType;

import com.google.inject.Inject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.base.util.JsonTypeUtil.jsonParse;
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
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.toTrinoTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class ClusterClient
        extends BaseJdbcClient
{
    private static final Logger log = Logger.get(ClusterClient.class);

    private static final int MAX_SUPPORTED_DATE_TIME_PRECISION = 6;
    // Cluster driver returns width of timestamp types instead of precision.
    // 19 characters are used for zero-precision timestamps while others
    // require 19 + precision + 1 characters with the additional character
    // required for the decimal separator.
    private static final int ZERO_PRECISION_TIMESTAMP_COLUMN_SIZE = 19;
    // Cluster driver returns width of time types instead of precision, same as the above timestamp type.
    private static final int ZERO_PRECISION_TIME_COLUMN_SIZE = 8;

    // An empty character means that the table doesn't have a comment in Cluster
    private static final String NO_COMMENT = "";
    private final TypeManager typeManager;
    private final Type jsonType;
    private final boolean statisticsEnabled;
    private final ConnectorExpressionRewriter<ParameterizedExpression> connectorExpressionRewriter;
    private final AggregateFunctionRewriter<JdbcExpression, ?> aggregateFunctionRewriter;

    private static final PredicatePushdownController DCSQL_CHARACTER_PUSHDOWN = (session, domain) -> {
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
        this.jsonType = typeManager.getType(new TypeSignature(StandardTypes.JSON));
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
                .add(new RewriteLikeWithCaseSensitivity())
                .add(new RewriteLikeEscapeWithCaseSensitivity())
                .build();

        JdbcTypeHandle bigintTypeHandle = new JdbcTypeHandle(Types.BIGINT, Optional.of("bigint"), Optional.of(0), Optional.of(0), Optional.empty(), Optional.empty());
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

    protected static boolean preventTextualTypeAggregationPushdown(List<List<ColumnHandle>> groupingSets)
    {
        // Remote database can be case insensitive or sorts textual types differently than Trino.
        // In such cases we should not pushdown aggregations if the grouping set contains a textual type.
        return true;
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
        // for trino cluster, we need to list catalogs instead of schemas
        try (ResultSet resultSet = connection.getMetaData().getSchemas()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            String catalog = connection.getCatalog();

            while (resultSet.next()) {
                String tmpSchemaName = resultSet.getString("TABLE_SCHEM");
                String tmpCatalogName = resultSet.getString("TABLE_CATALOG");
                // skip internal schemas
                if (filterRemoteSchema(tmpSchemaName) && tmpCatalogName.equals(catalog)) {
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
    public List<SchemaTableName> getTableNames(ConnectorSession session, Optional<String> schema)
    {
        log.debug("trace cluster getTableNames schema=" + schema.get());
        try (Connection connection = connectionFactory.openConnection(session)) {
            String catalog = connection.getCatalog();
            PreparedStatement statement = connection.prepareStatement(String.format("show tables from %s.%s", catalog, schema.get()));

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
    public void abortReadConnection(Connection connection, ResultSet resultSet)
            throws SQLException
    {
        if (!resultSet.isAfterLast()) {
            // Abort connection before closing. Without this, the Cluster driver
            // attempts to drain the connection by reading all the results.
            connection.close();
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
    public Optional<JdbcTableHandle> getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        try (Connection connection = connectionFactory.openConnection(session)) {
            String catalog = connection.getCatalog();
            PreparedStatement statement = connection.prepareStatement(String.format("show tables from %s.%s", catalog, schemaTableName.getSchemaName()));
            try (ResultSet resultSet = statement.executeQuery()) {
                List<JdbcTableHandle> tableHandles = new ArrayList<>();
                while (resultSet.next()) {
                    String resultTableName = resultSet.getObject("Table").toString();
                    if (resultTableName.equals(schemaTableName.getTableName())) {
                        RemoteTableName remoteTableName = new RemoteTableName(Optional.ofNullable(catalog),
                                Optional.of(schemaTableName.getSchemaName()),
                                schemaTableName.getTableName());
                        tableHandles.add(new JdbcTableHandle(schemaTableName, remoteTableName, Optional.ofNullable(null)));
                        break;
                    }
                }
                if (tableHandles.isEmpty()) {
                    return Optional.empty();
                }
                if (tableHandles.size() > 1) {
                    throw new TrinoException(NOT_SUPPORTED, "Multiple tables matched: " + schemaTableName);
                }
                return Optional.of(getOnlyElement(tableHandles));
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected List<String> createTableSqls(RemoteTableName remoteTableName, List<String> columns, ConnectorTableMetadata tableMetadata)
    {
        checkArgument(tableMetadata.getProperties().isEmpty(), "Unsupported table properties: %s", tableMetadata.getProperties());
        return ImmutableList.of(format("CREATE TABLE %s (%s) COMMENT %s", quoted(remoteTableName), join(", ", columns), clusterVarcharLiteral(tableMetadata.getComment().orElse(NO_COMMENT))));
    }

    @Override
    protected JdbcOutputTableHandle beginInsertTable(
            ConnectorSession session,
            Connection connection,
            RemoteIdentifiers remoteIdentifiers,
            String catalog,
            String remoteSchema,
            String remoteTable,
            List<JdbcColumnHandle> columns)
            throws SQLException
    {
        log.debug("trace cluster beginInsertTable");

        ImmutableList.Builder<String> columnNames = ImmutableList.builder();
        ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
        ImmutableList.Builder<JdbcTypeHandle> jdbcColumnTypes = ImmutableList.builder();
        for (JdbcColumnHandle column : columns) {
            columnNames.add(column.getColumnName());
            columnTypes.add(column.getColumnType());
            jdbcColumnTypes.add(column.getJdbcTypeHandle());
        }

        return new JdbcOutputTableHandle(
                new RemoteTableName(Optional.ofNullable(catalog), Optional.ofNullable(remoteSchema), remoteTable),
                columnNames.build(),
                columnTypes.build(),
                Optional.of(jdbcColumnTypes.build()),
                Optional.empty(),
                Optional.empty());
    }

    @Override
    public void finishInsertTable(ConnectorSession session, JdbcOutputTableHandle handle, Set<Long> pageSinkIds)
    {
        log.debug("trace cluster finishInsertTable");
        checkState(handle.getTemporaryTableName().isEmpty(), "Unexpected use of temporary table when non transactional inserts are enabled");
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
                int decimalDigits = typeHandle.requiredDecimalDigits();
                int precision = typeHandle.requiredColumnSize();
                if (getDecimalRounding(session) == ALLOW_OVERFLOW && precision > Decimals.MAX_PRECISION) {
                    int scale = min(decimalDigits, getDecimalDefaultScale(session));
                    return Optional.of(decimalColumnMapping(createDecimalType(Decimals.MAX_PRECISION, scale), getDecimalRoundingMode(session)));
                }
                // TODO does Cluster support negative scale?
                precision = precision + max(-decimalDigits, 0); // Map decimal(p, -s) (negative scale) to decimal(p+s, 0).
                if (precision > Decimals.MAX_PRECISION) {
                    break;
                }
                return Optional.of(decimalColumnMapping(createDecimalType(precision, max(decimalDigits, 0))));

            case Types.CHAR:
                return Optional.of(dcSqlDefaultCharColumnMapping(typeHandle.requiredColumnSize(), typeHandle.caseSensitivity()));

            // TODO not all these type constants are necessarily used by the JDBC driver
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                return Optional.of(dcSqlDefaultVarcharColumnMapping(typeHandle.requiredColumnSize(), typeHandle.caseSensitivity()));

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return Optional.of(ColumnMapping.sliceMapping(VARBINARY, varbinaryReadFunction(), varbinaryWriteFunction(), FULL_PUSHDOWN));

            case Types.DATE:
                return Optional.of(ColumnMapping.longMapping(
                        DATE,
                        clusterDateReadFunctionUsingLocalDate(),
                        clusterDateWriteFunctionUsingLocalDate()));

            case Types.TIME:
                log.debug("trace toColumnMapping Types.TIME clusterTimeReadFunction() timeWriteFunction()");
                TimeType timeType = createTimeType(getTimePrecision(typeHandle.requiredColumnSize()));
                requireNonNull(timeType, "timeType is null");
                checkArgument(timeType.getPrecision() <= 9, "Unsupported type precision: %s", timeType);
                return Optional.of(ColumnMapping.longMapping(
                        timeType,
                        clusterTimeReadFunction(timeType),
                        timeWriteFunction(timeType.getPrecision())));

            case Types.TIMESTAMP:
                return Optional.of(ColumnMapping.longMapping(
                        TimestampType.TIMESTAMP_MILLIS,
                        clusterTimestampReadFunction(TimestampType.TIMESTAMP_MILLIS),
                        timestampWriteFunction(TimestampType.TIMESTAMP_MILLIS)));
        }

        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return mapToUnboundedVarchar(typeHandle);
        }
        return Optional.empty();
    }

    private static ColumnMapping dcSqlDefaultVarcharColumnMapping(int columnSize, Optional<CaseSensitivity> caseSensitivity)
    {
        if (columnSize > VarcharType.MAX_LENGTH) {
            return dcSqlVarcharColumnMapping(createUnboundedVarcharType(), caseSensitivity);
        }
        return dcSqlVarcharColumnMapping(createVarcharType(columnSize), caseSensitivity);
    }

    private static ColumnMapping dcSqlVarcharColumnMapping(VarcharType varcharType, Optional<CaseSensitivity> caseSensitivity)
    {
        PredicatePushdownController pushdownController = DCSQL_CHARACTER_PUSHDOWN;
        return ColumnMapping.sliceMapping(varcharType, varcharReadFunction(varcharType), varcharWriteFunction(), pushdownController);
    }

    private static ColumnMapping dcSqlDefaultCharColumnMapping(int columnSize, Optional<CaseSensitivity> caseSensitivity)
    {
        if (columnSize > CharType.MAX_LENGTH) {
            return dcSqlDefaultVarcharColumnMapping(columnSize, caseSensitivity);
        }
        return dcSqlCharColumnMapping(createCharType(columnSize), caseSensitivity);
    }

    private static ColumnMapping dcSqlCharColumnMapping(CharType charType, Optional<CaseSensitivity> caseSensitivity)
    {
        requireNonNull(charType, "charType is null");
        PredicatePushdownController pushdownController = DCSQL_CHARACTER_PUSHDOWN;
        return ColumnMapping.sliceMapping(charType, charReadFunction(charType), charWriteFunction(), pushdownController);
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

    private static LongReadFunction clusterTimestampReadFunction(TimestampType timestampType)
    {
        return new LongReadFunction()
        {
            @Override
            public boolean isNull(ResultSet resultSet, int columnIndex)
                    throws SQLException
            {
                // super calls ResultSet#getObject(), which for TIMESTAMP type returns java.sql.Timestamp, for which the conversion can fail if the value isn't a valid instant in server's time zone.
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
                LocalDateTime localDateTime = LocalDateTime.parse(resultSet.getObject(columnIndex).toString(),
                        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
                return toTrinoTimestamp(timestampType, localDateTime);
            }
        };
    }

    private static LongReadFunction clusterTimeReadFunction(TimeType timeType)
    {
        return new LongReadFunction()
        {
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
                return timeReadFunction(timeType).readLong(resultSet, columnIndex);
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
            log.debug("trace toWriteMapping clusterDateWriteFunctionUsingLocalDate");
            return WriteMapping.longMapping("date", clusterDateWriteFunctionUsingLocalDate());
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

        if (VARBINARY.equals(type)) {
            return WriteMapping.sliceMapping("mediumblob", varbinaryWriteFunction());
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

        if (type.equals(jsonType)) {
            return WriteMapping.sliceMapping("json", varcharWriteFunction());
        }

        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
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
    public void createSchema(ConnectorSession session, String schemaName)
    {
        try (Connection connection = connectionFactory.openConnection(session);
                Statement statement = connection.createStatement()) {
            String catalog = connection.getCatalog();
            verify(connection.getAutoCommit());
            statement.execute(String.format("CREATE SCHEMA IF NOT EXISTS %s.%s", catalog, schemaName));
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
            String catalog = connection.getCatalog();
            String dropSchema = String.format("DROP SCHEMA IF EXISTS %s.%s", catalog, remoteSchemaName);
            if (cascade) {
                dropSchema += " CASCADE";
            }
            statement.execute(dropSchema);
        }
    }

    @Override
    public void renameSchema(ConnectorSession session, String schemaName, String newSchemaName)
    {
        // TODO - mismatched input
        try (Connection connection = connectionFactory.openConnection(session);
                Statement statement = connection.createStatement()) {
            verify(connection.getAutoCommit());
            String catalog = connection.getCatalog();
            statement.execute(String.format("ALTER SCHEMA name %s.%s TO %s.%s",
                    catalog,
                    schemaName,
                    catalog,
                    newSchemaName));
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected void copyTableSchema(ConnectorSession session, Connection connection, String catalogName, String schemaName, String tableName, String newTableName, List<String> columnNames)
    {
        String tableCopyFormat = "CREATE TABLE %s AS SELECT * FROM %s WHERE 0 = 1";
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
        // Cluster doesn't support specifying the catalog name in a rename. By setting the
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
                            // In Cluster ASC implies NULLS FIRST, DESC implies NULLS LAST
                            case ASC_NULLS_FIRST, DESC_NULLS_LAST -> Stream.of(columnSorting);
                            case ASC_NULLS_LAST -> Stream.of(
                                    format("ISNULL(%s) ASC", quoted(sortItem.column().getColumnName())),
                                    columnSorting);
                            case DESC_NULLS_FIRST -> Stream.of(
                                    format("ISNULL(%s) DESC", quoted(sortItem.column().getColumnName())),
                                    columnSorting);
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
            // Not supported in Cluster
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
            // Not supported in Cluster
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
            // Not supported in Cluster
            return false;
        }

        // Remote database can be case insensitive.
        return Stream.of(joinCondition.getLeftColumn(), joinCondition.getRightColumn())
                .map(JdbcColumnHandle::getColumnType)
                .noneMatch(type -> type instanceof CharType || type instanceof VarcharType);
    }

    private ColumnMapping jsonColumnMapping()
    {
        return ColumnMapping.sliceMapping(
                jsonType,
                (resultSet, columnIndex) -> jsonParse(utf8Slice(resultSet.getString(columnIndex))),
                varcharWriteFunction(),
                DISABLE_PUSHDOWN);
    }
}
