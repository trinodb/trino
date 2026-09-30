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
package io.trino.plugin.ydb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import io.trino.plugin.base.aggregation.AggregateFunctionRewriter;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.base.projection.ProjectFunctionRewriter;
import io.trino.plugin.base.projection.ProjectFunctionRule;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.BooleanWriteFunction;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcJoinCondition;
import io.trino.plugin.jdbc.JdbcMergeTableHandle;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcSortItem;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.aggregation.ImplementAvgFloatingPoint;
import io.trino.plugin.jdbc.aggregation.ImplementCount;
import io.trino.plugin.jdbc.aggregation.ImplementCountAll;
import io.trino.plugin.jdbc.aggregation.ImplementCountDistinct;
import io.trino.plugin.jdbc.aggregation.ImplementMinMax;
import io.trino.plugin.jdbc.aggregation.ImplementSum;
import io.trino.plugin.jdbc.expression.JdbcConnectorExpressionRewriterBuilder;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.expression.RewriteIn;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import jakarta.annotation.Nullable;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.trino.plugin.jdbc.DefaultJdbcMetadata.MERGE_ROW_ID;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.PredicatePushdownController.FULL_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.realColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.plugin.ydb.YdbColumnMappings.YDB_DATE32_SQL_TYPE;
import static io.trino.plugin.ydb.YdbColumnMappings.YDB_TIMESTAMP64_SQL_TYPE;
import static io.trino.plugin.ydb.YdbColumnMappings.dateColumnMapping;
import static io.trino.plugin.ydb.YdbColumnMappings.dateWriteFunction;
import static io.trino.plugin.ydb.YdbColumnMappings.decimalColumnMapping;
import static io.trino.plugin.ydb.YdbColumnMappings.timestampColumnMapping;
import static io.trino.plugin.ydb.YdbColumnMappings.timestampWriteFunction;
import static io.trino.plugin.ydb.YdbColumnMappings.unsignedBigintColumnMapping;
import static io.trino.plugin.ydb.YdbColumnMappings.unsignedColumnMapping;
import static io.trino.plugin.ydb.YdbTableProperties.PRIMARY_KEY_PROPERTY;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.JoinCondition.Operator.EQUAL;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.Math.max;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

public class YdbClient
        extends BaseJdbcClient
{
    static final String DEFAULT_SCHEMA = "default";
    private static final int YDB_DEFAULT_DECIMAL_PRECISION = 22;
    private static final int YDB_DEFAULT_DECIMAL_SCALE = 9;

    private final ConnectorExpressionRewriter<ParameterizedExpression> connectorExpressionRewriter;
    private final AggregateFunctionRewriter<JdbcExpression, ParameterizedExpression> aggregateFunctionRewriter;
    private final ProjectFunctionRewriter<JdbcExpression, ParameterizedExpression> projectFunctionRewriter;

    @Inject
    public YdbClient(
            BaseJdbcConfig config,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier remoteQueryModifier)
    {
        super("`",
                connectionFactory,
                queryBuilder,
                config.getJdbcTypesMappedToVarchar(),
                identifierMapping,
                remoteQueryModifier,
                true);

        this.connectorExpressionRewriter = JdbcConnectorExpressionRewriterBuilder.newBuilder()
                .addStandardRules(this::quoted)
                .add(new RewriteIn())
                .add(new RewriteDivideModulus())
                .add(new RewriteNullIf())
                .withTypeClass("integer_type", ImmutableSet.of("tinyint", "smallint", "integer", "bigint"))
                .withTypeClass("numeric_type", ImmutableSet.of("tinyint", "smallint", "integer", "bigint", "decimal", "real", "double"))
                .withTypeClass("comparable_type", ImmutableSet.of(
                        "tinyint", "smallint", "integer", "bigint", "decimal", "real", "double", "varchar", "char", "date", "timestamp"))
                .map("$equal(left, right)").to("left = right")
                .map("$not_equal(left, right)").to("left <> right")
                .map("$less_than(left: comparable_type, right: comparable_type)").to("left < right")
                .map("$less_than_or_equal(left: comparable_type, right: comparable_type)").to("left <= right")
                .map("$greater_than(left: comparable_type, right: comparable_type)").to("left > right")
                .map("$greater_than_or_equal(left: comparable_type, right: comparable_type)").to("left >= right")
                .map("$is_null(value)").to("value IS NULL")
                .map("$not($is_null(value))").to("value IS NOT NULL")
                .map("$concat(left: varchar, right: varchar)").to("left || right")
                .build();

        this.projectFunctionRewriter = new ProjectFunctionRewriter<>(
                this.connectorExpressionRewriter,
                ImmutableSet.<ProjectFunctionRule<JdbcExpression, ParameterizedExpression>>builder()
                        .add(new RewriteUnaryStringOperations())
                        .add(new RewriteStringPosition())
                        .build());

        JdbcTypeHandle bigintTypeHandle = YdbTypeUtils.toTypeHandle(BIGINT).orElseThrow();
        this.aggregateFunctionRewriter = new AggregateFunctionRewriter<>(
                this.connectorExpressionRewriter,
                ImmutableSet.<AggregateFunctionRule<JdbcExpression, ParameterizedExpression>>builder()
                        .add(new ImplementCountAll(bigintTypeHandle))
                        .add(new ImplementMinMax(true))
                        .add(new ImplementCount(bigintTypeHandle))
                        .add(new ImplementCountDistinct(bigintTypeHandle, true))
                        .add(new ImplementSum(YdbTypeUtils::toTypeHandle))
                        .add(new ImplementAvgFloatingPoint())
                        .build());
    }

    @Override
    protected boolean isSupportedJoinCondition(ConnectorSession session, JdbcJoinCondition condition)
    {
        return condition.getOperator() == EQUAL
                && condition.getLeftColumn().getColumnType().equals(condition.getRightColumn().getColumnType())
                && hasSupportedValueMapping(condition.getLeftColumn())
                && hasSupportedValueMapping(condition.getRightColumn());
    }

    private boolean hasSupportedValueMapping(JdbcColumnHandle column)
    {
        JdbcTypeHandle type = column.getJdbcTypeHandle();
        if (getForcedMappingToVarchar(type).isPresent()) {
            return false;
        }
        String name = type.jdbcTypeName().orElse("").toLowerCase(Locale.ROOT);
        return switch (name) {
            case "bool" -> column.getColumnType().equals(BOOLEAN);
            case "int8" -> column.getColumnType().equals(TINYINT);
            case "int16", "uint8" -> column.getColumnType().equals(SMALLINT);
            case "int32", "uint16" -> column.getColumnType().equals(INTEGER);
            case "int64", "uint32" -> column.getColumnType().equals(BIGINT);
            case "uint64" -> column.getColumnType().equals(createDecimalType(20, 0));
            case "float" -> column.getColumnType().equals(REAL);
            case "double" -> column.getColumnType().equals(DOUBLE);
            case "utf8", "text" -> column.getColumnType() instanceof VarcharType;
            case "date", "date32" -> column.getColumnType().equals(DATE);
            case "datetime", "datetime64", "timestamp", "timestamp64" -> column.getColumnType().equals(TIMESTAMP_MICROS);
            case "string", "bytes" -> column.getColumnType().equals(VARBINARY);
            // YDB Decimal also admits NaN and infinities, which Trino DECIMAL cannot represent.
            default -> false;
        };
    }

    private boolean supportsExpressionMapping(JdbcColumnHandle column)
    {
        if (!hasSupportedValueMapping(column)) {
            return false;
        }
        return switch (column.getJdbcTypeHandle().jdbcTypeName().orElse("").toLowerCase(Locale.ROOT)) {
            case "uint8", "uint16", "uint32", "uint64", "float", "double",
                 "date", "date32", "datetime", "datetime64", "timestamp" -> false;
            default -> true;
        };
    }

    private boolean supportsExpression(ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        if (expression instanceof Variable variable) {
            return supportsExpressionMapping((JdbcColumnHandle) assignments.get(variable.getName()));
        }
        return expression.getChildren().stream().allMatch(child -> supportsExpression(child, assignments));
    }

    @Override
    public boolean supportsAggregationPushdown(
            ConnectorSession session,
            JdbcTableHandle table,
            List<AggregateFunction> aggregates,
            Map<String, ColumnHandle> assignments,
            List<List<ColumnHandle>> groupingSets)
    {
        return groupingSets.stream().flatMap(List::stream).map(JdbcColumnHandle.class::cast)
                .allMatch(column -> hasSupportedValueMapping(column)
                        && !column.getColumnType().equals(REAL) && !column.getColumnType().equals(DOUBLE));
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
        if (joinConditions.isEmpty()) {
            return Optional.empty();
        }
        Map<String, JdbcColumnHandle> leftAliases = leftProjections.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
        Map<String, JdbcColumnHandle> rightAliases = rightProjections.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
        ImmutableList.Builder<JdbcJoinCondition> conditions = ImmutableList.builder();
        for (ParameterizedExpression expression : joinConditions) {
            Optional<YdbJoinCondition> parsed = YdbJoinCondition.parse(expression.expression());
            if (parsed.isEmpty() || !expression.parameters().isEmpty()) {
                return Optional.empty();
            }
            JdbcColumnHandle leftColumn = leftAliases.get(parsed.get().left());
            JdbcColumnHandle rightColumn = rightAliases.get(parsed.get().right());
            if (leftColumn == null || rightColumn == null) {
                leftColumn = leftAliases.get(parsed.get().right());
                rightColumn = rightAliases.get(parsed.get().left());
            }
            if (leftColumn == null || rightColumn == null) {
                return Optional.empty();
            }
            conditions.add(new JdbcJoinCondition(leftColumn, EQUAL, rightColumn));
        }
        return super.legacyImplementJoin(
                session,
                joinType,
                leftSource,
                rightSource,
                conditions.build(),
                rightProjections,
                leftProjections,
                statistics);
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(
            ConnectorSession session,
            AggregateFunction aggregate,
            Map<String, ColumnHandle> assignments)
    {
        if (aggregate.getArguments().stream().anyMatch(argument ->
                argument instanceof Variable variable
                        ? !hasSupportedValueMapping((JdbcColumnHandle) assignments.get(variable.getName()))
                        : !supportsExpression(argument, assignments))) {
            return Optional.empty();
        }
        boolean floatingArgument = aggregate.getArguments().stream().anyMatch(argument -> argument.getType().equals(REAL) || argument.getType().equals(DOUBLE));
        if ((aggregate.getFunctionName().equals("sum") || aggregate.getFunctionName().equals("avg")) && !floatingArgument) {
            // YQL integral/decimal aggregates do not implement Trino's overflow contract.
            return Optional.empty();
        }
        if (floatingArgument && (aggregate.getFunctionName().equals("min") || aggregate.getFunctionName().equals("max") || aggregate.isDistinct())) {
            // Native floating-key ordering and grouping do not normalize NaN and signed zero.
            return Optional.empty();
        }
        return aggregateFunctionRewriter.rewrite(session, aggregate, assignments);
    }

    @Override
    public Optional<ParameterizedExpression> convertPredicate(
            ConnectorSession session,
            ConnectorExpression expression,
            Map<String, ColumnHandle> assignments)
    {
        if (expression instanceof Call call && call.getFunctionName().equals(EQUAL_OPERATOR_FUNCTION_NAME)
                && call.getArguments().getFirst() instanceof Variable left
                && call.getArguments().get(1) instanceof Variable right) {
            JdbcColumnHandle leftColumn = (JdbcColumnHandle) assignments.get(left.getName());
            JdbcColumnHandle rightColumn = (JdbcColumnHandle) assignments.get(right.getName());
            if (hasSupportedValueMapping(leftColumn) && hasSupportedValueMapping(rightColumn)
                    && leftColumn.getColumnType().equals(rightColumn.getColumnType())) {
                return Optional.of(new ParameterizedExpression(
                        quoted(leftColumn.getColumnName()) + " = " + quoted(rightColumn.getColumnName()), ImmutableList.of()));
            }
        }
        if (!supportsExpression(expression, assignments)) {
            return Optional.empty();
        }
        return connectorExpressionRewriter.rewrite(session, expression, assignments);
    }

    @Override
    public Optional<JdbcExpression> convertProjection(
            ConnectorSession session,
            JdbcTableHandle handle,
            ConnectorExpression expression,
            Map<String, ColumnHandle> assignments)
    {
        if (!handle.getUpdateAssignments().isEmpty() || assignments.values().stream()
                .filter(JdbcColumnHandle.class::isInstance)
                .map(JdbcColumnHandle.class::cast)
                .anyMatch(column -> column.getColumnName().equals(MERGE_ROW_ID))) {
            return Optional.empty();
        }
        if (!supportsExpression(expression, assignments)) {
            return Optional.empty();
        }

        JdbcTypeHandle typeHandle = YdbTypeUtils.toTypeHandle(expression.getType()).orElse(null);
        if (Objects.isNull(typeHandle)) {
            return Optional.empty();
        }
        Optional<ParameterizedExpression> result = connectorExpressionRewriter.rewrite(session, expression, assignments);
        return result.map(parameterizedExpression -> new JdbcExpression(
                parameterizedExpression.expression(),
                parameterizedExpression.parameters(),
                typeHandle)).or(() -> projectFunctionRewriter.rewrite(session, handle, expression, assignments));
    }

    @Override
    public Collection<String> listSchemas(Connection connection)
    {
        return ImmutableSet.of(DEFAULT_SCHEMA);
    }

    @Override
    protected String escapeObjectNameForMetadataQuery(String name, String escape)
    {
        return name;
    }

    @Override
    public ResultSet getTables(Connection connection, Optional<String> remoteSchemaName, Optional<String> remoteTableName)
            throws SQLException
    {
        // default is a Trino schema; YDB JDBC has no physical schema with that name.
        return super.getTables(
                connection,
                remoteSchemaName.filter(schema -> !DEFAULT_SCHEMA.equalsIgnoreCase(schema)),
                remoteTableName);
    }

    @Override
    protected boolean filterRemoteSchema(String schemaName)
    {
        return DEFAULT_SCHEMA.equalsIgnoreCase(schemaName);
    }

    @Override
    public Optional<JdbcTableHandle> getTableHandle(ConnectorSession session, SchemaTableName table)
    {
        return filterRemoteSchema(table.getSchemaName()) ? super.getTableHandle(session, table) : Optional.empty();
    }

    @Override
    protected ResultSet getColumns(RemoteTableName table, DatabaseMetaData metadata)
            throws SQLException
    {
        return super.getColumns(new RemoteTableName(
                table.getCatalogName(),
                table.getSchemaName().filter(schema -> !DEFAULT_SCHEMA.equalsIgnoreCase(schema)),
                table.getTableName()), metadata);
    }

    @Override
    protected String getTableRemoteSchemaName(ResultSet resultSet)
    {
        return DEFAULT_SCHEMA;
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(
            ConnectorSession session,
            Connection connection,
            JdbcTypeHandle typeHandle)
    {
        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }

        String jdbcTypeName = typeHandle.jdbcTypeName().orElse("").toLowerCase(Locale.ROOT);
        if (jdbcTypeName.equals("bytes") || jdbcTypeName.equals("string")) {
            return Optional.of(varbinaryColumnMapping());
        }
        if (jdbcTypeName.equals("utf8") || jdbcTypeName.equals("text")) {
            return Optional.of(unboundedVarcharColumnMapping());
        }
        Optional<ColumnMapping> primitiveMapping = switch (jdbcTypeName) {
            case "bool" -> Optional.of(booleanColumnMapping());
            case "int8" -> Optional.of(tinyintColumnMapping());
            case "int16" -> Optional.of(smallintColumnMapping());
            case "int32" -> Optional.of(integerColumnMapping());
            case "int64" -> Optional.of(bigintColumnMapping());
            case "uint8" -> Optional.of(unsignedColumnMapping(SMALLINT, 2));
            case "uint16" -> Optional.of(unsignedColumnMapping(INTEGER, 4));
            case "uint32" -> Optional.of(unsignedColumnMapping(BIGINT, 6));
            case "uint64" -> Optional.of(unsignedBigintColumnMapping());
            case "float" -> {
                ColumnMapping valueMapping = realColumnMapping();
                yield Optional.of(ColumnMapping.mapping(REAL, valueMapping.getReadFunction(), valueMapping.getWriteFunction(), DISABLE_PUSHDOWN));
            }
            case "double" -> {
                ColumnMapping valueMapping = doubleColumnMapping();
                yield Optional.of(ColumnMapping.mapping(DOUBLE, valueMapping.getReadFunction(), valueMapping.getWriteFunction(), DISABLE_PUSHDOWN));
            }
            case "date", "date32" -> Optional.of(dateColumnMapping(typeHandle));
            case "datetime", "datetime64", "timestamp", "timestamp64" -> Optional.of(timestampColumnMapping(typeHandle));
            default -> Optional.empty();
        };
        if (primitiveMapping.isPresent()) {
            return primitiveMapping;
        }
        if (typeHandle.jdbcType() != Types.DECIMAL) {
            return getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR ? mapToUnboundedVarchar(typeHandle) : Optional.empty();
        }

        Optional<ColumnMapping> columnMapping = switch (typeHandle.jdbcType()) {
            case Types.DECIMAL -> {
                String typeName = typeHandle.jdbcTypeName().orElse("Decimal");
                int precision = typeHandle.columnSize().orElse(YDB_DEFAULT_DECIMAL_PRECISION);
                int scale = typeHandle.decimalDigits().orElse(YDB_DEFAULT_DECIMAL_SCALE);
                int start = typeName.indexOf('(');
                int end = typeName.indexOf(')');
                if (start >= 0 && end > start) {
                    String[] parts = typeName.substring(start + 1, end).split(",");
                    if (parts.length == 2) {
                        Integer typeNamePrecision = Ints.tryParse(parts[0].trim());
                        Integer typeNameScale = Ints.tryParse(parts[1].trim());
                        if (typeNamePrecision != null && typeNameScale != null) {
                            precision = typeNamePrecision;
                            scale = typeNameScale;
                        }
                    }
                }

                DecimalType decimalType = createDecimalType(precision, max(scale, 0));
                ColumnMapping decimalMapping = decimalColumnMapping(decimalType);
                yield Optional.of(ColumnMapping.mapping(
                        decimalType,
                        decimalMapping.getReadFunction(),
                        decimalMapping.getWriteFunction(),
                        DISABLE_PUSHDOWN));
            }
            default -> Optional.empty();
        };

        if (columnMapping.isPresent()) {
            return columnMapping;
        }

        return mapToUnboundedVarchar(typeHandle);
    }

    private static ColumnMapping unboundedVarcharColumnMapping()
    {
        VarcharType varcharType = createUnboundedVarcharType();
        return ColumnMapping.sliceMapping(
                varcharType,
                varcharReadFunction(varcharType),
                varcharWriteFunction(),
                FULL_PUSHDOWN);
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type == BOOLEAN) {
            return WriteMapping.booleanMapping("Bool", BooleanWriteFunction.of(Types.BOOLEAN, PreparedStatement::setBoolean));
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
            return WriteMapping.longMapping("Float", realWriteFunction());
        }
        if (type == DOUBLE) {
            return WriteMapping.doubleMapping("Double", doubleWriteFunction());
        }
        if (type instanceof DecimalType decimalType) {
            if (decimalType.getPrecision() > 35) {
                throw new TrinoException(NOT_SUPPORTED, "YDB Decimal precision cannot exceed 35");
            }
            String dataType = format("Decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            ColumnMapping mapping = decimalColumnMapping(decimalType);
            return decimalType.isShort()
                    ? WriteMapping.longMapping(dataType, (io.trino.plugin.jdbc.LongWriteFunction) mapping.getWriteFunction())
                    : WriteMapping.objectMapping(dataType, (io.trino.plugin.jdbc.ObjectWriteFunction) mapping.getWriteFunction());
        }
        if (type instanceof VarcharType) {
            return WriteMapping.sliceMapping("Text", varcharWriteFunction());
        }
        if (type == VARBINARY) {
            return WriteMapping.sliceMapping("Bytes", varbinaryWriteFunction());
        }
        if (type == DATE) {
            return WriteMapping.longMapping("Date32", dateWriteFunction(YDB_DATE32_SQL_TYPE));
        }
        if (type == TIMESTAMP_MILLIS || type == TIMESTAMP_MICROS) {
            return WriteMapping.longMapping("Timestamp64", timestampWriteFunction(YDB_TIMESTAMP64_SQL_TYPE));
        }

        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type);
    }

    @Override
    public boolean supportsTopN(ConnectorSession session, JdbcTableHandle handle, List<JdbcSortItem> sortOrder)
    {
        return sortOrder.stream().allMatch(item -> hasSupportedValueMapping(item.column())
                && item.column().getJdbcTypeHandle().jdbcTypeName().filter("Uint64"::equalsIgnoreCase).isEmpty());
    }

    @Override
    protected Optional<TopNFunction> topNFunction()
    {
        // YQL's native NULL and floating-point ordering differs from Trino.
        return Optional.of((query, sortItems, limit) -> {
            String orderBy = sortItems.stream()
                    .flatMap(sortItem -> {
                        String columnName = quoted(sortItem.column().getColumnName());
                        SortOrder sortOrder = sortItem.sortOrder();
                        String nullSort = "CASE WHEN %s IS NULL THEN %s ELSE %s END ASC".formatted(
                                columnName, sortOrder.isNullsFirst() ? 0 : 1, sortOrder.isNullsFirst() ? 1 : 0);
                        String direction = sortOrder.isAscending() ? "ASC" : "DESC";
                        Type type = sortItem.column().getColumnType();
                        if (type.equals(REAL) || type.equals(DOUBLE)) {
                            String nanSort = "CASE WHEN %1$s != %1$s THEN 1 ELSE 0 END %2$s".formatted(columnName, direction);
                            return Stream.of(nullSort, nanSort, "NANVL(%s, 0) %s".formatted(columnName, direction));
                        }
                        return Stream.of(nullSort, columnName + " " + direction);
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
    protected String quoted(@Nullable String catalog, @Nullable String schema, String table)
    {
        // YDB doesn't use catalog & schema in table names, only the table path
        return quoted(table);
    }

    @Override
    public boolean supportsRetries()
    {
        // Retrying writes requires an idempotent commit protocol.
        return false;
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating schemas");
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support dropping schemas");
    }

    @Override
    public void renameSchema(ConnectorSession session, String schemaName, String newSchemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming schemas");
    }

    @Override
    public void setColumnType(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Type type)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support setting column types");
    }

    @Override
    public void truncateTable(ConnectorSession session, JdbcTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support truncating tables");
    }

    @Override
    public void renameTable(ConnectorSession session, JdbcTableHandle handle, SchemaTableName newTableName)
    {
        SchemaTableName currentName = handle.asPlainTable().getSchemaTableName();
        if (!currentName.getSchemaName().equalsIgnoreCase(newTableName.getSchemaName())) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming tables across schemas");
        }
        super.renameTable(session, handle, newTableName);
    }

    @Override
    protected String getColumnDefinitionSql(ConnectorSession session, ColumnMetadata column, String columnName)
    {
        // YDB restriction, does not support column comments.
        if (column.getComment().isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with column comment");
        }

        StringBuilder sb = new StringBuilder()
                .append(quoted(columnName))
                .append(" ")
                .append(toWriteMapping(session, column.getType()).getDataType());

        if (!column.isNullable()) {
            sb.append(" NOT NULL");
        }
        if (column.getDefaultValue().isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support default column values");
        }

        return sb.toString();
    }

    @Override
    protected void addColumn(
            ConnectorSession session,
            Connection connection,
            RemoteTableName table,
            ColumnMetadata column)
            throws SQLException
    {
        if (!column.isNullable()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support adding not null columns");
        }
        super.addColumn(session, connection, table, column);
    }

    @Override
    protected void renameColumn(
            ConnectorSession session,
            Connection connection,
            RemoteTableName remoteTableName,
            String remoteColumnName,
            String newRemoteColumnName)
            throws SQLException
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming columns");
    }

    @Override
    public List<JdbcColumnHandle> getPrimaryKeys(ConnectorSession session, RemoteTableName remoteTableName)
    {
        String tableName = remoteTableName.getTableName();
        String metadataSchemaName = remoteTableName.getSchemaName().orElse(null);
        SchemaTableName schemaTableName = new SchemaTableName(
                remoteTableName.getSchemaName().orElse(DEFAULT_SCHEMA),
                tableName);
        List<JdbcColumnHandle> columns = getColumnsForPrimaryKeyLookup(session, schemaTableName, remoteTableName);
        Map<String, JdbcColumnHandle> columnsByName = columns.stream()
                .collect(Collectors.toMap(JdbcColumnHandle::getColumnName, Function.identity()));

        try (Connection connection = getConnection(session)) {
            DatabaseMetaData metaData = connection.getMetaData();
            String catalogName = remoteTableName.getCatalogName().orElse(null);

            try (ResultSet primaryKeyResultSet = metaData.getPrimaryKeys(catalogName, metadataSchemaName, tableName)) {
                Map<Short, JdbcColumnHandle> primaryKeysBySequence = new TreeMap<>();

                while (primaryKeyResultSet.next()) {
                    String columnName = primaryKeyResultSet.getString("COLUMN_NAME");
                    JdbcColumnHandle column = columnsByName.get(columnName);
                    if (column == null) {
                        throw new TrinoException(
                                JDBC_ERROR,
                                "Primary key column '%s' is absent from JDBC column metadata for %s"
                                        .formatted(columnName, remoteTableName));
                    }
                    short keySequence = primaryKeyResultSet.getShort("KEY_SEQ");
                    if (primaryKeysBySequence.put(keySequence, column) != null) {
                        throw new TrinoException(
                                JDBC_ERROR,
                                "Duplicate primary key sequence %s for %s".formatted(keySequence, remoteTableName));
                    }
                }
                return ImmutableList.copyOf(primaryKeysBySequence.values());
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, "Failed to read primary key metadata for " + remoteTableName, e);
        }
    }

    protected List<JdbcColumnHandle> getColumnsForPrimaryKeyLookup(
            ConnectorSession session,
            SchemaTableName schemaTableName,
            RemoteTableName remoteTableName)
    {
        return getColumns(session, schemaTableName, remoteTableName);
    }

    @Override
    public boolean supportsMerge()
    {
        return true;
    }

    @Override
    protected void copyTableSchema(
            ConnectorSession session,
            Connection connection,
            String catalogName,
            String schemaName,
            String tableName,
            String newTableName,
            List<String> columnNames)
    {
        RemoteTableName remoteTable = new RemoteTableName(Optional.ofNullable(catalogName), Optional.ofNullable(schemaName), tableName);
        Map<String, JdbcColumnHandle> sourceColumns = getColumns(session, new SchemaTableName(DEFAULT_SCHEMA, tableName), remoteTable).stream()
                .collect(Collectors.toMap(JdbcColumnHandle::getColumnName, Function.identity()));
        String stagingKey = "_trino_ydb_staging_key";
        while (columnNames.contains(stagingKey)) {
            stagingKey += "_";
        }
        String declarations = columnNames.stream().map(name -> {
            JdbcColumnHandle column = sourceColumns.get(name);
            String nativeType = column.getJdbcTypeHandle().jdbcTypeName().orElseThrow();
            if (nativeType.equalsIgnoreCase("Decimal")) {
                nativeType = toWriteMapping(session, column.getColumnType()).getDataType();
            }
            return quoted(name) + " " + nativeType;
        }).collect(joining(", "));
        String sql = "CREATE TABLE %s (%s, %s Serial, PRIMARY KEY (%s))".formatted(
                quoted(newTableName), declarations, quoted(stagingKey), quoted(stagingKey));
        try {
            execute(session, connection, sql);
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, "Failed to create YDB INSERT staging table", e);
        }
    }

    @Override
    public JdbcMergeTableHandle beginMerge(
            ConnectorSession session,
            JdbcTableHandle handle,
            Map<Integer, Collection<ColumnHandle>> updateColumnHandles,
            Consumer<Runnable> rollbackActionCollector,
            RetryMode retryMode)
    {
        if (retryMode != RetryMode.NO_RETRIES) {
            throw new TrinoException(NOT_SUPPORTED, "Query and task retries are not supported for direct YDB MERGE");
        }

        List<JdbcColumnHandle> primaryKeys = getPrimaryKeys(session, handle.getRequiredNamedRelation().getRemoteTableName());
        if (primaryKeys.isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "The connector cannot perform MERGE on a table without a primary key");
        }
        updateColumnHandles.values().forEach(columns -> verifyNoPrimaryKeyUpdate(primaryKeys, columns));

        SchemaTableName schemaTableName = handle.getRequiredNamedRelation().getSchemaTableName();
        RemoteTableName remoteTableName = handle.getRequiredNamedRelation().getRemoteTableName();
        RemoteTableName outputRemoteTableName = new RemoteTableName(
                remoteTableName.getCatalogName(),
                Optional.of(schemaTableName.getSchemaName()),
                remoteTableName.getTableName());

        List<JdbcColumnHandle> columns = getColumns(session, schemaTableName, remoteTableName);

        JdbcOutputTableHandle outputTableHandle = new JdbcOutputTableHandle(
                outputRemoteTableName,
                columns.stream().map(JdbcColumnHandle::getColumnName).toList(),
                columns.stream().map(JdbcColumnHandle::getColumnType).toList(),
                Optional.of(columns.stream().map(JdbcColumnHandle::getJdbcTypeHandle).toList()),
                Optional.empty(),
                Optional.empty());

        return new JdbcMergeTableHandle(
                handle,
                outputTableHandle,
                ImmutableMap.of(),
                Optional.empty(),
                primaryKeys,
                columns,
                updateColumnHandles);
    }

    @Override
    public void finishMerge(
            ConnectorSession session,
            JdbcMergeTableHandle tableHandle,
            Set<Long> pageSinkIds)
    {
        // The single YdbMergeSink commits all operation kinds in one owned transaction.
    }

    @Override
    public OptionalInt getMaxWriteParallelism(ConnectorSession session)
    {
        // One transaction cannot be shared across writer tasks.
        return OptionalInt.of(1);
    }

    @Override
    public OptionalLong delete(ConnectorSession session, JdbcTableHandle handle)
    {
        try (Connection connection = connectionFactory.openConnection(session)) {
            PreparedQuery preparedQuery = queryBuilder.prepareDeleteQuery(
                    this,
                    session,
                    connection,
                    handle.getRequiredNamedRelation(),
                    handle.getConstraint(),
                    getAdditionalPredicate(handle.getConstraintExpressions(), Optional.empty()));
            return OptionalLong.of(executeReturningDml(session, connection, handle, preparedQuery));
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    public OptionalLong update(ConnectorSession session, JdbcTableHandle handle)
    {
        verifyNoPrimaryKeyUpdate(
                getPrimaryKeys(session, handle.getRequiredNamedRelation().getRemoteTableName()),
                handle.getUpdateAssignments().stream().map(assignment -> (ColumnHandle) assignment.column()).toList());
        try (Connection connection = connectionFactory.openConnection(session)) {
            PreparedQuery preparedQuery = queryBuilder.prepareUpdateQuery(
                    this,
                    session,
                    connection,
                    handle.getRequiredNamedRelation(),
                    handle.getConstraint(),
                    getAdditionalPredicate(handle.getConstraintExpressions(), Optional.empty()),
                    handle.getUpdateAssignments());
            return OptionalLong.of(executeReturningDml(session, connection, handle, preparedQuery));
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    private static void verifyNoPrimaryKeyUpdate(List<JdbcColumnHandle> primaryKeys, Collection<ColumnHandle> updatedColumns)
    {
        if (updatedColumns.stream().anyMatch(primaryKeys::contains)) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    "YDB does not support updating primary key columns: https://ydb.tech/docs/en/yql/reference/syntax/update");
        }
    }

    private long executeReturningDml(
            ConnectorSession session,
            Connection connection,
            JdbcTableHandle handle,
            PreparedQuery preparedQuery)
            throws SQLException
    {
        List<JdbcColumnHandle> primaryKeys = getPrimaryKeys(
                session,
                handle.getRequiredNamedRelation().getRemoteTableName());
        if (primaryKeys.isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "YDB DML requires a table primary key");
        }
        PreparedQuery returningQuery = preparedQuery.transformQuery(
                query -> query + " RETURNING " + quoted(primaryKeys.getFirst().getColumnName()));

        long affectedRows = 0;
        try (PreparedStatement statement = queryBuilder.prepareStatement(this, session, connection, returningQuery, Optional.of(1));
                ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                affectedRows++;
            }
        }
        return affectedRows;
    }

    @Override
    protected List<String> createTableSqls(RemoteTableName remoteTableName, List<String> columns, ConnectorTableMetadata tableMetadata)
    {
        if (tableMetadata.getComment().isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables with table comment");
        }

        List<String> primaryKeys = YdbTableProperties.getPrimaryKey(tableMetadata.getProperties());
        if (primaryKeys.isEmpty()) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, "Table property 'primary_key' must contain at least one column");
        }
        if (primaryKeys.contains(null)) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, "Table property 'primary_key' must not contain null columns");
        }
        if (primaryKeys.stream().distinct().count() != primaryKeys.size()) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, "Table property 'primary_key' contains duplicate columns");
        }

        Set<String> columnNames = tableMetadata.getColumns().stream()
                .map(ColumnMetadata::getName)
                .collect(Collectors.toSet());
        primaryKeys.stream()
                .filter(primaryKey -> !columnNames.contains(primaryKey))
                .findFirst()
                .ifPresent(primaryKey -> {
                    throw new TrinoException(
                            INVALID_TABLE_PROPERTY,
                            "Column '%s' specified in table property '%s' does not exist".formatted(primaryKey, PRIMARY_KEY_PROPERTY));
                });

        return List.of("CREATE TABLE %s (%s, PRIMARY KEY (%s))".formatted(
                quoted(remoteTableName),
                String.join(", ", columns),
                primaryKeys.stream().map(this::quoted).collect(joining(", "))));
    }

    @Override
    public Map<String, Object> getTableProperties(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        List<String> primaryKeys = getPrimaryKeys(session, tableHandle.getRequiredNamedRelation().getRemoteTableName()).stream()
                .map(JdbcColumnHandle::getColumnName)
                .toList();
        return Map.of(PRIMARY_KEY_PROPERTY, primaryKeys);
    }
}
