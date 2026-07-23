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
package io.trino.plugin.trino;

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SortItem;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FieldDereference;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.StandardFunctions;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.NumberType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static io.trino.plugin.trino.TrinoCompatibilityRegistry.canonicalName;
import static io.trino.plugin.trino.TrinoCompatibilityRegistry.isSubscriptFunction;
import static io.trino.plugin.trino.TrinoRemoteCapabilities.CharToVarcharCastSemantics.RETAINS_TRAILING_SPACES;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

final class TrinoRemoteSqlRenderer
{
    private final Function<String, String> identifierQuote;
    private final TrinoCompatibilityRegistry compatibilityRegistry;

    TrinoRemoteSqlRenderer(Function<String, String> identifierQuote, TrinoCompatibilityRegistry compatibilityRegistry)
    {
        this.identifierQuote = requireNonNull(identifierQuote, "identifierQuote is null");
        this.compatibilityRegistry = requireNonNull(compatibilityRegistry, "compatibilityRegistry is null");
    }

    private static final Set<FunctionName> COMPARISON_FUNCTION_NAMES = ImmutableSet.of(
            StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
            StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME,
            StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME,
            StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME,
            StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME,
            StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME,
            StandardFunctions.IDENTICAL_OPERATOR_FUNCTION_NAME);

    Optional<ParameterizedExpression> renderExpression(
            ConnectorSession session,
            ConnectorExpression expression,
            Map<String, ColumnHandle> assignments,
            TrinoRemoteCapabilities capabilities)
    {
        requireNonNull(session, "session is null");
        requireNonNull(expression, "expression is null");
        requireNonNull(assignments, "assignments is null");
        requireNonNull(capabilities, "capabilities is null");
        if (isBareConstantComparison(expression)) {
            // Top-level comparisons of a bare column against a constant are reserved
            // for the baseline rewriter: the upstream join pushdown contract expects
            // constant join conditions to delegate only through its rules (exact
            // numeric and varchar constants). On the predicate path this shape only
            // arrives as a join condition (in WHERE clauses it is subsumed by the
            // tuple domain), but top-level projections of this shape reach here too
            // and deliberately fall back to local evaluation. NUMBER stays on the
            // renderer path: its comparisons are not representable as tuple domains
            // and need the typed bind expression.
            return Optional.empty();
        }
        return render(session, expression, assignments, capabilities);
    }

    private static boolean isBareConstantComparison(ConnectorExpression expression)
    {
        if (!(expression instanceof Call call) || call.getArguments().size() != 2) {
            return false;
        }
        if (!COMPARISON_FUNCTION_NAMES.contains(call.getFunctionName())) {
            return false;
        }
        ConnectorExpression left = call.getArguments().get(0);
        ConnectorExpression right = call.getArguments().get(1);
        boolean bareComparison = (left instanceof Variable && isParameterConstant(right)) ||
                (isParameterConstant(left) && right instanceof Variable);
        return bareComparison &&
                !(left.getType() instanceof NumberType) &&
                !(right.getType() instanceof NumberType);
    }

    private static boolean isParameterConstant(ConnectorExpression expression)
    {
        // NULL constants render as typed literals without parameters, so they are
        // not subject to the constant binding contract above
        return expression instanceof Constant constant && constant.getValue() != null;
    }

    Optional<JdbcExpression> renderProjection(
            ConnectorSession session,
            ConnectorExpression expression,
            Map<String, ColumnHandle> assignments,
            TrinoRemoteCapabilities capabilities)
    {
        return renderExpression(session, expression, assignments, capabilities)
                .map(rendered -> new JdbcExpression(
                        rendered.expression(),
                        rendered.parameters(),
                        jdbcTypeHandleFor(expression.getType())));
    }

    Optional<JdbcExpression> renderAggregation(
            ConnectorSession session,
            AggregateFunction aggregate,
            Map<String, ColumnHandle> assignments,
            TrinoRemoteCapabilities capabilities)
    {
        requireNonNull(session, "session is null");
        requireNonNull(aggregate, "aggregate is null");
        requireNonNull(assignments, "assignments is null");
        requireNonNull(capabilities, "capabilities is null");
        if (!compatibilityRegistry.isAggregationSupported(aggregate, capabilities)) {
            return Optional.empty();
        }

        List<String> arguments = new ArrayList<>();
        List<QueryParameter> parameters = new ArrayList<>();
        for (ConnectorExpression argument : aggregate.getArguments()) {
            Optional<ParameterizedExpression> renderedArgument = render(session, argument, assignments, capabilities);
            if (renderedArgument.isEmpty()) {
                return Optional.empty();
            }
            arguments.add(renderedArgument.orElseThrow().expression());
            parameters.addAll(renderedArgument.orElseThrow().parameters());
        }

        String name = aggregate.getFunctionName();
        String argumentSql;
        if (arguments.isEmpty() && name.equalsIgnoreCase("count")) {
            argumentSql = "*";
        }
        else {
            argumentSql = String.join(", ", arguments);
            if (aggregate.isDistinct()) {
                argumentSql = "DISTINCT " + argumentSql;
            }
        }

        StringBuilder sql = new StringBuilder(name).append("(").append(argumentSql);
        if (!aggregate.getSortItems().isEmpty()) {
            List<String> sortItems = new ArrayList<>();
            for (SortItem sortItem : aggregate.getSortItems()) {
                ColumnHandle assignment = assignments.get(sortItem.getName());
                if (!(assignment instanceof JdbcColumnHandle column)) {
                    return Optional.empty();
                }
                sortItems.add(identifierQuote.apply(column.getColumnName()) + " " + sortOrder(sortItem));
            }
            sql.append(" ORDER BY ").append(String.join(", ", sortItems));
        }
        sql.append(")");

        if (aggregate.getFilter().isPresent()) {
            Optional<ParameterizedExpression> filter = render(session, aggregate.getFilter().orElseThrow(), assignments, capabilities);
            if (filter.isEmpty()) {
                return Optional.empty();
            }
            sql.append(" FILTER (WHERE ").append(filter.orElseThrow().expression()).append(")");
            parameters.addAll(filter.orElseThrow().parameters());
        }

        return Optional.of(new JdbcExpression(sql.toString(), List.copyOf(parameters), jdbcTypeHandleFor(aggregate.getOutputType())));
    }

    static JdbcTypeHandle jdbcTypeHandleFor(Type type)
    {
        if (type instanceof BooleanType) {
            return new JdbcTypeHandle(Types.BOOLEAN, Optional.of("boolean"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        }
        if (type instanceof TinyintType) {
            return new JdbcTypeHandle(Types.TINYINT, Optional.of("tinyint"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        }
        if (type instanceof SmallintType) {
            return new JdbcTypeHandle(Types.SMALLINT, Optional.of("smallint"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        }
        if (type instanceof IntegerType) {
            return new JdbcTypeHandle(Types.INTEGER, Optional.of("integer"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        }
        if (type instanceof BigintType) {
            return new JdbcTypeHandle(Types.BIGINT, Optional.of("bigint"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        }
        if (type instanceof RealType) {
            return new JdbcTypeHandle(Types.REAL, Optional.of("real"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        }
        if (type instanceof DoubleType) {
            return new JdbcTypeHandle(Types.DOUBLE, Optional.of("double"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        }
        if (type instanceof DecimalType decimalType) {
            return new JdbcTypeHandle(
                    Types.DECIMAL,
                    Optional.of(type.getDisplayName()),
                    Optional.of(decimalType.getPrecision()),
                    Optional.of(decimalType.getScale()),
                    Optional.empty(),
                    Optional.empty());
        }
        if (type instanceof NumberType) {
            return new JdbcTypeHandle(Types.OTHER, Optional.of("number"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        }
        if (type instanceof CharType charType) {
            return new JdbcTypeHandle(Types.CHAR, Optional.of(type.getDisplayName()), Optional.of(charType.getLength()), Optional.empty(), Optional.empty(), Optional.empty());
        }
        if (type instanceof VarcharType varcharType) {
            return new JdbcTypeHandle(Types.VARCHAR, Optional.of(type.getDisplayName()), varcharType.getLength(), Optional.empty(), Optional.empty(), Optional.empty());
        }
        if (type instanceof VarbinaryType) {
            return new JdbcTypeHandle(Types.VARBINARY, Optional.of("varbinary"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        }
        if (type instanceof DateType) {
            return new JdbcTypeHandle(Types.DATE, Optional.of("date"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        }
        if (type instanceof TimeType timeType) {
            return new JdbcTypeHandle(Types.TIME, Optional.of(type.getDisplayName()), Optional.of(timeType.getPrecision()), Optional.empty(), Optional.empty(), Optional.empty());
        }
        if (type instanceof TimeWithTimeZoneType timeWithTimeZoneType) {
            return new JdbcTypeHandle(Types.TIME_WITH_TIMEZONE, Optional.of(type.getDisplayName()), Optional.of(timeWithTimeZoneType.getPrecision()), Optional.empty(), Optional.empty(), Optional.empty());
        }
        if (type instanceof TimestampType timestampType) {
            return new JdbcTypeHandle(Types.TIMESTAMP, Optional.of(type.getDisplayName()), Optional.of(timestampType.getPrecision()), Optional.empty(), Optional.empty(), Optional.empty());
        }
        if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
            return new JdbcTypeHandle(Types.TIMESTAMP_WITH_TIMEZONE, Optional.of(type.getDisplayName()), Optional.of(timestampWithTimeZoneType.getPrecision()), Optional.empty(), Optional.empty(), Optional.empty());
        }
        if (type instanceof ArrayType) {
            return new JdbcTypeHandle(Types.ARRAY, Optional.of(type.getDisplayName()), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        }
        return new JdbcTypeHandle(Types.JAVA_OBJECT, Optional.of(type.getDisplayName()), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    private Optional<ParameterizedExpression> render(
            ConnectorSession session,
            ConnectorExpression expression,
            Map<String, ColumnHandle> assignments,
            TrinoRemoteCapabilities capabilities)
    {
        if (expression instanceof Variable variable) {
            ColumnHandle assignment = assignments.get(variable.getName());
            if (!(assignment instanceof JdbcColumnHandle column)) {
                return Optional.empty();
            }
            return Optional.of(new ParameterizedExpression(identifierQuote.apply(column.getColumnName()), List.of()));
        }
        if (expression instanceof Constant constant) {
            return TrinoParameterBindingFactory.bindConstant(constant);
        }
        if (expression instanceof FieldDereference fieldDereference) {
            return renderFieldDereference(session, fieldDereference, assignments, capabilities);
        }
        if (expression instanceof Call call) {
            return renderCall(session, call, assignments, capabilities);
        }
        return Optional.empty();
    }

    private Optional<ParameterizedExpression> renderFieldDereference(
            ConnectorSession session,
            FieldDereference fieldDereference,
            Map<String, ColumnHandle> assignments,
            TrinoRemoteCapabilities capabilities)
    {
        Optional<ParameterizedExpression> target = render(session, fieldDereference.getTarget(), assignments, capabilities);
        if (target.isEmpty()) {
            return Optional.empty();
        }

        String fieldReference;
        Type targetType = fieldDereference.getTarget().getType();
        if (targetType instanceof RowType rowType && rowType.getFields().get(fieldDereference.getField()).getName().isPresent()) {
            fieldReference = target.orElseThrow().expression() + "." + identifierQuote.apply(rowType.getFields().get(fieldDereference.getField()).getName().orElseThrow());
        }
        else {
            fieldReference = target.orElseThrow().expression() + "[" + (fieldDereference.getField() + 1) + "]";
        }
        return Optional.of(new ParameterizedExpression(fieldReference, target.orElseThrow().parameters()));
    }

    private Optional<ParameterizedExpression> renderCall(
            ConnectorSession session,
            Call call,
            Map<String, ColumnHandle> assignments,
            TrinoRemoteCapabilities capabilities)
    {
        FunctionName functionName = call.getFunctionName();
        if (!compatibilityRegistry.isFunctionSupported(session, call, capabilities)) {
            return Optional.empty();
        }

        if (functionName.equals(StandardFunctions.CAST_FUNCTION_NAME) || functionName.equals(StandardFunctions.TRY_CAST_FUNCTION_NAME)) {
            return renderCast(session, call, assignments, capabilities, functionName.equals(StandardFunctions.TRY_CAST_FUNCTION_NAME));
        }
        if (functionName.equals(StandardFunctions.AND_FUNCTION_NAME)) {
            return renderNary(session, call, assignments, capabilities, "AND");
        }
        if (functionName.equals(StandardFunctions.OR_FUNCTION_NAME)) {
            return renderNary(session, call, assignments, capabilities, "OR");
        }
        if (functionName.equals(StandardFunctions.NOT_FUNCTION_NAME)) {
            return renderUnary(session, call, assignments, capabilities, "NOT ", "");
        }
        if (functionName.equals(StandardFunctions.IS_NULL_FUNCTION_NAME)) {
            return renderUnary(session, call, assignments, capabilities, "", " IS NULL");
        }
        if (functionName.equals(StandardFunctions.NULLIF_FUNCTION_NAME)) {
            return renderFunction(session, call, assignments, capabilities, "NULLIF");
        }
        if (functionName.equals(StandardFunctions.IDENTICAL_OPERATOR_FUNCTION_NAME)) {
            return renderBinary(session, call, assignments, capabilities, "IS NOT DISTINCT FROM");
        }
        if (functionName.equals(StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME)) {
            return renderBinary(session, call, assignments, capabilities, "=");
        }
        if (functionName.equals(StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME)) {
            return renderBinary(session, call, assignments, capabilities, "<>");
        }
        if (functionName.equals(StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME)) {
            return renderBinary(session, call, assignments, capabilities, "<");
        }
        if (functionName.equals(StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME)) {
            return renderBinary(session, call, assignments, capabilities, "<=");
        }
        if (functionName.equals(StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME)) {
            return renderBinary(session, call, assignments, capabilities, ">");
        }
        if (functionName.equals(StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME)) {
            return renderBinary(session, call, assignments, capabilities, ">=");
        }
        if (functionName.equals(StandardFunctions.ADD_FUNCTION_NAME)) {
            return renderBinary(session, call, assignments, capabilities, "+");
        }
        if (functionName.equals(StandardFunctions.SUBTRACT_FUNCTION_NAME)) {
            return renderBinary(session, call, assignments, capabilities, "-");
        }
        if (functionName.equals(StandardFunctions.MULTIPLY_FUNCTION_NAME)) {
            return renderBinary(session, call, assignments, capabilities, "*");
        }
        if (functionName.equals(StandardFunctions.DIVIDE_FUNCTION_NAME)) {
            return renderBinary(session, call, assignments, capabilities, "/");
        }
        if (functionName.equals(StandardFunctions.MODULO_FUNCTION_NAME)) {
            return renderBinary(session, call, assignments, capabilities, "%");
        }
        if (functionName.equals(StandardFunctions.NEGATE_FUNCTION_NAME)) {
            return renderUnary(session, call, assignments, capabilities, "-", "");
        }
        if (functionName.equals(StandardFunctions.LIKE_FUNCTION_NAME)) {
            return renderLike(session, call, assignments, capabilities);
        }
        if (functionName.equals(StandardFunctions.IN_PREDICATE_FUNCTION_NAME)) {
            return renderInPredicate(session, call, assignments, capabilities);
        }
        if (functionName.equals(StandardFunctions.ARRAY_CONSTRUCTOR_FUNCTION_NAME)) {
            return renderArrayConstructor(session, call, assignments, capabilities);
        }

        String canonicalName = canonicalName(functionName);
        if (isSubscriptFunction(canonicalName)) {
            return renderSubscript(session, call, assignments, capabilities);
        }
        return renderFunction(session, call, assignments, capabilities, canonicalName);
    }

    private Optional<ParameterizedExpression> renderCast(
            ConnectorSession session,
            Call call,
            Map<String, ColumnHandle> assignments,
            TrinoRemoteCapabilities capabilities,
            boolean tryCast)
    {
        if (call.getArguments().size() != 1) {
            return Optional.empty();
        }
        Optional<ParameterizedExpression> value = render(session, call.getArguments().getFirst(), assignments, capabilities);
        if (value.isEmpty()) {
            return Optional.empty();
        }

        ParameterizedExpression renderedValue = value.orElseThrow();
        String sourceExpression = renderedValue.expression();
        Type sourceType = call.getArguments().getFirst().getType();
        Type targetType = call.getType();
        if (sourceType instanceof CharType && targetType instanceof VarcharType) {
            Optional<TrinoRemoteCapabilities.CharToVarcharCastSemantics> semantics = capabilities.charToVarcharCastSemantics();
            if (semantics.isEmpty()) {
                return Optional.empty();
            }
            if (semantics.orElseThrow() == RETAINS_TRAILING_SPACES) {
                sourceExpression = "trim(TRAILING ' ' FROM " + sourceExpression + ")";
            }
        }

        return Optional.of(new ParameterizedExpression(
                (tryCast ? "TRY_CAST(" : "CAST(") + sourceExpression + " AS " + typeName(targetType) + ")",
                renderedValue.parameters()));
    }

    private Optional<ParameterizedExpression> renderNary(
            ConnectorSession session,
            Call call,
            Map<String, ColumnHandle> assignments,
            TrinoRemoteCapabilities capabilities,
            String operator)
    {
        if (call.getArguments().isEmpty()) {
            return Optional.empty();
        }
        List<ParameterizedExpression> renderedArguments = new ArrayList<>();
        for (ConnectorExpression argument : call.getArguments()) {
            Optional<ParameterizedExpression> rendered = render(session, argument, assignments, capabilities);
            if (rendered.isEmpty()) {
                return Optional.empty();
            }
            renderedArguments.add(rendered.orElseThrow());
        }
        return Optional.of(joinExpressions(renderedArguments, " " + operator + " "));
    }

    private Optional<ParameterizedExpression> renderUnary(
            ConnectorSession session,
            Call call,
            Map<String, ColumnHandle> assignments,
            TrinoRemoteCapabilities capabilities,
            String prefix,
            String suffix)
    {
        if (call.getArguments().size() != 1) {
            return Optional.empty();
        }
        Optional<ParameterizedExpression> value = render(session, call.getArguments().getFirst(), assignments, capabilities);
        if (value.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new ParameterizedExpression(
                "(" + prefix + value.orElseThrow().expression() + suffix + ")",
                value.orElseThrow().parameters()));
    }

    private Optional<ParameterizedExpression> renderBinary(
            ConnectorSession session,
            Call call,
            Map<String, ColumnHandle> assignments,
            TrinoRemoteCapabilities capabilities,
            String operator)
    {
        if (call.getArguments().size() != 2) {
            return Optional.empty();
        }
        Optional<ParameterizedExpression> left = render(session, call.getArguments().get(0), assignments, capabilities);
        Optional<ParameterizedExpression> right = render(session, call.getArguments().get(1), assignments, capabilities);
        if (left.isEmpty() || right.isEmpty()) {
            return Optional.empty();
        }

        List<QueryParameter> parameters = new ArrayList<>();
        parameters.addAll(left.orElseThrow().parameters());
        parameters.addAll(right.orElseThrow().parameters());
        return Optional.of(new ParameterizedExpression(
                format("(%s %s %s)", left.orElseThrow().expression(), operator, right.orElseThrow().expression()),
                List.copyOf(parameters)));
    }

    private Optional<ParameterizedExpression> renderLike(
            ConnectorSession session,
            Call call,
            Map<String, ColumnHandle> assignments,
            TrinoRemoteCapabilities capabilities)
    {
        if (call.getArguments().size() < 2 || call.getArguments().size() > 3) {
            return Optional.empty();
        }
        Optional<ParameterizedExpression> value = render(session, call.getArguments().get(0), assignments, capabilities);
        Optional<ParameterizedExpression> pattern = render(session, call.getArguments().get(1), assignments, capabilities);
        if (value.isEmpty() || pattern.isEmpty()) {
            return Optional.empty();
        }
        List<QueryParameter> parameters = new ArrayList<>();
        parameters.addAll(value.orElseThrow().parameters());
        parameters.addAll(pattern.orElseThrow().parameters());
        String sql = "(" + value.orElseThrow().expression() + " LIKE " + pattern.orElseThrow().expression();
        if (call.getArguments().size() == 3) {
            Optional<ParameterizedExpression> escape = render(session, call.getArguments().get(2), assignments, capabilities);
            if (escape.isEmpty()) {
                return Optional.empty();
            }
            sql += " ESCAPE " + escape.orElseThrow().expression();
            parameters.addAll(escape.orElseThrow().parameters());
        }
        return Optional.of(new ParameterizedExpression(sql + ")", List.copyOf(parameters)));
    }

    private Optional<ParameterizedExpression> renderInPredicate(
            ConnectorSession session,
            Call call,
            Map<String, ColumnHandle> assignments,
            TrinoRemoteCapabilities capabilities)
    {
        if (call.getArguments().size() < 2) {
            return Optional.empty();
        }
        Optional<ParameterizedExpression> value = render(session, call.getArguments().getFirst(), assignments, capabilities);
        if (value.isEmpty()) {
            return Optional.empty();
        }

        List<ConnectorExpression> values = call.getArguments().subList(1, call.getArguments().size());
        if (values.size() == 1 &&
                values.getFirst() instanceof Call arrayConstructor &&
                arrayConstructor.getFunctionName().equals(StandardFunctions.ARRAY_CONSTRUCTOR_FUNCTION_NAME)) {
            values = arrayConstructor.getArguments();
        }
        if (values.isEmpty()) {
            return Optional.empty();
        }

        List<String> valueSql = new ArrayList<>();
        List<QueryParameter> parameters = new ArrayList<>(value.orElseThrow().parameters());
        for (ConnectorExpression argument : values) {
            Optional<ParameterizedExpression> rendered = render(session, argument, assignments, capabilities);
            if (rendered.isEmpty()) {
                return Optional.empty();
            }
            valueSql.add(rendered.orElseThrow().expression());
            parameters.addAll(rendered.orElseThrow().parameters());
        }
        return Optional.of(new ParameterizedExpression(
                "(" + value.orElseThrow().expression() + " IN (" + String.join(", ", valueSql) + "))",
                List.copyOf(parameters)));
    }

    private Optional<ParameterizedExpression> renderArrayConstructor(
            ConnectorSession session,
            Call call,
            Map<String, ColumnHandle> assignments,
            TrinoRemoteCapabilities capabilities)
    {
        List<String> values = new ArrayList<>();
        List<QueryParameter> parameters = new ArrayList<>();
        for (ConnectorExpression argument : call.getArguments()) {
            Optional<ParameterizedExpression> rendered = render(session, argument, assignments, capabilities);
            if (rendered.isEmpty()) {
                return Optional.empty();
            }
            values.add(rendered.orElseThrow().expression());
            parameters.addAll(rendered.orElseThrow().parameters());
        }
        return Optional.of(new ParameterizedExpression("ARRAY[" + String.join(", ", values) + "]", List.copyOf(parameters)));
    }

    private Optional<ParameterizedExpression> renderSubscript(
            ConnectorSession session,
            Call call,
            Map<String, ColumnHandle> assignments,
            TrinoRemoteCapabilities capabilities)
    {
        if (call.getArguments().size() != 2) {
            return Optional.empty();
        }
        Optional<ParameterizedExpression> base = render(session, call.getArguments().get(0), assignments, capabilities);
        Optional<ParameterizedExpression> index = render(session, call.getArguments().get(1), assignments, capabilities);
        if (base.isEmpty() || index.isEmpty()) {
            return Optional.empty();
        }
        List<QueryParameter> parameters = new ArrayList<>();
        parameters.addAll(base.orElseThrow().parameters());
        parameters.addAll(index.orElseThrow().parameters());
        return Optional.of(new ParameterizedExpression(
                base.orElseThrow().expression() + "[" + index.orElseThrow().expression() + "]",
                List.copyOf(parameters)));
    }

    private Optional<ParameterizedExpression> renderFunction(
            ConnectorSession session,
            Call call,
            Map<String, ColumnHandle> assignments,
            TrinoRemoteCapabilities capabilities,
            String sqlFunctionName)
    {
        List<String> arguments = new ArrayList<>();
        List<QueryParameter> parameters = new ArrayList<>();
        for (ConnectorExpression argument : call.getArguments()) {
            Optional<ParameterizedExpression> rendered = render(session, argument, assignments, capabilities);
            if (rendered.isEmpty()) {
                return Optional.empty();
            }
            arguments.add(rendered.orElseThrow().expression());
            parameters.addAll(rendered.orElseThrow().parameters());
        }
        return Optional.of(new ParameterizedExpression(
                sqlFunctionName + "(" + String.join(", ", arguments) + ")",
                List.copyOf(parameters)));
    }

    private static ParameterizedExpression joinExpressions(List<ParameterizedExpression> expressions, String separator)
    {
        List<String> sql = new ArrayList<>();
        List<QueryParameter> parameters = new ArrayList<>();
        for (ParameterizedExpression expression : expressions) {
            sql.add(expression.expression());
            parameters.addAll(expression.parameters());
        }
        return new ParameterizedExpression("(" + String.join(separator, sql) + ")", List.copyOf(parameters));
    }

    private static String sortOrder(SortItem sortItem)
    {
        return (sortItem.getSortOrder().isAscending() ? "ASC" : "DESC") +
                (sortItem.getSortOrder().isNullsFirst() ? " NULLS FIRST" : " NULLS LAST");
    }

    private static String typeName(Type type)
    {
        return type.getDisplayName();
    }
}
