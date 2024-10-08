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
package io.trino.plugin.postgresql.rule;

import com.google.common.collect.ImmutableList;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.projection.ProjectFunctionRule;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;

import java.sql.Types;
import java.util.Optional;
import java.util.StringJoiner;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.expression;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static io.trino.spi.expression.StandardFunctions.CAST_FUNCTION_NAME;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static java.util.Objects.requireNonNull;

public final class RewriteVectorDistanceFunction
        implements ProjectFunctionRule<JdbcExpression, ParameterizedExpression>
{
    private static final Capture<ConnectorExpression> LEFT_ARGUMENT = newCapture();
    private static final Capture<ConnectorExpression> RIGHT_ARGUMENT = newCapture();

    private final Pattern<Call> pattern;
    private final String operator;

    public RewriteVectorDistanceFunction(String functionName, String operator)
    {
        pattern = call()
                .with(functionName().equalTo(new FunctionName(requireNonNull(functionName, "functionName is null"))))
                .with(type().matching(type -> type == DOUBLE))
                .with(argumentCount().equalTo(2))
                .with(argument(0).matching(expression().capturedAs(LEFT_ARGUMENT).with(type().matching(RewriteVectorDistanceFunction::isArrayTypeWithRealOrDouble))))
                .with(argument(1).matching(expression().capturedAs(RIGHT_ARGUMENT).with(type().matching(RewriteVectorDistanceFunction::isArrayTypeWithRealOrDouble))));
        this.operator = requireNonNull(operator, "operator is null");
    }

    @Override
    public Pattern<? extends ConnectorExpression> getPattern()
    {
        return pattern;
    }

    @Override
    public Optional<JdbcExpression> rewrite(ConnectorTableHandle handle, ConnectorExpression projectionExpression, Captures captures, RewriteContext<ParameterizedExpression> context)
    {
        Optional<ParameterizedExpression> leftExpression = rewrite(captures.get(LEFT_ARGUMENT), context);
        if (leftExpression.isEmpty()) {
            return Optional.empty();
        }

        Optional<ParameterizedExpression> rightExpression = rewrite(captures.get(RIGHT_ARGUMENT), context);
        if (rightExpression.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(new JdbcExpression(
                "%s %s %s".formatted(leftExpression.get().expression(), operator, rightExpression.get().expression()),
                ImmutableList.<QueryParameter>builder()
                        .addAll(leftExpression.get().parameters())
                        .addAll(rightExpression.get().parameters())
                        .build(),
                new JdbcTypeHandle(
                        Types.DOUBLE,
                        Optional.of("double"),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty())));
    }

    public static Optional<ParameterizedExpression> rewrite(ConnectorExpression expression, RewriteContext<ParameterizedExpression> context)
    {
        if (expression instanceof Constant constant) {
            Type elementType = ((ArrayType) constant.getType()).getElementType();
            Block value = (Block) constant.getValue();
            StringJoiner vector = new StringJoiner(",", "'[", "]'");
            for (int i = 0; i < value.getPositionCount(); i++) {
                if (value.isNull(i)) {
                    return Optional.empty();
                }
                double doubleValue = elementType.getDouble(value, i);
                if (!isSupportedVector(doubleValue)) {
                    return Optional.empty();
                }
                vector.add(Double.toString(doubleValue));
            }
            return Optional.of(new ParameterizedExpression(vector.toString(), ImmutableList.of()));
        }
        if (expression instanceof Call call && call.getFunctionName().equals(CAST_FUNCTION_NAME)) {
            ConnectorExpression argument = getOnlyElement(call.getArguments());
            if (argument instanceof Variable variable) {
                JdbcColumnHandle columnHandle = (JdbcColumnHandle) context.getAssignment(variable.getName());
                JdbcTypeHandle typeHandle = columnHandle.getJdbcTypeHandle();
                // TODO type.equals("vector") should be improved to support pushdown on vector type which is installed in other schemas
                if (!typeHandle.jdbcTypeName().map(type -> type.equals("vector")).orElse(false)) {
                    return Optional.empty();
                }
                return Optional.of(new ParameterizedExpression(quoted(columnHandle.getColumnName()), ImmutableList.of()));
            }
            return Optional.empty();
        }
        Optional<ParameterizedExpression> translatedArgument = context.rewriteExpression(expression);
        if (translatedArgument.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(translatedArgument.orElseThrow());
    }

    public static boolean isArrayTypeWithRealOrDouble(Type type)
    {
        return type instanceof ArrayType arrayType && (arrayType.getElementType() == REAL || arrayType.getElementType() == DOUBLE);
    }

    private static boolean isSupportedVector(double value)
    {
        return !Double.isNaN(value) &&
                !Double.isInfinite(value) &&
                // pgvector throws an exception if the value is outside of float type
                (value >= Float.MIN_VALUE && value <= Float.MAX_VALUE);
    }

    private static String quoted(String name)
    {
        name = name.replace("\"", "\"\"");
        return "\"" + name + "\"";
    }
}
