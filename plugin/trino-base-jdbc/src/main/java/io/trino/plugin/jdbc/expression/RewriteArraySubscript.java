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
package io.trino.plugin.jdbc.expression;

import com.google.common.collect.ImmutableList;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.plugin.base.projection.ProjectFunctionRule;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.ArrayType;

import java.util.Optional;
import java.util.function.BiFunction;

import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.expression;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.variable;
import static java.util.Objects.requireNonNull;

public class RewriteArraySubscript
{
    private static final Capture<Variable> VALUE = newCapture();
    private static final Capture<ConnectorExpression> INDEX = newCapture();
    private static final Pattern<Call> PATTERN = call()
            .with(functionName().equalTo(new FunctionName("$operator$subscript")))
            .with(type().matching(type -> !(type instanceof ArrayType)))
            .with(argumentCount().equalTo(2))
            .with(argument(0).matching(variable().capturedAs(VALUE)))
            .with(argument(1).matching(expression().capturedAs(INDEX)));

    public static class RewriteArraySubscriptFilter
            implements ConnectorExpressionRule<Call, ParameterizedExpression>
    {
        private final BiFunction<ConnectorSession, JdbcColumnHandle, JdbcTypeHandle> arrayElementTypeHandleProvider;

        public RewriteArraySubscriptFilter(BiFunction<ConnectorSession, JdbcColumnHandle, JdbcTypeHandle> arrayElementTypeHandleProvider)
        {
            this.arrayElementTypeHandleProvider = requireNonNull(arrayElementTypeHandleProvider, "arrayElementTypeHandleProvider is null");
        }

        @Override
        public Pattern<Call> getPattern()
        {
            return PATTERN;
        }

        @Override
        public Optional<ParameterizedExpression> rewrite(Call call, Captures captures, RewriteContext<ParameterizedExpression> context)
        {
            Variable value = captures.get(VALUE);
            JdbcColumnHandle columnHandle = (JdbcColumnHandle) context.getAssignment(value.getName());

            Optional<ParameterizedExpression> index = context.defaultRewrite(captures.get(INDEX));
            if (index.isEmpty()) {
                return Optional.empty();
            }

            JdbcTypeHandle arrayElementTypeHandle = arrayElementTypeHandleProvider.apply(context.getSession(), columnHandle);
            if (arrayElementTypeHandle.jdbcTypeName().isEmpty()) {
                return Optional.empty();
            }

            return Optional.of(rewriteArraySubscriptExpression(value, index.get(), arrayElementTypeHandle));
        }
    }

    public static class RewriteArraySubscriptProjection
            implements ProjectFunctionRule<JdbcExpression, ParameterizedExpression>
    {
        private final BiFunction<ConnectorSession, JdbcColumnHandle, JdbcTypeHandle> arrayElementTypeHandleProvider;

        public RewriteArraySubscriptProjection(BiFunction<ConnectorSession, JdbcColumnHandle, JdbcTypeHandle> arrayElementTypeHandleProvider)
        {
            this.arrayElementTypeHandleProvider = requireNonNull(arrayElementTypeHandleProvider, "arrayElementTypeHandleProvider is null");
        }

        @Override
        public Pattern<Call> getPattern()
        {
            return PATTERN;
        }

        @Override
        public Optional<JdbcExpression> rewrite(ConnectorExpression projectionExpression, Captures captures, RewriteContext<ParameterizedExpression> context)
        {
            context.rewriteExpression(projectionExpression);
            Variable value = captures.get(VALUE);
            JdbcColumnHandle columnHandle = (JdbcColumnHandle) context.getAssignment(value.getName());

            Optional<ParameterizedExpression> index = context.rewriteExpression(captures.get(INDEX));
            if (index.isEmpty()) {
                return Optional.empty();
            }

            JdbcTypeHandle arrayElementTypeHandle = arrayElementTypeHandleProvider.apply(context.getSession(), columnHandle);
            if (arrayElementTypeHandle.jdbcTypeName().isEmpty()) {
                return Optional.empty();
            }

            ParameterizedExpression parameterizedExpression = rewriteArraySubscriptExpression(value, index.get(), arrayElementTypeHandle);
            return Optional.of(new JdbcExpression(
                    parameterizedExpression.expression(),
                    parameterizedExpression.parameters(),
                    arrayElementTypeHandle));
        }
    }

    private static ParameterizedExpression rewriteArraySubscriptExpression(Variable value, ParameterizedExpression index, JdbcTypeHandle arrayElementTypeHandle)
    {
        return new ParameterizedExpression(
                "(CASE WHEN (%2$s > 0 AND ARRAY_NDIMS(%1$s) = 1 AND ARRAY_LENGTH(%1$s, 1) >= %2$s) THEN %1$s ELSE ARRAY[null, ARRAY[(SELECT null)]]::%3$s[] END)[%2$s]"
                        .formatted(value.getName(), index.expression(), arrayElementTypeHandle.jdbcTypeName().get()),
                ImmutableList.<QueryParameter>builder()
                        .addAll(index.parameters())
                        .addAll(index.parameters())
                        .addAll(index.parameters())
                        .build());
    }
}
