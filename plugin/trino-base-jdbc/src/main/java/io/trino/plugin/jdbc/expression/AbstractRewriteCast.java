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

import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.projection.ProjectFunctionRule;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.Type;

import java.util.Optional;
import java.util.function.BiFunction;

import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.variable;
import static io.trino.spi.expression.StandardFunctions.CAST_FUNCTION_NAME;
import static java.util.Objects.requireNonNull;

public abstract class AbstractRewriteCast
        implements ProjectFunctionRule<JdbcExpression, ParameterizedExpression>
{
    private static final Capture<Variable> VALUE = newCapture();

    private final BiFunction<ConnectorSession, Type, String> jdbcTypeProvider;

    protected abstract Optional<JdbcTypeHandle> toJdbcTypeHandle(JdbcTypeHandle sourceType, Type targetType);

    public AbstractRewriteCast(BiFunction<ConnectorSession, Type, String> jdbcTypeProvider)
    {
        this.jdbcTypeProvider = requireNonNull(jdbcTypeProvider, "jdbcTypeProvider is null");
    }

    protected String buildCast(@SuppressWarnings("unused") Type sourceType, @SuppressWarnings("unused") Type targetType, String expression, String castType)
    {
        return "CAST(%s AS %s)".formatted(expression, castType);
    }

    @Override
    public Pattern<Call> getPattern()
    {
        return call()
                .with(functionName().equalTo(CAST_FUNCTION_NAME))
                .with(argumentCount().equalTo(1))
                .with(argument(0).matching(variable().capturedAs(VALUE)));
    }

    @Override
    public Optional<JdbcExpression> rewrite(ConnectorTableHandle handle, ConnectorExpression castExpression, Captures captures, RewriteContext<ParameterizedExpression> context)
    {
        Variable variable = captures.get(VALUE);
        JdbcTypeHandle sourceTypeJdbcHandle = ((JdbcColumnHandle) context.getAssignment(variable.getName())).getJdbcTypeHandle();
        Type targetType = castExpression.getType();
        Optional<JdbcTypeHandle> targetJdbcTypeHandle = toJdbcTypeHandle(sourceTypeJdbcHandle, targetType);

        if (targetJdbcTypeHandle.isEmpty()) {
            return Optional.empty();
        }

        Optional<ParameterizedExpression> value = context.rewriteExpression(variable);
        if (value.isEmpty()) {
            return Optional.empty();
        }

        Type sourceType = variable.getType();
        String castType = jdbcTypeProvider.apply(context.getSession(), targetType);

        return Optional.of(new JdbcExpression(
                buildCast(sourceType, targetType, value.get().expression(), castType),
                value.get().parameters(),
                targetJdbcTypeHandle.get()));
    }
}
