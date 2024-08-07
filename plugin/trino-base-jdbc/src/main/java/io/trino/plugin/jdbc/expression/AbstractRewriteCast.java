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
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.type.Type;

import java.util.Optional;
import java.util.function.BiFunction;

import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.expression;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.spi.expression.StandardFunctions.CAST_FUNCTION_NAME;
import static java.util.Objects.requireNonNull;

public abstract class AbstractRewriteCast
        implements ProjectFunctionRule<JdbcExpression, ParameterizedExpression>
{
    private static final Capture<ConnectorExpression> VALUE = newCapture();

    private final BiFunction<ConnectorSession, Type, String> toTargetType;

    protected abstract boolean eligibleForPushdown(Type sourceType, Type targetType);

    protected abstract Optional<JdbcTypeHandle> toJdbcTypeHandle(Type type);

    protected abstract String buildCast(Type sourceType, Type targetType, String expression, String castType);

    public AbstractRewriteCast(BiFunction<ConnectorSession, Type, String> toTargetType)
    {
        this.toTargetType = requireNonNull(toTargetType, "toTargetType is null");
    }

    @Override
    public Pattern<Call> getPattern()
    {
        return call()
                .with(functionName().equalTo(CAST_FUNCTION_NAME))
                .with(argumentCount().equalTo(1))
                .with(argument(0).matching(expression().capturedAs(VALUE)));
    }

    @Override
    public Optional<JdbcExpression> rewrite(ConnectorExpression projectionExpression, Captures captures, RewriteContext<ParameterizedExpression> context)
    {
        Call call = (Call) projectionExpression;
        Type targetType = call.getType();
        ConnectorExpression capturedValue = captures.get(VALUE);
        Type sourceType = capturedValue.getType();

        Optional<ParameterizedExpression> value = context.rewriteExpression(capturedValue);
        if (value.isEmpty()) {
            // if argument is a call chain that can't be rewritten, then we can't push it down
            return Optional.empty();
        }

        if (!eligibleForPushdown(sourceType, targetType)) {
            return Optional.empty();
        }

        Optional<JdbcTypeHandle> targetTypeHandle = toJdbcTypeHandle(targetType);
        if (targetTypeHandle.isEmpty()) {
            return Optional.empty();
        }

        String castType = toTargetType.apply(context.getSession(), targetType);

        return Optional.of(new JdbcExpression(
                buildCast(sourceType, targetType, value.get().expression(), castType),
                value.get().parameters(),
                targetTypeHandle.get()));
    }
}
