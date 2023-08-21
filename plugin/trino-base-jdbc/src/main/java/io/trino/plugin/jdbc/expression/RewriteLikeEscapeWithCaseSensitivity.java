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
import io.trino.plugin.jdbc.CaseSensitivity;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.VarcharType;

import java.util.Optional;

import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.expression;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static io.trino.plugin.jdbc.CaseSensitivity.CASE_INSENSITIVE;
import static io.trino.spi.expression.StandardFunctions.LIKE_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.lang.String.format;

public class RewriteLikeEscapeWithCaseSensitivity
        implements ConnectorExpressionRule<Call, ParameterizedExpression>
{
    private static final Capture<ConnectorExpression> LIKE_VALUE = newCapture();
    private static final Capture<ConnectorExpression> LIKE_PATTERN = newCapture();
    private static final Capture<ConnectorExpression> ESCAPE_PATTERN = newCapture();
    private static final Pattern<Call> PATTERN = call()
            .with(functionName().equalTo(LIKE_FUNCTION_NAME))
            .with(type().equalTo(BOOLEAN))
            .with(argumentCount().equalTo(3))
            .with(argument(0).matching(expression().capturedAs(LIKE_VALUE).with(type().matching(VarcharType.class::isInstance))))
            .with(argument(1).matching(expression().capturedAs(LIKE_PATTERN).with(type().matching(VarcharType.class::isInstance))))
            .with(argument(2).matching(expression().capturedAs(ESCAPE_PATTERN).with(type().matching(VarcharType.class::isInstance))));

    @Override
    public Pattern<Call> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<ParameterizedExpression> rewrite(Call expression, Captures captures, RewriteContext<ParameterizedExpression> context)
    {
        ConnectorExpression capturedValue = captures.get(LIKE_VALUE);
        if (capturedValue instanceof Variable variable) {
            JdbcColumnHandle columnHandle = (JdbcColumnHandle) context.getAssignment(variable.getName());
            Optional<CaseSensitivity> caseSensitivity = columnHandle.getJdbcTypeHandle().getCaseSensitivity();
            if (caseSensitivity.orElse(CASE_INSENSITIVE) == CASE_INSENSITIVE) {
                return Optional.empty();
            }
        }
        Optional<ParameterizedExpression> value = context.defaultRewrite(capturedValue);
        if (value.isEmpty()) {
            return Optional.empty();
        }

        ImmutableList.Builder<QueryParameter> parameters = ImmutableList.builder();
        parameters.addAll(value.get().parameters());
        Optional<ParameterizedExpression> pattern = context.defaultRewrite(captures.get(LIKE_PATTERN));
        if (pattern.isEmpty()) {
            return Optional.empty();
        }
        parameters.addAll(pattern.get().parameters());

        Optional<ParameterizedExpression> escape = context.defaultRewrite(captures.get(ESCAPE_PATTERN));
        if (escape.isEmpty()) {
            return Optional.empty();
        }
        parameters.addAll(escape.get().parameters());
        return Optional.of(new ParameterizedExpression(format("%s LIKE %s ESCAPE %s", value.get().expression(), pattern.get().expression(), escape.get().expression()), parameters.build()));
    }
}
