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
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.spi.TrinoException;
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
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.expression.StandardFunctions.CAST_FUNCTION_NAME;
import static java.util.Objects.requireNonNull;

public class RewriteCast
        implements ConnectorExpressionRule<Call, ParameterizedExpression>
{
    private static final Capture<ConnectorExpression> VALUE = newCapture();
    private final BiFunction<ConnectorSession, Type, WriteMapping> toWriteMapping;

    public RewriteCast(BiFunction<ConnectorSession, Type, WriteMapping> toWriteMapping)
    {
        this.toWriteMapping = requireNonNull(toWriteMapping, "toWriteMapping is null");
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
    public Optional<ParameterizedExpression> rewrite(Call call, Captures captures, RewriteContext<ParameterizedExpression> context)
    {
        Type trinoType = call.getType();
        ConnectorExpression capturedValue = captures.get(VALUE);

        Optional<ParameterizedExpression> value = context.defaultRewrite(capturedValue);
        if (value.isEmpty()) {
            // if argument is a call chain that can't be rewritten, then we can't push it down
            return Optional.empty();
        }

        WriteMapping writeMapping;
        try {
            writeMapping = toWriteMapping.apply(context.getSession(), trinoType);
        }
        catch (TrinoException e) {
            if (NOT_SUPPORTED.toErrorCode().equals(e.getErrorCode())) {
                return Optional.empty();
            }
            throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
        }

        String targetType = writeMapping.getDataType();
        return Optional.of(new ParameterizedExpression(
                "CAST(%s AS %s)".formatted(value.get().expression(), targetType),
                value.get().parameters()));
    }
}
