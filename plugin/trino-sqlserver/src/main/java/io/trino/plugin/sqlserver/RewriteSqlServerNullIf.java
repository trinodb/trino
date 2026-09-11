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
package io.trino.plugin.sqlserver;

import com.google.common.collect.ImmutableList;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;

import java.util.Optional;

import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.expression;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.plugin.sqlserver.RemoteComparison.isSupportedComparison;
import static io.trino.spi.expression.StandardFunctions.NULLIF_FUNCTION_NAME;

/**
 * Pushes {@code NULLIF} down only when {@link RemoteComparison} allows comparing the values remotely.
 */
public class RewriteSqlServerNullIf
        implements ConnectorExpressionRule<Call, ParameterizedExpression>
{
    private static final Capture<ConnectorExpression> FIRST = newCapture();
    private static final Capture<ConnectorExpression> SECOND = newCapture();

    private static final Pattern<Call> PATTERN = call()
            .with(functionName().equalTo(NULLIF_FUNCTION_NAME))
            .with(argumentCount().equalTo(2))
            .with(argument(0).matching(expression().capturedAs(FIRST)))
            .with(argument(1).matching(expression().capturedAs(SECOND)));

    @Override
    public Pattern<Call> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<ParameterizedExpression> rewrite(Call call, Captures captures, RewriteContext<ParameterizedExpression> context)
    {
        if (!isSupportedComparison(call, context)) {
            return Optional.empty();
        }
        return context.defaultRewrite(captures.get(FIRST)).flatMap(first ->
                context.defaultRewrite(captures.get(SECOND)).map(second ->
                        new ParameterizedExpression(
                                "NULLIF((%s), (%s))".formatted(first.expression(), second.expression()),
                                ImmutableList.<QueryParameter>builder()
                                        .addAll(first.parameters())
                                        .addAll(second.parameters())
                                        .build())));
    }
}
