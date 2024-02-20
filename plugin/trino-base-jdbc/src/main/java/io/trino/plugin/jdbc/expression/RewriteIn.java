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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.arguments;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.expression;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static io.trino.plugin.jdbc.JdbcMetadataSessionProperties.getDomainCompactionThreshold;
import static io.trino.spi.expression.StandardFunctions.ARRAY_CONSTRUCTOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IN_PREDICATE_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.lang.String.format;

public class RewriteIn
        implements ConnectorExpressionRule<Call, ParameterizedExpression>
{
    private static final Capture<ConnectorExpression> VALUE = newCapture();
    private static final Capture<List<ConnectorExpression>> EXPRESSIONS = newCapture();

    private static final Pattern<Call> PATTERN = call()
            .with(functionName().equalTo(IN_PREDICATE_FUNCTION_NAME))
            .with(type().equalTo(BOOLEAN))
            .with(argumentCount().equalTo(2))
            .with(argument(0).matching(expression().capturedAs(VALUE)))
            .with(argument(1).matching(call().with(functionName().equalTo(ARRAY_CONSTRUCTOR_FUNCTION_NAME)).with(arguments().capturedAs(EXPRESSIONS))));

    @Override
    public Pattern<Call> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<ParameterizedExpression> rewrite(Call call, Captures captures, RewriteContext<ParameterizedExpression> context)
    {
        Optional<ParameterizedExpression> value = context.defaultRewrite(captures.get(VALUE));
        if (value.isEmpty()) {
            return Optional.empty();
        }

        List<ConnectorExpression> expressions = captures.get(EXPRESSIONS);
        if (expressions.size() > getDomainCompactionThreshold(context.getSession())) {
            // We don't want to push down too long IN query text
            return Optional.empty();
        }

        ImmutableList.Builder<QueryParameter> parameters = ImmutableList.builder();
        parameters.addAll(value.get().parameters());
        ImmutableList.Builder<String> rewrittenValues = ImmutableList.builderWithExpectedSize(expressions.size());
        for (ConnectorExpression expression : expressions) {
            Optional<ParameterizedExpression> rewritten = context.defaultRewrite(expression);
            if (rewritten.isEmpty()) {
                return Optional.empty();
            }
            rewrittenValues.add(rewritten.get().expression());
            parameters.addAll(rewritten.get().parameters());
        }

        List<String> values = rewrittenValues.build();
        verify(!values.isEmpty(), "Empty values");
        return Optional.of(new ParameterizedExpression(
                format("(%s) IN (%s)", value.get().expression(), Joiner.on(", ").join(values)),
                parameters.build()));
    }
}
