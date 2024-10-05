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
package io.trino.plugin.clickhouse.expression;

import com.clickhouse.data.ClickHouseDataType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.expression.ComparisonOperator;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.expression;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static io.trino.plugin.clickhouse.ClickHouseClient.supportsPushdown;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.lang.String.format;

public class RewriteComparison
        implements ConnectorExpressionRule<Call, ParameterizedExpression>
{
    private static final Capture<ConnectorExpression> LEFT = newCapture();
    private static final Capture<ConnectorExpression> RIGHT = newCapture();

    private final Pattern<Call> pattern;
    private final ImmutableSet<ClickHouseDataType> nativeTypes;

    public RewriteComparison(List<Class<?>> classes, ImmutableSet<ClickHouseDataType> nativeTypes)
    {
        pattern = call()
                .with(type().equalTo(BOOLEAN))
                .with(functionName().matching(Stream.of(ComparisonOperator.values())
                        .filter(comparison -> comparison != ComparisonOperator.IDENTICAL)
                        .map(ComparisonOperator::getFunctionName)
                        .collect(toImmutableSet())
                        ::contains))
                .with(argumentCount().equalTo(2))
                .with(argument(0).matching(expression().with(type().matching(type -> classes.stream().anyMatch(aClass -> aClass.isInstance(type)))).capturedAs(LEFT)))
                .with(argument(1).matching(expression().with(type().matching(type -> classes.stream().anyMatch(aClass -> aClass.isInstance(type)))).capturedAs(RIGHT)));
        this.nativeTypes = nativeTypes;
    }

    @Override
    public Pattern<Call> getPattern()
    {
        return pattern;
    }

    @Override
    public Optional<ParameterizedExpression> rewrite(Call expression, Captures captures, RewriteContext<ParameterizedExpression> context)
    {
        ComparisonOperator comparison = ComparisonOperator.forFunctionName(expression.getFunctionName());
        ConnectorExpression leftExpression = captures.get(LEFT);
        ConnectorExpression rightExpression = captures.get(RIGHT);

        if (leftExpression instanceof Variable variable && !supportsPushdown(variable, context, nativeTypes)) {
            return Optional.empty();
        }

        if (rightExpression instanceof Variable variable && !supportsPushdown(variable, context, nativeTypes)) {
            return Optional.empty();
        }

        Optional<ParameterizedExpression> left = context.defaultRewrite(leftExpression);
        if (left.isEmpty()) {
            return Optional.empty();
        }

        Optional<ParameterizedExpression> right = context.defaultRewrite(rightExpression);
        if (right.isEmpty()) {
            return Optional.empty();
        }

        verify(expression.getFunctionName().getCatalogSchema().isEmpty()); // filtered out by the pattern
        return Optional.of(new ParameterizedExpression(
                format("(%s) %s (%s)", left.get().expression(), comparison.getOperator(), right.get().expression()),
                ImmutableList.<QueryParameter>builder()
                        .addAll(left.get().parameters())
                        .addAll(right.get().parameters())
                        .build()));
    }
}
