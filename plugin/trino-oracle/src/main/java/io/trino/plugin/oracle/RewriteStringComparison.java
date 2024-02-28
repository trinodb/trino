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
package io.trino.plugin.oracle;

import com.google.common.collect.ImmutableList;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.expression.ComparisonOperator;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.CharType;
import io.trino.spi.type.VarcharType;
import oracle.jdbc.OracleTypes;

import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.variable;
import static io.trino.spi.type.BooleanType.BOOLEAN;

public class RewriteStringComparison
        implements ConnectorExpressionRule<Call, ParameterizedExpression>
{
    private static final Capture<Variable> FIRST_ARGUMENT = newCapture();
    private static final Capture<Variable> SECOND_ARGUMENT = newCapture();
    private static final Pattern<Call> PATTERN = call()
            .with(type().equalTo(BOOLEAN))
            .with(functionName().matching(Stream.of(ComparisonOperator.values())
                    .filter(comparison -> comparison != ComparisonOperator.IS_DISTINCT_FROM)
                    .map(ComparisonOperator::getFunctionName)
                    .collect(toImmutableSet())
                    ::contains))
            .with(argumentCount().equalTo(2))
            .with(argument(0).matching(variable().with(type().matching(type -> type instanceof CharType || type instanceof VarcharType)).capturedAs(FIRST_ARGUMENT)))
            .with(argument(1).matching(variable().with(type().matching(type -> type instanceof CharType || type instanceof VarcharType)).capturedAs(SECOND_ARGUMENT)));

    @Override
    public Pattern<Call> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<ParameterizedExpression> rewrite(Call expression, Captures captures, RewriteContext<ParameterizedExpression> context)
    {
        ComparisonOperator comparison = ComparisonOperator.forFunctionName(expression.getFunctionName());
        Variable firstArgument = captures.get(FIRST_ARGUMENT);
        Variable secondArgument = captures.get(SECOND_ARGUMENT);

        if (isClob(firstArgument, context) || isClob(secondArgument, context)) {
            return Optional.empty();
        }
        return context.defaultRewrite(firstArgument).flatMap(first ->
                context.defaultRewrite(secondArgument).map(second ->
                        new ParameterizedExpression(
                                "(%s) %s (%s)".formatted(first.expression(), comparison.getOperator(), second.expression()),
                                ImmutableList.<QueryParameter>builder()
                                        .addAll(first.parameters())
                                        .addAll(second.parameters())
                                        .build())));
    }

    private static boolean isClob(Variable variable, RewriteContext<?> context)
    {
        return switch (((JdbcColumnHandle) context.getAssignment(variable.getName())).getJdbcTypeHandle().getJdbcType()) {
            case OracleTypes.CLOB, OracleTypes.NCLOB -> true;
            default -> false;
        };
    }
}
