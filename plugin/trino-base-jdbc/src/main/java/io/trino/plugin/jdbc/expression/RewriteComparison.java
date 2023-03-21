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
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.FunctionName;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Verify.verify;
import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.expression;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class RewriteComparison
        implements ConnectorExpressionRule<Call, ParameterizedExpression>
{
    private static final Capture<ConnectorExpression> LEFT = newCapture();
    private static final Capture<ConnectorExpression> RIGHT = newCapture();

    public enum ComparisonOperator
    {
        EQUAL(EQUAL_OPERATOR_FUNCTION_NAME, "="),
        NOT_EQUAL(NOT_EQUAL_OPERATOR_FUNCTION_NAME, "<>"),
        LESS_THAN(LESS_THAN_OPERATOR_FUNCTION_NAME, "<"),
        LESS_THAN_OR_EQUAL(LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, "<="),
        GREATER_THAN(GREATER_THAN_OPERATOR_FUNCTION_NAME, ">"),
        GREATER_THAN_OR_EQUAL(GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME, ">="),
        IS_DISTINCT_FROM(IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME, "IS DISTINCT FROM"),
        /**/;

        private final FunctionName functionName;
        private final String operator;

        private static final Map<FunctionName, ComparisonOperator> OPERATOR_BY_FUNCTION_NAME = Stream.of(values())
                .collect(toImmutableMap(ComparisonOperator::getFunctionName, identity()));

        ComparisonOperator(FunctionName functionName, String operator)
        {
            this.functionName = requireNonNull(functionName, "functionName is null");
            this.operator = requireNonNull(operator, "operator is null");
        }

        private FunctionName getFunctionName()
        {
            return functionName;
        }

        private String getOperator()
        {
            return operator;
        }

        private static ComparisonOperator forFunctionName(FunctionName functionName)
        {
            return verifyNotNull(OPERATOR_BY_FUNCTION_NAME.get(functionName), "Function name not recognized: %s", functionName);
        }
    }

    private final Pattern<Call> pattern;

    public RewriteComparison(Set<ComparisonOperator> enabledOperators)
    {
        Set<FunctionName> functionNames = enabledOperators.stream()
                .map(ComparisonOperator::getFunctionName)
                .collect(toImmutableSet());

        pattern = call()
                .with(functionName().matching(functionNames::contains))
                .with(type().equalTo(BOOLEAN))
                .with(argumentCount().equalTo(2))
                .with(argument(0).matching(expression().capturedAs(LEFT)))
                .with(argument(1).matching(expression().capturedAs(RIGHT)));
    }

    @Override
    public Pattern<Call> getPattern()
    {
        return pattern;
    }

    @Override
    public Optional<ParameterizedExpression> rewrite(Call call, Captures captures, RewriteContext<ParameterizedExpression> context)
    {
        Optional<ParameterizedExpression> left = context.defaultRewrite(captures.get(LEFT));
        if (left.isEmpty()) {
            return Optional.empty();
        }
        Optional<ParameterizedExpression> right = context.defaultRewrite(captures.get(RIGHT));
        if (right.isEmpty()) {
            return Optional.empty();
        }
        verify(call.getFunctionName().getCatalogSchema().isEmpty()); // filtered out by the pattern
        ComparisonOperator operator = ComparisonOperator.forFunctionName(call.getFunctionName());
        return Optional.of(new ParameterizedExpression(
                format("(%s) %s (%s)", left.get().expression(), operator.getOperator(), right.get().expression()),
                ImmutableList.<QueryParameter>builder()
                        .addAll(left.get().parameters())
                        .addAll(right.get().parameters())
                        .build()));
    }
}
