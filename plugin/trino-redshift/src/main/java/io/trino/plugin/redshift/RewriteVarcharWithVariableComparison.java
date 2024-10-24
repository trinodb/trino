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
package io.trino.plugin.redshift;

import com.google.common.collect.ImmutableList;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.expression.ComparisonOperator;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.VarcharType;

import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.variable;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.lang.String.format;

public class RewriteVarcharWithVariableComparison
        implements ConnectorExpressionRule<Call, ParameterizedExpression>
{
    private static final Capture<Variable> LEFT = newCapture();
    private static final Capture<Variable> RIGHT = newCapture();

    private final Pattern<Call> pattern;

    public RewriteVarcharWithVariableComparison(Set<ComparisonOperator> enabledOperators)
    {
        Set<FunctionName> functionNames = enabledOperators.stream()
                .map(ComparisonOperator::getFunctionName)
                .collect(toImmutableSet());

        pattern = call()
                .with(functionName().matching(functionNames::contains))
                .with(type().equalTo(BOOLEAN))
                .with(argumentCount().equalTo(2))
                .with(argument(0).matching(variable().capturedAs(LEFT).with(type().matching(type -> type instanceof VarcharType))))
                .with(argument(1).matching(variable().capturedAs(RIGHT).with(type().matching(type -> type instanceof VarcharType))));
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
                format("COLLATE(%s, 'case_sensitive') %s COLLATE(%s, 'case_sensitive')", left.get().expression(), operator.getOperator(), right.get().expression()),
                ImmutableList.<QueryParameter>builder()
                        .addAll(left.get().parameters())
                        .addAll(right.get().parameters())
                        .build()));
    }
}
