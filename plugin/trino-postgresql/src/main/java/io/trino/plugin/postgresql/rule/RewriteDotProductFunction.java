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
package io.trino.plugin.postgresql.rule;

import com.google.common.collect.ImmutableList;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.projection.ProjectFunctionRule;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.FunctionName;

import java.sql.Types;
import java.util.Optional;

import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.expression;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static io.trino.plugin.postgresql.rule.RewriteVectorDistanceFunction.isArrayTypeWithRealOrDouble;
import static io.trino.spi.type.DoubleType.DOUBLE;

public final class RewriteDotProductFunction
        implements ProjectFunctionRule<JdbcExpression, ParameterizedExpression>
{
    private static final Capture<ConnectorExpression> CALL = newCapture();

    private static final Pattern<Call> PATTERN = call()
            .with(functionName().equalTo(new FunctionName("$negate")))
            .with(type().matching(type -> type == DOUBLE))
            .with(argumentCount().equalTo(1))
            .with(argument(0).matching(expression().capturedAs(CALL).matching(expression -> expression instanceof Call call
                    && call.getFunctionName().equals(new FunctionName("dot_product"))
                    && call.getArguments().size() == 2
                    && call.getArguments().stream().allMatch(argument -> isArrayTypeWithRealOrDouble(argument.getType())))));

    @Override
    public Pattern<? extends ConnectorExpression> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<JdbcExpression> rewrite(ConnectorTableHandle handle, ConnectorExpression projectionExpression, Captures captures, RewriteContext<ParameterizedExpression> context)
    {
        ConnectorExpression call = captures.get(CALL);

        Optional<ParameterizedExpression> leftExpression = RewriteVectorDistanceFunction.rewrite(call.getChildren().getFirst(), context);
        if (leftExpression.isEmpty()) {
            return Optional.empty();
        }

        Optional<ParameterizedExpression> rightExpression = RewriteVectorDistanceFunction.rewrite(call.getChildren().get(1), context);
        if (rightExpression.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(new JdbcExpression(
                "%s <#> %s".formatted(leftExpression.get().expression(), rightExpression.get().expression()),
                ImmutableList.<QueryParameter>builder()
                        .addAll(leftExpression.get().parameters())
                        .addAll(rightExpression.get().parameters())
                        .build(),
                new JdbcTypeHandle(
                        Types.DOUBLE,
                        Optional.of("double"),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty())));
    }
}
