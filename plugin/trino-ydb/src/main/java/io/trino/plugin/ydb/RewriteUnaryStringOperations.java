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
package io.trino.plugin.ydb;

import com.google.common.collect.ImmutableList;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.projection.ProjectFunctionRule;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.type.VarcharType;

import java.util.Objects;
import java.util.Optional;

import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.expression;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;

public class RewriteUnaryStringOperations
        implements ProjectFunctionRule<JdbcExpression, ParameterizedExpression>
{
    private static final Capture<ConnectorExpression> VALUE = newCapture();

    private static final Pattern<Call> PATTERN = call()
            .with(functionName().matching(name -> name.equals(new FunctionName("upper")) ||
                    name.equals(new FunctionName("lower"))))
            .with(type().matching(type -> type instanceof VarcharType))
            .with(argumentCount().equalTo(1))
            .with(argument(0).matching(expression().capturedAs(VALUE)));

    @Override
    public Pattern<? extends ConnectorExpression> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<JdbcExpression> rewrite(ConnectorTableHandle handle, ConnectorExpression projectionExpression, Captures captures, RewriteContext<ParameterizedExpression> context)
    {
        JdbcTypeHandle varcharTypeHandle = YdbTypeUtils.toTypeHandle(VarcharType.VARCHAR).orElse(null);
        if (Objects.isNull(varcharTypeHandle)) {
            return Optional.empty();
        }

        Call call = (Call) projectionExpression;

        ConnectorExpression valueExpr;
        if (call.getArguments().size() == 1) {
            valueExpr = call.getArguments().getFirst();
        }
        else {
            return Optional.empty();
        }

        Optional<ParameterizedExpression> rewrittenValue = context.rewriteExpression(valueExpr);
        if (rewrittenValue.isEmpty()) {
            return Optional.empty();
        }

        String expression = mapToYql(call.getFunctionName().getName()).formatted(rewrittenValue.get().expression());

        return Optional.of(new JdbcExpression(
                expression,
                ImmutableList.copyOf(rewrittenValue.get().parameters()),
                varcharTypeHandle));
    }

    private static String mapToYql(String functionName)
    {
        return switch (functionName) {
            case "upper" -> "Unicode::ToUpper(%s)";
            case "lower" -> "Unicode::ToLower(%s)";
            default -> throw new IllegalArgumentException("Unexpected function name: " + functionName);
        };
    }
}
