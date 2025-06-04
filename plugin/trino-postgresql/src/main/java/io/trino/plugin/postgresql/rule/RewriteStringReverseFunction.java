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
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.VarcharType;

import java.sql.JDBCType;
import java.util.Optional;

import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.variable;

public class RewriteStringReverseFunction
        implements ProjectFunctionRule<JdbcExpression, ParameterizedExpression>
{
    private static final Capture<Variable> ARGUMENT = newCapture();

    private static final Pattern<Call> PATTERN = call()
            .with(functionName().equalTo(new FunctionName("reverse")))
            .with(type().matching(type -> type instanceof VarcharType))
            .with(argumentCount().equalTo(1))
            .with(argument(0).matching(variable().capturedAs(ARGUMENT).with(type().matching(type -> type instanceof VarcharType))));

    @Override
    public Pattern<? extends ConnectorExpression> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<JdbcExpression> rewrite(ConnectorTableHandle handle, ConnectorExpression projectionExpression, Captures captures, RewriteContext<ParameterizedExpression> context)
    {
        Variable argument = captures.get(ARGUMENT);
        JdbcTypeHandle typeHandle = ((JdbcColumnHandle) context.getAssignment(argument.getName())).getJdbcTypeHandle();

        // Rewrite the expression only for `VARCHAR` columns and not for columns with special data type
        if (JDBCType.valueOf(typeHandle.jdbcType()) == JDBCType.VARCHAR && typeHandle.jdbcTypeName().map("varchar"::equals).orElse(false)) {
            Optional<ParameterizedExpression> translatedArgument = context.rewriteExpression(argument);
            if (translatedArgument.isEmpty()) {
                return Optional.empty();
            }

            return Optional.of(new JdbcExpression(
                    "REVERSE(%s)".formatted(translatedArgument.get().expression()),
                    ImmutableList.copyOf(translatedArgument.get().parameters()),
                    typeHandle));
        }
        return Optional.empty();
    }
}
