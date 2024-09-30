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
import io.trino.plugin.base.projection.ProjectFunctionRule;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.VarcharType;

import java.util.Optional;

import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.argumentCount;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.call;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.constant;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.functionName;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.variable;
import static io.trino.spi.type.IntegerType.INTEGER;

public class ImplementRedshiftLpad
        implements ProjectFunctionRule<JdbcExpression, ParameterizedExpression>
{
    private static final Capture<Variable> ARGUMENT1 = newCapture();
    private static final Capture<Constant> ARGUMENT2 = newCapture();
    private static final Capture<Constant> ARGUMENT3 = newCapture();

    private static final Pattern<Call> PATTERN = call()
            .with(functionName().equalTo(new FunctionName("lpad")))
            .with(type().matching(type -> type instanceof VarcharType))
            .with(argumentCount().equalTo(3))
            .with(argument(0).matching(variable().capturedAs(ARGUMENT1).with(type().matching(type -> type instanceof VarcharType))))
            .with(argument(1).matching(constant().capturedAs(ARGUMENT2).with(type().matching(type -> type instanceof BigintType))))
            .with(argument(2).matching(constant().capturedAs(ARGUMENT3).with(type().matching(type -> type instanceof VarcharType))));

    @Override
    public Pattern<? extends ConnectorExpression> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<JdbcExpression> rewrite(ConnectorTableHandle handle, ConnectorExpression projectionExpression, Captures captures, RewriteContext<ParameterizedExpression> context)
    {
        Variable argument1 = captures.get(ARGUMENT1);
        Constant argument2 = captures.get(ARGUMENT2);
        Constant argument3 = captures.get(ARGUMENT3);

        JdbcTypeHandle typeHandle = ((JdbcColumnHandle) context.getAssignment(argument1.getName())).getJdbcTypeHandle();

        ParameterizedExpression translatedArgument1 = context.rewriteExpression(argument1).orElseThrow();
        ParameterizedExpression translatedArgument2 = context.rewriteExpression(argument2).orElseThrow();
        ParameterizedExpression translatedArgument3 = context.rewriteExpression(argument3).orElseThrow();

        // Pass empty columnSize to match unbounded varchar type created for synthetic column created for lpad expression.
        JdbcTypeHandle updatedTypeHandle = new JdbcTypeHandle(typeHandle.jdbcType(), typeHandle.jdbcTypeName(), Optional.empty(), typeHandle.decimalDigits(), typeHandle.arrayDimensions(), typeHandle.caseSensitivity());

        QueryParameter queryParameter = translatedArgument2.parameters().getFirst();
        // Redshift requires integer type for size parameter. With bigint, driver throws `function lpad(character varying, bigint, "unknown") does not exist`
        QueryParameter updatedQueryParameter = new QueryParameter(INTEGER, queryParameter.getValue());

        return Optional.of(new JdbcExpression(
                "lpad(%s, %s, %s)".formatted(translatedArgument1.expression(), translatedArgument2.expression(), translatedArgument3.expression()),
                ImmutableList.<QueryParameter>builder()
                        .addAll(translatedArgument1.parameters())
                        .add(updatedQueryParameter)
                        .addAll(translatedArgument3.parameters())
                        .build(),
                updatedTypeHandle));
    }
}
