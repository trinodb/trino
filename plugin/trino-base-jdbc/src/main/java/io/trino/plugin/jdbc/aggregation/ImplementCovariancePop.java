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
package io.trino.plugin.jdbc.aggregation;

import com.google.common.collect.ImmutableList;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.expression.Variable;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.arguments;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.basicAggregation;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.expressionTypes;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.functionName;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.variables;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static java.lang.String.format;

public class ImplementCovariancePop
        implements AggregateFunctionRule<JdbcExpression, ParameterizedExpression>
{
    private static final Capture<List<Variable>> ARGUMENTS = newCapture();

    @Override
    public Pattern<AggregateFunction> getPattern()
    {
        return basicAggregation()
                .with(functionName().equalTo("covar_pop"))
                .with(arguments().matching(
                        variables()
                                .matching(expressionTypes(REAL, REAL).or(expressionTypes(DOUBLE, DOUBLE)))
                                .capturedAs(ARGUMENTS)));
    }

    @Override
    public Optional<JdbcExpression> rewrite(AggregateFunction aggregateFunction, Captures captures, RewriteContext<ParameterizedExpression> context)
    {
        List<Variable> arguments = captures.get(ARGUMENTS);
        verify(arguments.size() == 2);

        Variable argument1 = arguments.get(0);
        Variable argument2 = arguments.get(1);
        JdbcColumnHandle columnHandle1 = (JdbcColumnHandle) context.getAssignment(argument1.getName());
        verify(aggregateFunction.getOutputType().equals(columnHandle1.getColumnType()));

        ParameterizedExpression rewrittenArgument1 = context.rewriteExpression(argument1).orElseThrow();
        ParameterizedExpression rewrittenArgument2 = context.rewriteExpression(argument2).orElseThrow();
        return Optional.of(new JdbcExpression(
                format("covar_pop(%s, %s)", rewrittenArgument1.expression(), rewrittenArgument2.expression()),
                ImmutableList.<QueryParameter>builder()
                        .addAll(rewrittenArgument1.parameters())
                        .addAll(rewrittenArgument2.parameters())
                        .build(),
                columnHandle1.getJdbcTypeHandle()));
    }
}
