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

import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.DecimalType;

import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.basicAggregation;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.expressionType;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.functionName;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.singleArgument;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.variable;
import static java.lang.String.format;

/**
 * Implements {@code avg(decimal(p, s)}
 */
public class ImplementAvgDecimal
        implements AggregateFunctionRule<JdbcExpression, ParameterizedExpression>
{
    private static final Capture<Variable> ARGUMENT = newCapture();

    @Override
    public Pattern<AggregateFunction> getPattern()
    {
        return basicAggregation()
                .with(functionName().equalTo("avg"))
                .with(singleArgument().matching(
                        variable()
                                .with(expressionType().matching(DecimalType.class::isInstance))
                                .capturedAs(ARGUMENT)));
    }

    @Override
    public Optional<JdbcExpression> rewrite(AggregateFunction aggregateFunction, Captures captures, RewriteContext<ParameterizedExpression> context)
    {
        Variable argument = captures.get(ARGUMENT);
        JdbcColumnHandle columnHandle = (JdbcColumnHandle) context.getAssignment(argument.getName());
        DecimalType type = (DecimalType) columnHandle.getColumnType();
        verify(aggregateFunction.getOutputType().equals(type));

        ParameterizedExpression rewrittenArgument = context.rewriteExpression(argument).orElseThrow();
        return Optional.of(new JdbcExpression(
                format("CAST(avg(%s) AS decimal(%s, %s))", rewrittenArgument.expression(), type.getPrecision(), type.getScale()),
                rewrittenArgument.parameters(),
                columnHandle.getJdbcTypeHandle()));
    }
}
