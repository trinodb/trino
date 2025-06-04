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
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.DecimalType;

import java.util.Optional;
import java.util.function.Function;

import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.functionName;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.hasFilter;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.hasSortOrder;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.singleArgument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.variable;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Implements {@code sum([DISTINCT] x)}
 */
public class ImplementSum
        implements AggregateFunctionRule<JdbcExpression, ParameterizedExpression>
{
    private static final Capture<Variable> ARGUMENT = newCapture();

    private final Function<DecimalType, Optional<JdbcTypeHandle>> decimalTypeHandle;

    public ImplementSum(Function<DecimalType, Optional<JdbcTypeHandle>> decimalTypeHandle)
    {
        this.decimalTypeHandle = requireNonNull(decimalTypeHandle, "decimalTypeHandle is null");
    }

    @Override
    public Pattern<AggregateFunction> getPattern()
    {
        return Pattern.typeOf(AggregateFunction.class)
                .with(hasSortOrder().equalTo(false))
                .with(hasFilter().equalTo(false))
                .with(functionName().equalTo("sum"))
                .with(singleArgument().matching(variable().capturedAs(ARGUMENT)));
    }

    @Override
    public Optional<JdbcExpression> rewrite(AggregateFunction aggregateFunction, Captures captures, RewriteContext<ParameterizedExpression> context)
    {
        Variable argument = captures.get(ARGUMENT);
        JdbcColumnHandle columnHandle = (JdbcColumnHandle) context.getAssignment(argument.getName());

        JdbcTypeHandle resultTypeHandle;
        if (columnHandle.getColumnType().equals(aggregateFunction.getOutputType())) {
            resultTypeHandle = columnHandle.getJdbcTypeHandle();
        }
        else if (aggregateFunction.getOutputType() instanceof DecimalType) {
            Optional<JdbcTypeHandle> decimalTypeHandle = this.decimalTypeHandle.apply(((DecimalType) aggregateFunction.getOutputType()));
            if (decimalTypeHandle.isEmpty()) {
                return Optional.empty();
            }
            resultTypeHandle = decimalTypeHandle.get();
        }
        else {
            return Optional.empty();
        }

        ParameterizedExpression rewrittenArgument = context.rewriteExpression(argument).orElseThrow();
        String function = aggregateFunction.isDistinct() ? "sum(DISTINCT %s)" : "sum(%s)";
        return Optional.of(new JdbcExpression(
                format(function, rewrittenArgument.expression()),
                rewrittenArgument.parameters(),
                resultTypeHandle));
    }
}
