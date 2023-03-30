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
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.expression.Variable;

import java.sql.Types;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.basicAggregation;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.expressionType;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.functionName;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.singleArgument;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.variable;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static java.lang.String.format;

/**
 * Implements {@code avg(x)} for bigint columns while preserving Trino semantics.
 * Trino semantics say the output should be a double but pushing down the aggregation to some databases
 * can result in rounding of the output to a bigint.
 */
public abstract class BaseImplementAvgBigint
        implements AggregateFunctionRule<JdbcExpression, ParameterizedExpression>
{
    private final Capture<Variable> argument;

    public BaseImplementAvgBigint()
    {
        this.argument = newCapture();
    }

    @Override
    public Pattern<AggregateFunction> getPattern()
    {
        return basicAggregation()
                .with(functionName().equalTo("avg"))
                .with(singleArgument().matching(
                        variable()
                                .with(expressionType().matching(type -> type == BIGINT))
                                .capturedAs(this.argument)));
    }

    @Override
    public Optional<JdbcExpression> rewrite(AggregateFunction aggregateFunction, Captures captures, RewriteContext<ParameterizedExpression> context)
    {
        Variable argument = captures.get(this.argument);
        verify(aggregateFunction.getOutputType() == DOUBLE);

        ParameterizedExpression rewrittenArgument = context.rewriteExpression(argument).orElseThrow();
        return Optional.of(new JdbcExpression(
                format(getRewriteFormatExpression(), rewrittenArgument.expression()),
                rewrittenArgument.parameters(),
                new JdbcTypeHandle(Types.DOUBLE, Optional.of("double"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty())));
    }

    // TODO String.format is not great for contract of an extensible API. Replace with formatting method.
    /**
     * Implement this method for each connector supporting avg(bigint) pushdown
     *
     * @return A format string expression with a single placeholder for the column name; The string expression pushes down avg to the remote database
     */
    protected abstract String getRewriteFormatExpression();
}
