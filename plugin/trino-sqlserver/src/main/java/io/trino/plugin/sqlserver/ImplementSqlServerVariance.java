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
package io.trino.plugin.sqlserver;

import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.DoubleType;

import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.basicAggregation;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.expressionType;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.functionName;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.singleInput;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.variable;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static java.lang.String.format;

public class ImplementSqlServerVariance
        implements AggregateFunctionRule<JdbcExpression>
{
    private static final Capture<Variable> INPUT = newCapture();

    @Override
    public Pattern<AggregateFunction> getPattern()
    {
        return basicAggregation()
                .with(functionName().equalTo("variance"))
                .with(singleInput().matching(
                        variable()
                                .with(expressionType().matching(DoubleType.class::isInstance))
                                .capturedAs(INPUT)));
    }

    @Override
    public Optional<JdbcExpression> rewrite(AggregateFunction aggregateFunction, Captures captures, RewriteContext context)
    {
        Variable input = captures.get(INPUT);
        JdbcColumnHandle columnHandle = (JdbcColumnHandle) context.getAssignment(input.getName());
        verify(columnHandle.getColumnType().equals(DOUBLE));
        verify(aggregateFunction.getOutputType().equals(DOUBLE));

        return Optional.of(new JdbcExpression(
                format("VAR(%s)", context.getIdentifierQuote().apply(columnHandle.getColumnName())),
                columnHandle.getJdbcTypeHandle()));
    }
}
