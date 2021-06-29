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
package io.trino.plugin.clickhouse;

import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.expression.AggregateFunctionRule;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.DecimalType;

import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.clickhouse.ClickHouseClient.CLICKHOUSE_MAX_DECIMAL_PRECISION;
import static io.trino.plugin.jdbc.expression.AggregateFunctionPatterns.basicAggregation;
import static io.trino.plugin.jdbc.expression.AggregateFunctionPatterns.expressionType;
import static io.trino.plugin.jdbc.expression.AggregateFunctionPatterns.functionName;
import static io.trino.plugin.jdbc.expression.AggregateFunctionPatterns.singleInput;
import static io.trino.plugin.jdbc.expression.AggregateFunctionPatterns.variable;
import static java.lang.String.format;

/**
 * Implements {@code avg(decimal(p, s)}
 */
public class ImplementAvgDecimal
        implements AggregateFunctionRule
{
    private static final Capture<Variable> INPUT = newCapture();

    @Override
    public Pattern<AggregateFunction> getPattern()
    {
        return basicAggregation()
                .with(functionName().equalTo("avg"))
                .with(singleInput().matching(
                        variable()
                                .with(expressionType().matching(DecimalType.class::isInstance))
                                .capturedAs(INPUT)));
    }

    @Override
    public Optional<JdbcExpression> rewrite(AggregateFunction aggregateFunction, Captures captures, RewriteContext context)
    {
        Variable input = captures.get(INPUT);
        JdbcColumnHandle columnHandle = (JdbcColumnHandle) context.getAssignment(input.getName());
        DecimalType type = (DecimalType) columnHandle.getColumnType();
        verify(aggregateFunction.getOutputType().equals(type));

        // When decimal type has maximum precision we can get result that does not match Trino avg semantics.
        if (type.getPrecision() == CLICKHOUSE_MAX_DECIMAL_PRECISION) {
            return Optional.of(new JdbcExpression(
                    format("avg(CAST(%s AS decimal(%s, %s)))", context.getIdentifierQuote().apply(columnHandle.getColumnName()), type.getPrecision(), type.getScale()),
                    columnHandle.getJdbcTypeHandle()));
        }

        // ClickHouse avg function rounds down resulting decimal.
        // To match Trino avg semantics, we extend scale by 1 and round result to target scale.
        return Optional.of(new JdbcExpression(
                format("round(avg(CAST(%s AS decimal(%s, %s))), %s)", context.getIdentifierQuote().apply(columnHandle.getColumnName()), type.getPrecision() + 1, type.getScale() + 1, type.getScale()),
                columnHandle.getJdbcTypeHandle()));
    }
}
