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
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.expression.AggregateFunctionRule;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.expression.Variable;

import java.sql.Types;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.jdbc.expression.AggregateFunctionPatterns.basicAggregation;
import static io.trino.plugin.jdbc.expression.AggregateFunctionPatterns.expressionType;
import static io.trino.plugin.jdbc.expression.AggregateFunctionPatterns.functionName;
import static io.trino.plugin.jdbc.expression.AggregateFunctionPatterns.singleInput;
import static io.trino.plugin.jdbc.expression.AggregateFunctionPatterns.variable;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static java.lang.String.format;

public class ImplementAvgBigint
        implements AggregateFunctionRule
{
    private static final Capture<Variable> INPUT = newCapture();

    @Override
    public Pattern<AggregateFunction> getPattern()
    {
        return basicAggregation()
                .with(functionName().equalTo("avg"))
                .with(singleInput().matching(variable().with(expressionType().equalTo(BIGINT)).capturedAs(INPUT)));
    }

    @Override
    public Optional<JdbcExpression> rewrite(AggregateFunction aggregateFunction, Captures captures, RewriteContext context)
    {
        Variable input = captures.get(INPUT);
        JdbcColumnHandle columnHandle = (JdbcColumnHandle) context.getAssignment(input.getName());
        verify(aggregateFunction.getOutputType() == DOUBLE);

        return Optional.of(new JdbcExpression(
                format("avg((%s * 1.0))", context.getIdentifierQuote().apply(columnHandle.getColumnName())),
                new JdbcTypeHandle(Types.DOUBLE, Optional.of("double"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty())));
    }
}
