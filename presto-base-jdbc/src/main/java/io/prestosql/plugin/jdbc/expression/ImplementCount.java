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
package io.prestosql.plugin.jdbc.expression;

import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcExpression;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.spi.connector.AggregateFunction;
import io.prestosql.spi.expression.Variable;
import io.prestosql.spi.type.BigintType;

import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static com.google.common.base.Verify.verifyNotNull;
import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.plugin.jdbc.expression.AggregateFunctionPatterns.basicAggregation;
import static io.prestosql.plugin.jdbc.expression.AggregateFunctionPatterns.functionName;
import static io.prestosql.plugin.jdbc.expression.AggregateFunctionPatterns.singleInput;
import static io.prestosql.plugin.jdbc.expression.AggregateFunctionPatterns.variable;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Implements {@code count(x)}.
 */
public class ImplementCount
        implements AggregateFunctionRule
{
    private static final Capture<Variable> INPUT = newCapture();

    private final JdbcTypeHandle bigintTypeHandle;

    /**
     * @param bigintTypeHandle A {@link JdbcTypeHandle} that will be mapped to {@link BigintType} by {@link JdbcClient#toPrestoType}.
     */
    public ImplementCount(JdbcTypeHandle bigintTypeHandle)
    {
        this.bigintTypeHandle = requireNonNull(bigintTypeHandle, "bigintTypeHandle is null");
    }

    @Override
    public Pattern<AggregateFunction> getPattern()
    {
        return basicAggregation()
                .with(functionName().equalTo("count"))
                .with(singleInput().matching(variable().capturedAs(INPUT)));
    }

    @Override
    public Optional<JdbcExpression> rewrite(AggregateFunction aggregateFunction, Captures captures, RewriteContext context)
    {
        Variable input = captures.get(INPUT);
        JdbcColumnHandle columnHandle = (JdbcColumnHandle) context.getAssignments().get(input.getName());
        verifyNotNull(columnHandle, "Unbound variable: %s", input);
        verify(aggregateFunction.getOutputType() == BIGINT);

        return Optional.of(new JdbcExpression(
                format("count(%s)", columnHandle.toSqlExpression(context.getIdentifierQuote())),
                bigintTypeHandle));
    }
}
