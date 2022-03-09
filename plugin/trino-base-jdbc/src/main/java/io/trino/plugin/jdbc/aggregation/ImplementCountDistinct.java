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
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.VarcharType;

import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.distinct;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.functionName;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.hasFilter;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.singleArgument;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.variable;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Implements {@code count(DISTINCT x)}.
 */
public class ImplementCountDistinct
        implements AggregateFunctionRule<JdbcExpression>
{
    private static final Capture<Variable> ARGUMENT = newCapture();

    private final JdbcTypeHandle bigintTypeHandle;
    private final boolean isRemoteCollationSensitive;

    /**
     * @param bigintTypeHandle A {@link JdbcTypeHandle} that will be mapped to {@link BigintType} by {@link JdbcClient#toColumnMapping}.
     */
    public ImplementCountDistinct(JdbcTypeHandle bigintTypeHandle, boolean isRemoteCollationSensitive)
    {
        this.bigintTypeHandle = requireNonNull(bigintTypeHandle, "bigintTypeHandle is null");
        this.isRemoteCollationSensitive = isRemoteCollationSensitive;
    }

    @Override
    public Pattern<AggregateFunction> getPattern()
    {
        return Pattern.typeOf(AggregateFunction.class)
                .with(distinct().equalTo(true))
                .with(hasFilter().equalTo(false))
                .with(functionName().equalTo("count"))
                .with(singleArgument().matching(variable().capturedAs(ARGUMENT)));
    }

    @Override
    public Optional<JdbcExpression> rewrite(AggregateFunction aggregateFunction, Captures captures, RewriteContext context)
    {
        Variable argument = captures.get(ARGUMENT);
        JdbcColumnHandle columnHandle = (JdbcColumnHandle) context.getAssignment(argument.getName());
        verify(aggregateFunction.getOutputType() == BIGINT);

        boolean isCaseSensitiveType = columnHandle.getColumnType() instanceof CharType || columnHandle.getColumnType() instanceof VarcharType;
        if (aggregateFunction.isDistinct() && !isRemoteCollationSensitive && isCaseSensitiveType) {
            // Remote database is case insensitive or compares values differently from Trino
            return Optional.empty();
        }

        return Optional.of(new JdbcExpression(
                format("count(DISTINCT %s)", context.getIdentifierQuote().apply(columnHandle.getColumnName())),
                bigintTypeHandle));
    }
}
