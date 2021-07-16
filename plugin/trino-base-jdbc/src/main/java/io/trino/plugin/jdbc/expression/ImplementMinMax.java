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
package io.trino.plugin.jdbc.expression;

import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.AggregateFunctionRule;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.CharType;
import io.trino.spi.type.VarcharType;

import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.expression.AggregateFunctionPatterns.basicAggregation;
import static io.trino.plugin.base.expression.AggregateFunctionPatterns.functionName;
import static io.trino.plugin.base.expression.AggregateFunctionPatterns.singleInput;
import static io.trino.plugin.base.expression.AggregateFunctionPatterns.variable;
import static java.lang.String.format;

/**
 * Implements {@code min(x)}, {@code max(x)}.
 */
public class ImplementMinMax
        implements AggregateFunctionRule<JdbcExpression>
{
    private static final Capture<Variable> INPUT = newCapture();

    private final boolean isRemoteCollationSensitive;

    public ImplementMinMax(boolean isRemoteCollationSensitive)
    {
        this.isRemoteCollationSensitive = isRemoteCollationSensitive;
    }

    @Override
    public Pattern<AggregateFunction> getPattern()
    {
        return basicAggregation()
                .with(functionName().matching(Set.of("min", "max")::contains))
                .with(singleInput().matching(variable().capturedAs(INPUT)));
    }

    @Override
    public Optional<JdbcExpression> rewrite(AggregateFunction aggregateFunction, Captures captures, RewriteContext context)
    {
        Variable input = captures.get(INPUT);
        JdbcColumnHandle columnHandle = (JdbcColumnHandle) context.getAssignment(input.getName());
        verify(columnHandle.getColumnType().equals(aggregateFunction.getOutputType()));

        // Remote database is case insensitive or sorts values differently from Trino
        if (!isRemoteCollationSensitive && (columnHandle.getColumnType() instanceof CharType || columnHandle.getColumnType() instanceof VarcharType)) {
            return Optional.empty();
        }

        return Optional.of(new JdbcExpression(
                format("%s(%s)", aggregateFunction.getFunctionName(), context.getIdentifierQuote().apply(columnHandle.getColumnName())),
                columnHandle.getJdbcTypeHandle()));
    }
}
