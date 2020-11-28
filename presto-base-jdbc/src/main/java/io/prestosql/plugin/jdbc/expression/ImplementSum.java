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
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcExpression;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.spi.connector.AggregateFunction;
import io.prestosql.spi.expression.Variable;
import io.prestosql.spi.type.DecimalType;

import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Verify.verifyNotNull;
import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.plugin.jdbc.expression.AggregateFunctionPatterns.basicAggregation;
import static io.prestosql.plugin.jdbc.expression.AggregateFunctionPatterns.functionName;
import static io.prestosql.plugin.jdbc.expression.AggregateFunctionPatterns.singleInput;
import static io.prestosql.plugin.jdbc.expression.AggregateFunctionPatterns.variable;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Implements {@code sum(x)}
 */
public class ImplementSum
        implements AggregateFunctionRule
{
    private static final Capture<Variable> INPUT = newCapture();

    private final Function<DecimalType, Optional<JdbcTypeHandle>> decimalTypeHandle;

    public ImplementSum(Function<DecimalType, Optional<JdbcTypeHandle>> decimalTypeHandle)
    {
        this.decimalTypeHandle = requireNonNull(decimalTypeHandle, "decimalTypeHandle is null");
    }

    @Override
    public Pattern<AggregateFunction> getPattern()
    {
        return basicAggregation()
                .with(functionName().equalTo("sum"))
                .with(singleInput().matching(variable().capturedAs(INPUT)));
    }

    @Override
    public Optional<JdbcExpression> rewrite(AggregateFunction aggregateFunction, Captures captures, RewriteContext context)
    {
        Variable input = captures.get(INPUT);
        JdbcColumnHandle columnHandle = (JdbcColumnHandle) context.getAssignments().get(input.getName());
        verifyNotNull(columnHandle, "Unbound variable: %s", input);

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

        return Optional.of(new JdbcExpression(
                format("sum(%s)", columnHandle.toSqlExpression(context.getIdentifierQuote())),
                resultTypeHandle));
    }
}
