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
package io.trino.plugin.pinot.query.aggregation;

import com.google.common.collect.ImmutableSet;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.pinot.PinotColumnHandle;
import io.trino.plugin.pinot.query.AggregateExpression;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.Type;

import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.basicAggregation;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.functionName;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.singleArgument;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.variable;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static java.util.Objects.requireNonNull;

public class ImplementAvg
        implements AggregateFunctionRule<AggregateExpression, Void>
{
    private static final Capture<Variable> ARGUMENT = newCapture();
    private static final Set<Type> SUPPORTED_ARGUMENT_TYPES = ImmutableSet.of(INTEGER, BIGINT, REAL, DOUBLE);

    private final Function<String, String> identifierQuote;

    public ImplementAvg(Function<String, String> identifierQuote)
    {
        this.identifierQuote = requireNonNull(identifierQuote, "identifierQuote is null");
    }

    @Override
    public Pattern<AggregateFunction> getPattern()
    {
        return basicAggregation()
                .with(functionName().equalTo("avg"))
                .with(singleArgument().matching(
                        variable()
                                .with(type().matching(SUPPORTED_ARGUMENT_TYPES::contains))
                                .capturedAs(ARGUMENT)));
    }

    @Override
    public Optional<AggregateExpression> rewrite(AggregateFunction aggregateFunction, Captures captures, RewriteContext<Void> context)
    {
        Variable argument = captures.get(ARGUMENT);
        PinotColumnHandle columnHandle = (PinotColumnHandle) context.getAssignment(argument.getName());
        return Optional.of(new AggregateExpression(aggregateFunction.getFunctionName(), identifierQuote.apply(columnHandle.getColumnName()), true));
    }
}
