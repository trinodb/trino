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
package io.trino.plugin.pinot.query.expression;

import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.pinot.PinotColumnHandle;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.expression.Variable;

import java.util.Optional;

import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.pinot.query.DynamicTableBuilder.SUPPORTED_INPUT_TYPES;
import static io.trino.plugin.pinot.query.expression.AggregateFunctionPatterns.basicAggregation;
import static io.trino.plugin.pinot.query.expression.AggregateFunctionPatterns.expressionType;
import static io.trino.plugin.pinot.query.expression.AggregateFunctionPatterns.functionName;
import static io.trino.plugin.pinot.query.expression.AggregateFunctionPatterns.singleInput;
import static io.trino.plugin.pinot.query.expression.AggregateFunctionPatterns.variable;
import static java.lang.String.format;

/**
 * Implements {@code sum(x)}
 */
public class ImplementSum
        implements AggregateFunctionRule
{
    // Extracted from io.trino.plugin.jdbc.expression.ImplementSum
    private static final Capture<Variable> INPUT = newCapture();

    @Override
    public Pattern<AggregateFunction> getPattern()
    {
        return basicAggregation()
                .with(functionName().equalTo("sum"))
                .with(singleInput().matching(
                        variable()
                                .with(expressionType().matching(SUPPORTED_INPUT_TYPES::contains))
                                .capturedAs(INPUT)));
    }

    @Override
    public Optional<PinotColumnHandle> rewrite(AggregateFunction aggregateFunction, Captures captures, RewriteContext context)
    {
        Variable input = captures.get(INPUT);
        PinotColumnHandle columnHandle = (PinotColumnHandle) context.getAssignment(input.getName());
        PinotColumnHandle outputColumnHandle = new PinotColumnHandle(format("sum(%s)", context.getIdentifierQuote().apply(columnHandle.getColumnName())), aggregateFunction.getOutputType());
        return Optional.of(outputColumnHandle);
    }
}
