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

import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.pinot.query.AggregateExpression;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.expression.Variable;

import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.basicAggregation;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.functionName;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.outputType;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.singleInput;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.variable;
import static io.trino.plugin.pinot.PinotSessionProperties.isCountDistinctPushdownEnabled;
import static io.trino.spi.type.BigintType.BIGINT;

public class ImplementCountDistinct
        implements AggregateFunctionRule<AggregateExpression>
{
    private static final Capture<Variable> INPUT = newCapture();

    @Override
    public Pattern<AggregateFunction> getPattern()
    {
        return basicAggregation()
                .with(functionName().equalTo("count"))
                .with(outputType().equalTo(BIGINT))
                .with(singleInput().matching(variable().capturedAs(INPUT)));
    }

    @Override
    public Optional<AggregateExpression> rewrite(AggregateFunction aggregateFunction, Captures captures, RewriteContext context)
    {
        if (!isCountDistinctPushdownEnabled(context.getSession())) {
            return Optional.empty();
        }
        Variable input = captures.get(INPUT);
        verify(aggregateFunction.getOutputType() == BIGINT);
        return Optional.of(new AggregateExpression("distinctcount", context.getIdentifierQuote().apply(input.getName()), false));
    }
}
