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

import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.pinot.query.AggregateExpression;
import io.trino.spi.connector.AggregateFunction;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.basicAggregation;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.functionName;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.inputs;
import static io.trino.plugin.base.aggregation.AggregateFunctionPatterns.outputType;
import static io.trino.spi.type.BigintType.BIGINT;

/**
 * Implements {@code count(*)}.
 */
public class ImplementCountAll
        implements AggregateFunctionRule<AggregateExpression>
{
    @Override
    public Pattern<AggregateFunction> getPattern()
    {
        return basicAggregation()
                .with(functionName().equalTo("count"))
                .with(inputs().equalTo(List.of()))
                .with(outputType().equalTo(BIGINT));
    }

    @Override
    public Optional<AggregateExpression> rewrite(AggregateFunction aggregateFunction, Captures captures, RewriteContext context)
    {
        return Optional.of(new AggregateExpression("count", "*", false));
    }
}
