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
package io.trino.operator.aggregation;

import io.trino.operator.aggregation.state.LongState;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.Decomposition;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import static io.trino.spi.type.BigintType.BIGINT;

/**
 * Sum aggregation that returns zero if there is no input.
 * This is used for the aggregating the partial output from the count function which also must return zero if there is no input.
 */
@AggregationFunction(value = "$sum0", hidden = true)
public final class Sum0Aggregation
{
    private Sum0Aggregation() {}

    @InputFunction
    public static void partialInput(@AggregationState LongState state, @SqlType(StandardTypes.BIGINT) long value)
    {
        state.setValue(state.getValue() + value);
    }

    @OutputFunction(value = StandardTypes.BIGINT, decomposition = @Decomposition(partial = "$sum0", output = "$sum0"))
    public static void intermediateOutput(@AggregationState LongState state, BlockBuilder out)
    {
        BIGINT.writeLong(out, state.getValue());
    }
}
