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

import io.trino.operator.aggregation.state.NullableLongState;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.StandardTypes;

@AggregationFunction("bitwise_and_agg")
public final class BitwiseAndAggregation
{
    private BitwiseAndAggregation() {}

    @InputFunction
    public static void bitAnd(@AggregationState NullableLongState state, @SqlType(StandardTypes.BIGINT) long value)
    {
        if (state.isNull()) {
            state.setValue(value);
        }
        else {
            state.setValue(state.getValue() & value);
        }
        state.setNull(false);
    }

    @CombineFunction
    public static void combine(@AggregationState NullableLongState state, @AggregationState NullableLongState otherState)
    {
        if (state.isNull()) {
            state.set(otherState);
        }
        else if (!otherState.isNull()) {
            state.setValue(state.getValue() & otherState.getValue());
        }
    }

    @OutputFunction(StandardTypes.BIGINT)
    public static void output(@AggregationState NullableLongState state, BlockBuilder out)
    {
        NullableLongState.write(BigintType.BIGINT, state, out);
    }
}
