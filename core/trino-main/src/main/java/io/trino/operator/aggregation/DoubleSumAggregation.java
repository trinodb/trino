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

import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.RemoveInputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.StandardTypes;

@AggregationFunction("sum")
public final class DoubleSumAggregation
{
    private DoubleSumAggregation() {}

    @InputFunction
    public static void sum(@AggregationState LongDoubleState state, @SqlType(StandardTypes.DOUBLE) double value)
    {
        state.setFirst(state.getFirst() + 1);
        state.setSecond(state.getSecond() + value);
    }

    @RemoveInputFunction
    public static void removeInput(@AggregationState LongDoubleState state, @SqlType(StandardTypes.DOUBLE) double value)
    {
        state.setFirst(state.getFirst() - 1);
        state.setSecond(state.getSecond() - value);
    }

    @CombineFunction
    public static void combine(@AggregationState LongDoubleState state, @AggregationState LongDoubleState otherState)
    {
        state.setFirst(state.getFirst() + otherState.getFirst());
        state.setSecond(state.getSecond() + otherState.getSecond());
    }

    @OutputFunction(StandardTypes.DOUBLE)
    public static void output(@AggregationState LongDoubleState state, BlockBuilder out)
    {
        if (state.getFirst() == 0) {
            out.appendNull();
        }
        else {
            DoubleType.DOUBLE.writeDouble(out, state.getSecond());
        }
    }
}
