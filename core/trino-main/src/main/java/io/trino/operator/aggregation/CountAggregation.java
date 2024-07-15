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

import io.trino.annotation.UsedByGeneratedCode;
import io.trino.operator.aggregation.state.LongState;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.WindowAccumulator;
import io.trino.spi.function.WindowIndex;
import io.trino.spi.type.StandardTypes;

import static io.trino.spi.type.BigintType.BIGINT;

@AggregationFunction(value = "count", windowAccumulator = CountAggregation.CountWindowAccumulator.class)
public final class CountAggregation
{
    private CountAggregation() {}

    @InputFunction
    public static void input(@AggregationState LongState state)
    {
        state.setValue(state.getValue() + 1);
    }

    @CombineFunction
    public static void combine(@AggregationState LongState state, @AggregationState LongState otherState)
    {
        state.setValue(state.getValue() + otherState.getValue());
    }

    @OutputFunction(StandardTypes.BIGINT)
    public static void output(@AggregationState LongState state, BlockBuilder out)
    {
        BIGINT.writeLong(out, state.getValue());
    }

    public static class CountWindowAccumulator
            implements WindowAccumulator
    {
        private long count;

        @UsedByGeneratedCode
        public CountWindowAccumulator() {}

        private CountWindowAccumulator(long count)
        {
            this.count = count;
        }

        @Override
        public long getEstimatedSize()
        {
            return Long.BYTES;
        }

        @Override
        public WindowAccumulator copy()
        {
            return new CountWindowAccumulator(count);
        }

        @Override
        public void addInput(WindowIndex index, int startPosition, int endPosition)
        {
            count += endPosition - startPosition + 1;
        }

        @Override
        public boolean removeInput(WindowIndex index, int startPosition, int endPosition)
        {
            count -= endPosition - startPosition + 1;
            return true;
        }

        @Override
        public void output(BlockBuilder blockBuilder)
        {
            BIGINT.writeLong(blockBuilder, count);
        }
    }
}
