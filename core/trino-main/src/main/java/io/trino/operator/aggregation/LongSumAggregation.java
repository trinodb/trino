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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.operator.aggregation.state.NullableLongState;
import io.trino.operator.window.InternalWindowIndex;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.function.WindowAccumulator;
import io.trino.spi.function.WindowIndex;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.type.BigintOperators;
import io.trino.type.StreamType;

import static io.trino.spi.type.BigintType.BIGINT;

@AggregationFunction(value = "sum", windowAccumulator = LongSumAggregation.LongSumWindowAccumulator.class)
public final class LongSumAggregation
{
    private LongSumAggregation() {}

    @InputFunction
    @TypeParameter("T")
    public static void sum(@AggregationState NullableLongState state, @SqlType("T") long value)
    {
        state.setNull(false);
        state.setValue(BigintOperators.add(state.getValue(), value));
    }

    @InputFunction
    @TypeParameter("T")
    public static void sum(@TypeParameter("T") Type type, @AggregationState NullableLongState state, @SqlType("T") Block value)
    {
        if (type instanceof StreamType streamType) {
            Streams.stream(streamType.valueIterable(value))
                    .map(Long.class::cast)
                    .reduce(BigintOperators::add)
                    .ifPresent(v -> sum(state, v));
        }
        else {
            throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    @CombineFunction
    public static void combine(@AggregationState NullableLongState state, @AggregationState NullableLongState otherState)
    {
        if (state.isNull()) {
            state.set(otherState);
            return;
        }

        state.setValue(BigintOperators.add(state.getValue(), otherState.getValue()));
    }

    @OutputFunction(StandardTypes.BIGINT)
    public static void output(@AggregationState NullableLongState state, BlockBuilder out)
    {
        NullableLongState.write(BIGINT, state, out);
    }

    public static class LongSumWindowAccumulator
            implements WindowAccumulator
    {
        private long count;
        private long sum;

        @UsedByGeneratedCode
        public LongSumWindowAccumulator() {}

        private LongSumWindowAccumulator(long count, long sum)
        {
            this.count = count;
            this.sum = sum;
        }

        @Override
        public long getEstimatedSize()
        {
            return Long.BYTES + Long.BYTES;
        }

        @Override
        public WindowAccumulator copy()
        {
            return new LongSumWindowAccumulator(count, sum);
        }

        private Iterable<Object> getValues(WindowIndex index, int position)
        {
            if (index instanceof InternalWindowIndex internalWindowIndex && internalWindowIndex.getType(0) instanceof StreamType streamType) {
                Block block = index.getSingleValueBlock(0, position);
                return streamType.valueIterable(block);
            }

            return ImmutableList.of(index.getLong(0, position));
        }

        @Override
        public void addInput(WindowIndex index, int startPosition, int endPosition)
        {
            for (int i = startPosition; i <= endPosition; i++) {
                if (!index.isNull(0, i)) {
                    for (Object value : getValues(index, i)) {
                        sum += (long) value;
                        count++;
                    }
                }
            }
        }

        @Override
        public boolean removeInput(WindowIndex index, int startPosition, int endPosition)
        {
            for (int i = startPosition; i <= endPosition; i++) {
                if (!index.isNull(0, i)) {
                    for (Object value : getValues(index, i)) {
                        sum -= (long) value;
                        count--;
                    }
                }
            }
            return true;
        }

        @Override
        public void output(BlockBuilder blockBuilder)
        {
            if (count == 0) {
                blockBuilder.appendNull();
            }
            else {
                BIGINT.writeLong(blockBuilder, sum);
            }
        }
    }
}
