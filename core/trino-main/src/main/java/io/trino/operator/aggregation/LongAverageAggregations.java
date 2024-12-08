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
import io.trino.spi.function.SqlType;
import io.trino.spi.function.WindowAccumulator;
import io.trino.spi.function.WindowIndex;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.StandardTypes;

import java.math.BigDecimal;

import static io.trino.spi.type.DecimalType.createDecimalType;

@AggregationFunction("avg")
public final class LongAverageAggregations
{
    private LongAverageAggregations() {}

    @InputFunction
    public static void input(@AggregationState LongState state, @SqlType(StandardTypes.BIGINT) long value)
    {
        state.setValue(state.getValue() + value);
        state.setCount(state.getCount() + 1);
    }

    @CombineFunction
    public static void combine(@AggregationState LongState state, @AggregationState LongState otherState)
    {
        state.setValue(state.getValue() + otherState.getValue());
        state.setCount(state.getCount() + otherState.getCount());
    }

    @OutputFunction("decimal(38,6)")
    public static void output(@AggregationState LongState state, BlockBuilder out)
    {
        long count = state.getCount();
        if (count == 0) {
            out.appendNull();
        }
        else {
            BigDecimal sum = BigDecimal.valueOf(state.getValue());
            BigDecimal average = sum.divide(BigDecimal.valueOf(count), createDecimalType(38, 6).getScale(), BigDecimal.ROUND_HALF_UP);

            Int128 encodedAverage = Decimals.encodeScaledValue(average, 6);
            createDecimalType(38, 6).writeObject(out, encodedAverage);
        }
    }

    public static class LongAverageWindowAccumulator
            implements WindowAccumulator
    {
        private long count;
        private BigDecimal sum = BigDecimal.ZERO;

        @UsedByGeneratedCode
        public LongAverageWindowAccumulator() {}

        private LongAverageWindowAccumulator(long count, BigDecimal sum)
        {
            this.count = count;
            this.sum = sum;
        }

        @Override
        public long getEstimatedSize()
        {
            return Long.BYTES + sum.unscaledValue().bitLength() / 8;
        }

        @Override
        public WindowAccumulator copy()
        {
            return new LongAverageWindowAccumulator(count, sum);
        }

        @Override
        public void addInput(WindowIndex index, int startPosition, int endPosition)
        {
            for (int i = startPosition; i <= endPosition; i++) {
                if (!index.isNull(0, i)) {
                    sum = sum.add(BigDecimal.valueOf(index.getLong(0, i)));
                    count++;
                }
            }
        }

        @Override
        public boolean removeInput(WindowIndex index, int startPosition, int endPosition)
        {
            for (int i = startPosition; i <= endPosition; i++) {
                if (!index.isNull(0, i)) {
                    sum = sum.subtract(BigDecimal.valueOf(index.getLong(0, i)));
                    count--;
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
                BigDecimal average = sum.divide(BigDecimal.valueOf(count), createDecimalType(38, 6).getScale(), BigDecimal.ROUND_HALF_UP);
                createDecimalType(38, 6).writeObject(blockBuilder, average);
            }
        }
    }
}
