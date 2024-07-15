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
import io.trino.operator.aggregation.state.DoubleState;
import io.trino.operator.aggregation.state.LongState;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.Description;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.WindowAccumulator;
import io.trino.spi.function.WindowIndex;

import static io.trino.spi.type.RealType.REAL;
import static io.trino.type.Reals.toReal;
import static java.lang.Float.intBitsToFloat;

@AggregationFunction(value = "avg", windowAccumulator = RealAverageAggregation.RealAverageWindowAccumulator.class)
@Description("Returns the average value of the argument")
public final class RealAverageAggregation
{
    private RealAverageAggregation() {}

    @InputFunction
    public static void input(
            @AggregationState LongState count,
            @AggregationState DoubleState sum,
            @SqlType("REAL") long value)
    {
        count.setValue(count.getValue() + 1);
        sum.setValue(sum.getValue() + intBitsToFloat((int) value));
    }

    @CombineFunction
    public static void combine(
            @AggregationState LongState count,
            @AggregationState DoubleState sum,
            @AggregationState LongState otherCount,
            @AggregationState DoubleState otherSum)
    {
        count.setValue(count.getValue() + otherCount.getValue());
        sum.setValue(sum.getValue() + otherSum.getValue());
    }

    @OutputFunction("REAL")
    public static void output(
            @AggregationState LongState count,
            @AggregationState DoubleState sum,
            BlockBuilder out)
    {
        if (count.getValue() == 0) {
            out.appendNull();
        }
        else {
            REAL.writeLong(out, toReal((float) (sum.getValue() / count.getValue())));
        }
    }

    public static class RealAverageWindowAccumulator
            implements WindowAccumulator
    {
        private long count;
        private double sum;

        @UsedByGeneratedCode
        public RealAverageWindowAccumulator() {}

        private RealAverageWindowAccumulator(long count, double sum)
        {
            this.count = count;
            this.sum = sum;
        }

        @Override
        public long getEstimatedSize()
        {
            return Long.BYTES + Double.BYTES;
        }

        @Override
        public WindowAccumulator copy()
        {
            return new RealAverageWindowAccumulator(count, sum);
        }

        @Override
        public void addInput(WindowIndex index, int startPosition, int endPosition)
        {
            for (int i = startPosition; i <= endPosition; i++) {
                if (!index.isNull(0, i)) {
                    sum += intBitsToFloat((int) index.getLong(0, i));
                    count++;
                }
            }
        }

        @Override
        public boolean removeInput(WindowIndex index, int startPosition, int endPosition)
        {
            // If sum is finite, all value to be removed are finite
            if (!Double.isFinite(sum)) {
                return false;
            }

            for (int i = startPosition; i <= endPosition; i++) {
                if (!index.isNull(0, i)) {
                    sum -= intBitsToFloat((int) index.getLong(0, i));
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
                REAL.writeLong(blockBuilder, toReal((float) (sum / count)));
            }
        }
    }
}
