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
import io.trino.operator.aggregation.state.LongAndDoubleState;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.Decomposition;
import io.trino.spi.function.Description;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.Subsumed;
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
    public static void input(@AggregationState LongAndDoubleState state, @SqlType("REAL") long value)
    {
        state.setLong(state.getLong() + 1);
        state.setDouble(state.getDouble() + intBitsToFloat((int) value));
    }

    @SqlNullable
    @OutputFunction(value = "REAL", decomposition = @Decomposition(partial = "avg$intermediate", output = "avg_real$final", subsumes = {@Subsumed(function = "sum", output = "avg_sum_real$final"), @Subsumed(function = "count", output = "avg_count$final")}))
    public static void output(@AggregationState LongAndDoubleState state, BlockBuilder out)
    {
        long count = state.getLong();
        if (count == 0) {
            out.appendNull();
        }
        else {
            REAL.writeLong(out, toReal((float) (state.getDouble() / count)));
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
