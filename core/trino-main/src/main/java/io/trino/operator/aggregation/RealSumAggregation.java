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
import io.trino.operator.aggregation.state.NullableDoubleState;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.Decomposition;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.WindowAccumulator;
import io.trino.spi.function.WindowIndex;
import io.trino.spi.type.StandardTypes;
import io.trino.type.Reals;

import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;

@AggregationFunction(value = "sum", windowAccumulator = RealSumAggregation.RealSumWindowAccumulator.class)
public final class RealSumAggregation
{
    private RealSumAggregation() {}

    @InputFunction
    public static void sum(@AggregationState NullableDoubleState state, @SqlType(StandardTypes.REAL) long value)
    {
        state.setNull(false);
        state.setValue(state.getValue() + intBitsToFloat((int) value));
    }

    @OutputFunction(value = StandardTypes.REAL, decomposition = @Decomposition(partial = "sum_real$intermediate", output = "sum_real$final"))
    public static void output(@AggregationState NullableDoubleState state, BlockBuilder out)
    {
        if (state.isNull()) {
            out.appendNull();
        }
        else {
            REAL.writeLong(out, floatToRawIntBits((float) state.getValue()));
        }
    }

    // sum(REAL) uses double as an intermediate type, so the partial and final cannot be mixed into the same class
    @AggregationFunction
    public static final class RealSumDecomposedAggregation
    {
        private RealSumDecomposedAggregation() {}

        @InputFunction
        public static void partialInput(@AggregationState NullableDoubleState state, @SqlType(StandardTypes.REAL) long value)
        {
            state.setNull(false);
            state.setValue(state.getValue() + intBitsToFloat((int) value));
        }

        @InputFunction
        public static void intermediateInput(@AggregationState NullableDoubleState state, @SqlType(StandardTypes.DOUBLE) double value)
        {
            state.setNull(false);
            state.setValue(state.getValue() + value);
        }

        @AggregationFunction(value = "sum_real$intermediate", hidden = true)
        @OutputFunction(value = StandardTypes.DOUBLE, decomposition = @Decomposition(partial = "sum_real$intermediate", output = "sum_real$intermediate"))
        public static void intermediateOutput(@AggregationState NullableDoubleState state, BlockBuilder out)
        {
            if (state.isNull()) {
                out.appendNull();
            }
            else {
                DOUBLE.writeDouble(out, state.getValue());
            }
        }

        @AggregationFunction(value = "sum_real$final", hidden = true)
        @OutputFunction(value = StandardTypes.REAL, decomposition = @Decomposition(partial = "sum_real$intermediate", output = "sum_real$final"))
        public static void output(@AggregationState NullableDoubleState state, BlockBuilder out)
        {
            if (state.isNull()) {
                out.appendNull();
            }
            else {
                REAL.writeLong(out, floatToRawIntBits((float) state.getValue()));
            }
        }
    }

    public static class RealSumWindowAccumulator
            implements WindowAccumulator
    {
        private long count;
        private double sum;

        @UsedByGeneratedCode
        public RealSumWindowAccumulator() {}

        private RealSumWindowAccumulator(long count, double sum)
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
            return new RealSumWindowAccumulator(count, sum);
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
            // If the sum is finite, all values to be removed are finite
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
                REAL.writeLong(blockBuilder, Reals.toReal((float) sum));
            }
        }
    }
}
