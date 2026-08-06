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

import io.trino.operator.aggregation.state.LongAndDoubleState;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.BlockIndex;
import io.trino.spi.function.BlockPosition;
import io.trino.spi.function.Decomposition;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;

// all geometric_mean variants share a (count, log sum) intermediate, so the partials and finals for
// every input type live in this single class to declare the intermediate row functions only once
@AggregationFunction
public final class GeometricMeanDecomposedAggregation
{
    private GeometricMeanDecomposedAggregation() {}

    @InputFunction
    public static void bigintInput(@AggregationState LongAndDoubleState state, @SqlType(StandardTypes.BIGINT) long value)
    {
        state.setLong(state.getLong() + 1);
        state.setDouble(state.getDouble() + Math.log(value));
    }

    @InputFunction
    public static void doubleInput(@AggregationState LongAndDoubleState state, @SqlType(StandardTypes.DOUBLE) double value)
    {
        state.setLong(state.getLong() + 1);
        state.setDouble(state.getDouble() + Math.log(value));
    }

    @InputFunction
    public static void realInput(@AggregationState LongAndDoubleState state, @SqlType(StandardTypes.REAL) long value)
    {
        state.setLong(state.getLong() + 1);
        state.setDouble(state.getDouble() + Math.log(intBitsToFloat((int) value)));
    }

    // the intermediate row layout must match the generated LongAndDoubleState serializer, which orders
    // state fields alphabetically: (sum double, count bigint)
    @InputFunction(hidden = true)
    public static void intermediateInput(
            @AggregationState LongAndDoubleState state,
            @BlockPosition @SqlType("row(sum double, count bigint)") ValueBlock block,
            @BlockIndex int position)
    {
        RowBlock rowBlock = (RowBlock) block;
        state.setDouble(state.getDouble() + DOUBLE.getDouble(rowBlock.getFieldBlock(0), position));
        state.setLong(state.getLong() + BIGINT.getLong(rowBlock.getFieldBlock(1), position));
    }

    @AggregationFunction(value = "geometric_mean$intermediate", hidden = true)
    @OutputFunction(value = "row(sum double, count bigint)", decomposition = @Decomposition(partial = "geometric_mean$intermediate", output = "geometric_mean$intermediate"))
    public static void intermediateOutput(@AggregationState LongAndDoubleState state, BlockBuilder out)
    {
        ((RowBlockBuilder) out).buildEntry(fieldBuilders -> {
            DOUBLE.writeDouble(fieldBuilders.get(0), state.getDouble());
            BIGINT.writeLong(fieldBuilders.get(1), state.getLong());
        });
    }

    @AggregationFunction(value = "geometric_mean_count$final", hidden = true)
    @OutputFunction(value = StandardTypes.BIGINT, decomposition = @Decomposition(partial = "geometric_mean$intermediate", output = "geometric_mean_count$final"))
    public static void countOutput(@AggregationState LongAndDoubleState state, BlockBuilder out)
    {
        BIGINT.writeLong(out, state.getLong());
    }

    @AggregationFunction(value = "geometric_mean$final", hidden = true)
    @SqlNullable
    @OutputFunction(value = StandardTypes.DOUBLE, decomposition = @Decomposition(partial = "geometric_mean$intermediate", output = "geometric_mean$final"))
    public static void output(@AggregationState LongAndDoubleState state, BlockBuilder out)
    {
        long count = state.getLong();
        if (count == 0) {
            out.appendNull();
        }
        else {
            DOUBLE.writeDouble(out, Math.exp(state.getDouble() / count));
        }
    }

    @AggregationFunction(value = "geometric_mean_real$final", hidden = true)
    @SqlNullable
    @OutputFunction(value = StandardTypes.REAL, decomposition = @Decomposition(partial = "geometric_mean$intermediate", output = "geometric_mean_real$final"))
    public static void realOutput(@AggregationState LongAndDoubleState state, BlockBuilder out)
    {
        long count = state.getLong();
        if (count == 0) {
            out.appendNull();
        }
        else {
            REAL.writeLong(out, floatToRawIntBits((float) Math.exp(state.getDouble() / count)));
        }
    }
}
