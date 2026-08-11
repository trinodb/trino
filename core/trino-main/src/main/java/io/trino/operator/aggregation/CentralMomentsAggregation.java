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

import io.trino.operator.aggregation.state.CentralMomentsState;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.BlockIndex;
import io.trino.spi.function.BlockPosition;
import io.trino.spi.function.Decomposition;
import io.trino.spi.function.Description;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.Subsumed;
import io.trino.spi.type.StandardTypes;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;

@AggregationFunction
@Description("Returns the central moments of the argument as an array")
public final class CentralMomentsAggregation
{
    private CentralMomentsAggregation() {}

    @InputFunction
    public static void doubleInput(@AggregationState CentralMomentsState state, @SqlType(StandardTypes.DOUBLE) double value)
    {
        state.update(value);
    }

    @InputFunction
    public static void bigintInput(@AggregationState CentralMomentsState state, @SqlType(StandardTypes.BIGINT) long value)
    {
        state.update((double) value);
    }

    // the intermediate row layout must match the generated CentralMomentsState serializer, which orders
    // state fields alphabetically: (count bigint, m1 double, m2 double, m3 double, m4 double)
    @InputFunction(hidden = true)
    public static void intermediateInput(
            @AggregationState CentralMomentsState state,
            @BlockPosition @SqlType("row(count bigint, m1 double, m2 double, m3 double, m4 double)") ValueBlock block,
            @BlockIndex int position)
    {
        RowBlock rowBlock = (RowBlock) block;
        state.merge(
                BIGINT.getLong(rowBlock.getFieldBlock(0), position),
                DOUBLE.getDouble(rowBlock.getFieldBlock(1), position),
                DOUBLE.getDouble(rowBlock.getFieldBlock(2), position),
                DOUBLE.getDouble(rowBlock.getFieldBlock(3), position),
                DOUBLE.getDouble(rowBlock.getFieldBlock(4), position));
    }

    @AggregationFunction(value = "central_moments$intermediate", hidden = true)
    @OutputFunction(value = "row(count bigint, m1 double, m2 double, m3 double, m4 double)", decomposition = @Decomposition(partial = "central_moments$intermediate"))
    public static void intermediateOutput(@AggregationState CentralMomentsState state, BlockBuilder out)
    {
        ((RowBlockBuilder) out).buildEntry(fieldBuilders -> {
            BIGINT.writeLong(fieldBuilders.get(0), state.getCount());
            DOUBLE.writeDouble(fieldBuilders.get(1), state.getM1());
            DOUBLE.writeDouble(fieldBuilders.get(2), state.getM2());
            DOUBLE.writeDouble(fieldBuilders.get(3), state.getM3());
            DOUBLE.writeDouble(fieldBuilders.get(4), state.getM4());
        });
    }

    @AggregationFunction(value = "central_moments_count$final", hidden = true)
    @OutputFunction(value = StandardTypes.BIGINT, decomposition = @Decomposition(partial = "central_moments$intermediate", output = "central_moments_count$final"))
    public static void countOutput(@AggregationState CentralMomentsState state, BlockBuilder out)
    {
        BIGINT.writeLong(out, state.getCount());
    }

    @AggregationFunction("skewness")
    @Description("Returns the skewness of the argument")
    @SqlNullable
    @OutputFunction(value = StandardTypes.DOUBLE, decomposition = @Decomposition(partial = "central_moments$intermediate", output = "skewness", subsumes = @Subsumed(function = "count", output = "central_moments_count$final")))
    public static void skewness(@AggregationState CentralMomentsState state, BlockBuilder out)
    {
        long n = state.getCount();

        if (n < 3) {
            out.appendNull();
        }
        else {
            double result = Math.sqrt(n) * state.getM3() / Math.pow(state.getM2(), 1.5);
            DOUBLE.writeDouble(out, result);
        }
    }

    @AggregationFunction("kurtosis")
    @Description("Returns the (excess) kurtosis of the argument")
    @SqlNullable
    @OutputFunction(value = StandardTypes.DOUBLE, decomposition = @Decomposition(partial = "central_moments$intermediate", output = "kurtosis", subsumes = @Subsumed(function = "count", output = "central_moments_count$final")))
    public static void kurtosis(@AggregationState CentralMomentsState state, BlockBuilder out)
    {
        double n = state.getCount();

        if (n < 4) {
            out.appendNull();
        }
        else {
            double m2 = state.getM2();
            double m4 = state.getM4();
            double result = ((n - 1) * n * (n + 1)) / ((n - 2) * (n - 3)) * m4 / (m2 * m2) - 3 * ((n - 1) * (n - 1)) / ((n - 2) * (n - 3));
            DOUBLE.writeDouble(out, result);
        }
    }
}
