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

import io.trino.operator.aggregation.state.CovarianceState;
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

// covar_samp and covar_pop share a covariance intermediate state, so the partials and finals for
// every input type live in this single class to declare the intermediate row functions only once
@AggregationFunction
public final class CovarianceDecomposedAggregation
{
    private CovarianceDecomposedAggregation() {}

    @InputFunction
    public static void doubleInput(@AggregationState CovarianceState state, @SqlType(StandardTypes.DOUBLE) double dependentValue, @SqlType(StandardTypes.DOUBLE) double independentValue)
    {
        state.update(independentValue, dependentValue);
    }

    @InputFunction
    public static void realInput(@AggregationState CovarianceState state, @SqlType(StandardTypes.REAL) long dependentValue, @SqlType(StandardTypes.REAL) long independentValue)
    {
        state.update(intBitsToFloat((int) independentValue), intBitsToFloat((int) dependentValue));
    }

    // the intermediate row layout must match the generated CovarianceState serializer, which orders
    // state fields alphabetically: (c2, count, mean_x, mean_y)
    @InputFunction(hidden = true)
    public static void intermediateInput(
            @AggregationState CovarianceState state,
            @BlockPosition @SqlType("row(c2 double, count bigint, mean_x double, mean_y double)") ValueBlock block,
            @BlockIndex int position)
    {
        RowBlock rowBlock = (RowBlock) block;
        state.merge(
                BIGINT.getLong(rowBlock.getFieldBlock(1), position),
                DOUBLE.getDouble(rowBlock.getFieldBlock(2), position),
                DOUBLE.getDouble(rowBlock.getFieldBlock(3), position),
                DOUBLE.getDouble(rowBlock.getFieldBlock(0), position));
    }

    @AggregationFunction(value = "covariance$intermediate", hidden = true)
    @OutputFunction(value = "row(c2 double, count bigint, mean_x double, mean_y double)", decomposition = @Decomposition(partial = "covariance$intermediate", output = "covariance$intermediate"))
    public static void intermediateOutput(@AggregationState CovarianceState state, BlockBuilder out)
    {
        ((RowBlockBuilder) out).buildEntry(fieldBuilders -> {
            DOUBLE.writeDouble(fieldBuilders.get(0), state.getC2());
            BIGINT.writeLong(fieldBuilders.get(1), state.getCount());
            DOUBLE.writeDouble(fieldBuilders.get(2), state.getMeanX());
            DOUBLE.writeDouble(fieldBuilders.get(3), state.getMeanY());
        });
    }

    @AggregationFunction(value = "covar_samp$final", hidden = true)
    @SqlNullable
    @OutputFunction(value = StandardTypes.DOUBLE, decomposition = @Decomposition(partial = "covariance$intermediate", output = "covar_samp$final"))
    public static void covarSampOutput(@AggregationState CovarianceState state, BlockBuilder out)
    {
        if (state.getCount() <= 1) {
            out.appendNull();
        }
        else {
            DOUBLE.writeDouble(out, state.getCovarianceSample());
        }
    }

    @AggregationFunction(value = "covar_pop$final", hidden = true)
    @SqlNullable
    @OutputFunction(value = StandardTypes.DOUBLE, decomposition = @Decomposition(partial = "covariance$intermediate", output = "covar_pop$final"))
    public static void covarPopOutput(@AggregationState CovarianceState state, BlockBuilder out)
    {
        if (state.getCount() == 0) {
            out.appendNull();
        }
        else {
            DOUBLE.writeDouble(out, state.getCovariancePopulation());
        }
    }

    @AggregationFunction(value = "covar_samp_real$final", hidden = true)
    @SqlNullable
    @OutputFunction(value = StandardTypes.REAL, decomposition = @Decomposition(partial = "covariance$intermediate", output = "covar_samp_real$final"))
    public static void covarSampRealOutput(@AggregationState CovarianceState state, BlockBuilder out)
    {
        if (state.getCount() <= 1) {
            out.appendNull();
        }
        else {
            REAL.writeLong(out, floatToRawIntBits((float) state.getCovarianceSample()));
        }
    }

    @AggregationFunction(value = "covar_pop_real$final", hidden = true)
    @SqlNullable
    @OutputFunction(value = StandardTypes.REAL, decomposition = @Decomposition(partial = "covariance$intermediate", output = "covar_pop_real$final"))
    public static void covarPopRealOutput(@AggregationState CovarianceState state, BlockBuilder out)
    {
        if (state.getCount() == 0) {
            out.appendNull();
        }
        else {
            REAL.writeLong(out, floatToRawIntBits((float) state.getCovariancePopulation()));
        }
    }
}
