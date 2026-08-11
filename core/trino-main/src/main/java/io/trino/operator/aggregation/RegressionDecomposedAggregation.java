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

import io.trino.operator.aggregation.state.RegressionState;
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

// regr_slope and regr_intercept share a regression intermediate state, so the partials and finals
// for every input type live in this single class to declare the intermediate row functions only once
@AggregationFunction
public final class RegressionDecomposedAggregation
{
    private RegressionDecomposedAggregation() {}

    @InputFunction
    public static void doubleInput(@AggregationState RegressionState state, @SqlType(StandardTypes.DOUBLE) double dependentValue, @SqlType(StandardTypes.DOUBLE) double independentValue)
    {
        state.update(independentValue, dependentValue);
    }

    @InputFunction
    public static void realInput(@AggregationState RegressionState state, @SqlType(StandardTypes.REAL) long dependentValue, @SqlType(StandardTypes.REAL) long independentValue)
    {
        state.update(intBitsToFloat((int) independentValue), intBitsToFloat((int) dependentValue));
    }

    // the intermediate row layout must match the generated RegressionState serializer, which orders
    // state fields alphabetically: (c2, count, m2_x, mean_x, mean_y)
    @InputFunction(hidden = true)
    public static void intermediateInput(
            @AggregationState RegressionState state,
            @BlockPosition @SqlType("row(c2 double, count bigint, m2_x double, mean_x double, mean_y double)") ValueBlock block,
            @BlockIndex int position)
    {
        RowBlock rowBlock = (RowBlock) block;
        state.merge(
                BIGINT.getLong(rowBlock.getFieldBlock(1), position),
                DOUBLE.getDouble(rowBlock.getFieldBlock(3), position),
                DOUBLE.getDouble(rowBlock.getFieldBlock(4), position),
                DOUBLE.getDouble(rowBlock.getFieldBlock(0), position),
                DOUBLE.getDouble(rowBlock.getFieldBlock(2), position));
    }

    @AggregationFunction(value = "regression$intermediate", hidden = true)
    @OutputFunction(value = "row(c2 double, count bigint, m2_x double, mean_x double, mean_y double)", decomposition = @Decomposition(partial = "regression$intermediate"))
    public static void intermediateOutput(@AggregationState RegressionState state, BlockBuilder out)
    {
        ((RowBlockBuilder) out).buildEntry(fieldBuilders -> {
            DOUBLE.writeDouble(fieldBuilders.get(0), state.getC2());
            BIGINT.writeLong(fieldBuilders.get(1), state.getCount());
            DOUBLE.writeDouble(fieldBuilders.get(2), state.getM2X());
            DOUBLE.writeDouble(fieldBuilders.get(3), state.getMeanX());
            DOUBLE.writeDouble(fieldBuilders.get(4), state.getMeanY());
        });
    }

    @AggregationFunction(value = "regr_slope$final", hidden = true)
    @SqlNullable
    @OutputFunction(value = StandardTypes.DOUBLE, decomposition = @Decomposition(partial = "regression$intermediate", output = "regr_slope$final"))
    public static void slopeOutput(@AggregationState RegressionState state, BlockBuilder out)
    {
        double result = state.getRegressionSlope();
        if (Double.isFinite(result)) {
            DOUBLE.writeDouble(out, result);
        }
        else {
            out.appendNull();
        }
    }

    @AggregationFunction(value = "regr_intercept$final", hidden = true)
    @SqlNullable
    @OutputFunction(value = StandardTypes.DOUBLE, decomposition = @Decomposition(partial = "regression$intermediate", output = "regr_intercept$final"))
    public static void interceptOutput(@AggregationState RegressionState state, BlockBuilder out)
    {
        double result = state.getRegressionIntercept();
        if (Double.isFinite(result)) {
            DOUBLE.writeDouble(out, result);
        }
        else {
            out.appendNull();
        }
    }

    @AggregationFunction(value = "regr_slope_real$final", hidden = true)
    @SqlNullable
    @OutputFunction(value = StandardTypes.REAL, decomposition = @Decomposition(partial = "regression$intermediate", output = "regr_slope_real$final"))
    public static void slopeRealOutput(@AggregationState RegressionState state, BlockBuilder out)
    {
        double result = state.getRegressionSlope();
        if (Double.isFinite(result)) {
            REAL.writeLong(out, floatToRawIntBits((float) result));
        }
        else {
            out.appendNull();
        }
    }

    @AggregationFunction(value = "regr_intercept_real$final", hidden = true)
    @SqlNullable
    @OutputFunction(value = StandardTypes.REAL, decomposition = @Decomposition(partial = "regression$intermediate", output = "regr_intercept_real$final"))
    public static void interceptRealOutput(@AggregationState RegressionState state, BlockBuilder out)
    {
        double result = state.getRegressionIntercept();
        if (Double.isFinite(result)) {
            REAL.writeLong(out, floatToRawIntBits((float) result));
        }
        else {
            out.appendNull();
        }
    }
}
