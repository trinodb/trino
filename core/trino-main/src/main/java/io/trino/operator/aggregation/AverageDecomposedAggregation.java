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
import static io.trino.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static io.trino.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static io.trino.type.Reals.toReal;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.round;

// all avg variants share a (count, sum) intermediate, so the partials and finals for every input
// type live in this single class to declare the intermediate row functions only once
@AggregationFunction
public final class AverageDecomposedAggregation
{
    private AverageDecomposedAggregation() {}

    @InputFunction
    public static void bigintInput(@AggregationState LongAndDoubleState state, @SqlType(StandardTypes.BIGINT) long value)
    {
        state.setLong(state.getLong() + 1);
        state.setDouble(state.getDouble() + value);
    }

    @InputFunction
    public static void doubleInput(@AggregationState LongAndDoubleState state, @SqlType(StandardTypes.DOUBLE) double value)
    {
        state.setLong(state.getLong() + 1);
        state.setDouble(state.getDouble() + value);
    }

    @InputFunction
    public static void realInput(@AggregationState LongAndDoubleState state, @SqlType(StandardTypes.REAL) long value)
    {
        state.setLong(state.getLong() + 1);
        state.setDouble(state.getDouble() + intBitsToFloat((int) value));
    }

    @InputFunction
    public static void intervalDayToSecondInput(@AggregationState LongAndDoubleState state, @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long value)
    {
        state.setLong(state.getLong() + 1);
        state.setDouble(state.getDouble() + value);
    }

    @InputFunction
    public static void intervalYearToMonthInput(@AggregationState LongAndDoubleState state, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long value)
    {
        state.setLong(state.getLong() + 1);
        state.setDouble(state.getDouble() + value);
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

    @AggregationFunction(value = "avg$intermediate", hidden = true)
    @OutputFunction(value = "row(sum double, count bigint)", decomposition = @Decomposition(partial = "avg$intermediate", output = "avg$intermediate"))
    public static void intermediateOutput(@AggregationState LongAndDoubleState state, BlockBuilder out)
    {
        ((RowBlockBuilder) out).buildEntry(fieldBuilders -> {
            DOUBLE.writeDouble(fieldBuilders.get(0), state.getDouble());
            BIGINT.writeLong(fieldBuilders.get(1), state.getLong());
        });
    }

    @AggregationFunction(value = "avg$final", hidden = true)
    @SqlNullable
    @OutputFunction(value = StandardTypes.DOUBLE, decomposition = @Decomposition(partial = "avg$intermediate", output = "avg$final"))
    public static void output(@AggregationState LongAndDoubleState state, BlockBuilder out)
    {
        long count = state.getLong();
        if (count == 0) {
            out.appendNull();
        }
        else {
            DOUBLE.writeDouble(out, state.getDouble() / count);
        }
    }

    @AggregationFunction(value = "avg_real$final", hidden = true)
    @SqlNullable
    @OutputFunction(value = StandardTypes.REAL, decomposition = @Decomposition(partial = "avg$intermediate", output = "avg_real$final"))
    public static void realOutput(@AggregationState LongAndDoubleState state, BlockBuilder out)
    {
        long count = state.getLong();
        if (count == 0) {
            out.appendNull();
        }
        else {
            REAL.writeLong(out, toReal((float) (state.getDouble() / count)));
        }
    }

    @AggregationFunction(value = "avg_interval_day_to_second$final", hidden = true)
    @SqlNullable
    @OutputFunction(value = StandardTypes.INTERVAL_DAY_TO_SECOND, decomposition = @Decomposition(partial = "avg$intermediate", output = "avg_interval_day_to_second$final"))
    public static void intervalDayToSecondOutput(@AggregationState LongAndDoubleState state, BlockBuilder out)
    {
        long count = state.getLong();
        if (count == 0) {
            out.appendNull();
        }
        else {
            INTERVAL_DAY_TIME.writeLong(out, round(state.getDouble() / count));
        }
    }

    @AggregationFunction(value = "avg_interval_year_to_month$final", hidden = true)
    @SqlNullable
    @OutputFunction(value = StandardTypes.INTERVAL_YEAR_TO_MONTH, decomposition = @Decomposition(partial = "avg$intermediate", output = "avg_interval_year_to_month$final"))
    public static void intervalYearToMonthOutput(@AggregationState LongAndDoubleState state, BlockBuilder out)
    {
        long count = state.getLong();
        if (count == 0) {
            out.appendNull();
        }
        else {
            INTERVAL_YEAR_MONTH.writeLong(out, round(state.getDouble() / count));
        }
    }
}
