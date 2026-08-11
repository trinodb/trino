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

import io.trino.operator.aggregation.state.NullableLongState;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.Decomposition;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.BigintType;
import io.trino.type.BigintOperators;

import static io.trino.spi.type.StandardTypes.INTERVAL_DAY_TO_SECOND;
import static io.trino.type.IntervalDayTimeType.INTERVAL_DAY_TIME;

@AggregationFunction("sum")
public final class IntervalDayToSecondSumAggregation
{
    private IntervalDayToSecondSumAggregation() {}

    @InputFunction
    public static void sum(NullableLongState state, @SqlType(INTERVAL_DAY_TO_SECOND) long value)
    {
        state.setNull(false);
        state.setValue(BigintOperators.add(state.getValue(), value));
    }

    @AggregationFunction(value = "sum_interval_day_to_second$partial", hidden = true)
    @SqlNullable
    @OutputFunction(value = "BIGINT", decomposition = @Decomposition(partial = "sum_interval_day_to_second$partial", output = "sum_interval$merge"))
    public static void intermediateOutput(NullableLongState state, BlockBuilder out)
    {
        NullableLongState.write(BigintType.BIGINT, state, out);
    }

    @SqlNullable
    @OutputFunction(value = INTERVAL_DAY_TO_SECOND, decomposition = @Decomposition(partial = "sum_interval_day_to_second$partial", output = "sum_interval_day_to_second$final"))
    public static void output(NullableLongState state, BlockBuilder out)
    {
        NullableLongState.write(INTERVAL_DAY_TIME, state, out);
    }
}
