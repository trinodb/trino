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
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.type.BigintOperators;

import static io.trino.spi.type.StandardTypes.INTERVAL_YEAR_TO_MONTH;
import static io.trino.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;

@AggregationFunction("sum")
public final class IntervalYearToMonthSumAggregation
{
    private IntervalYearToMonthSumAggregation() {}

    @InputFunction
    public static void sum(NullableLongState state, @SqlType(INTERVAL_YEAR_TO_MONTH) long value)
    {
        state.setNull(false);
        state.setValue(BigintOperators.add(state.getValue(), value));
    }

    @CombineFunction
    public static void combine(NullableLongState state, NullableLongState otherState)
    {
        if (state.isNull()) {
            state.set(otherState);
            return;
        }

        state.setValue(BigintOperators.add(state.getValue(), otherState.getValue()));
    }

    @OutputFunction(INTERVAL_YEAR_TO_MONTH)
    public static void output(NullableLongState state, BlockBuilder out)
    {
        NullableLongState.write(INTERVAL_YEAR_MONTH, state, out);
    }
}
