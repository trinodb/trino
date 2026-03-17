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

import io.trino.operator.aggregation.state.LongAndNumberState;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TrinoNumber;

import java.math.BigDecimal;

import static io.trino.spi.type.NumberType.NUMBER;
import static io.trino.type.NumberOperators.add;
import static io.trino.type.NumberOperators.divide;

@AggregationFunction(value = "avg")
public final class NumberAverageAggregation
{
    private NumberAverageAggregation() {}

    @InputFunction
    public static void input(LongAndNumberState state, @SqlType(StandardTypes.NUMBER) TrinoNumber value)
    {
        state.setLong(state.getLong() + 1);
        if (state.getNumber() == null) {
            state.setNumber(value);
        }
        else {
            state.setNumber(add(state.getNumber(), value));
        }
    }

    @CombineFunction
    public static void combine(LongAndNumberState state, LongAndNumberState otherState)
    {
        if (otherState.getLong() == 0) {
            return;
        }

        if (state.getLong() == 0) {
            state.setLong(otherState.getLong());
            state.setNumber(otherState.getNumber());
            return;
        }

        state.setLong(state.getLong() + otherState.getLong());
        state.setNumber(add(state.getNumber(), otherState.getNumber()));
    }

    @OutputFunction(StandardTypes.NUMBER)
    public static void output(LongAndNumberState state, BlockBuilder out)
    {
        long count = state.getLong();
        if (count == 0) {
            out.appendNull();
        }
        else {
            TrinoNumber countAsNumber = TrinoNumber.from(BigDecimal.valueOf(count));
            TrinoNumber average = divide(state.getNumber(), countAsNumber);
            NUMBER.writeObject(out, average);
        }
    }
}
