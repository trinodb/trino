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

import io.trino.operator.aggregation.state.NumberState;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TrinoNumber;

import static io.trino.spi.type.NumberType.NUMBER;
import static io.trino.type.NumberOperators.add;

@AggregationFunction(value = "sum")
public final class NumberSumAggregation
{
    private NumberSumAggregation() {}

    @InputFunction
    public static void input(NumberState state, @SqlType(StandardTypes.NUMBER) TrinoNumber value)
    {
        if (state.getValue() == null) {
            state.setValue(value);
        }
        else {
            state.setValue(add(state.getValue(), value));
        }
    }

    @CombineFunction
    public static void combine(NumberState state, NumberState otherState)
    {
        if (state.getValue() == null) {
            if (otherState.getValue() == null) {
                return;
            }
            state.setValue(otherState.getValue());
            return;
        }

        if (otherState.getValue() != null) {
            state.setValue(add(state.getValue(), otherState.getValue()));
        }
    }

    @OutputFunction(StandardTypes.NUMBER)
    public static void output(NumberState state, BlockBuilder out)
    {
        if (state.getValue() == null) {
            out.appendNull();
        }
        else {
            NUMBER.writeObject(out, state.getValue());
        }
    }
}
