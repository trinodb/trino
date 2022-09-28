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

import io.trino.operator.aggregation.state.TriStateBooleanState;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.StandardTypes;

import static io.trino.operator.aggregation.state.TriStateBooleanState.FALSE_VALUE;
import static io.trino.operator.aggregation.state.TriStateBooleanState.NULL_VALUE;
import static io.trino.operator.aggregation.state.TriStateBooleanState.TRUE_VALUE;

@AggregationFunction(value = "bool_and", alias = "every")
public final class BooleanAndAggregation
{
    private BooleanAndAggregation() {}

    @InputFunction
    public static void booleanAnd(@AggregationState TriStateBooleanState state, @SqlType(StandardTypes.BOOLEAN) boolean value)
    {
        // if the value is false, the result is false
        if (!value) {
            state.setValue(FALSE_VALUE);
        }
        else {
            // if the current value is unset, set result to true
            if (state.getValue() == NULL_VALUE) {
                state.setValue(TRUE_VALUE);
            }
        }
    }

    @CombineFunction
    public static void combine(@AggregationState TriStateBooleanState state, @AggregationState TriStateBooleanState otherState)
    {
        if (state.getValue() == NULL_VALUE) {
            state.setValue(otherState.getValue());
            return;
        }
        if (otherState.getValue() == FALSE_VALUE) {
            state.setValue(FALSE_VALUE);
        }
    }

    @OutputFunction(StandardTypes.BOOLEAN)
    public static void output(@AggregationState TriStateBooleanState state, BlockBuilder out)
    {
        TriStateBooleanState.write(BooleanType.BOOLEAN, state, out);
    }
}
