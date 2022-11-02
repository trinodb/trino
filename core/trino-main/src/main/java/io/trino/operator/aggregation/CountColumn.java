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

import io.trino.operator.aggregation.state.LongState;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.BlockIndex;
import io.trino.spi.function.BlockPosition;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.Description;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.RemoveInputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;

import static io.trino.spi.type.BigintType.BIGINT;

@AggregationFunction("count")
@Description("Counts the non-null values")
public final class CountColumn
{
    private CountColumn() {}

    @InputFunction
    @TypeParameter("T")
    public static void input(
            @AggregationState LongState state,
            @BlockPosition @SqlType("T") Block block,
            @BlockIndex int position)
    {
        state.setValue(state.getValue() + 1);
    }

    @RemoveInputFunction
    public static void removeInput(
            @AggregationState LongState state,
            @BlockPosition @SqlType("T") Block block,
            @BlockIndex int position)
    {
        state.setValue(state.getValue() - 1);
    }

    @CombineFunction
    public static void combine(@AggregationState LongState state, LongState otherState)
    {
        state.setValue(state.getValue() + otherState.getValue());
    }

    @OutputFunction("BIGINT")
    public static void output(@AggregationState LongState state, BlockBuilder out)
    {
        BIGINT.writeLong(out, state.getValue());
    }
}
