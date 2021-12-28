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

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.BlockIndex;
import io.trino.spi.function.BlockPosition;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.Description;
import io.trino.spi.function.InOut;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;

@AggregationFunction("arbitrary")
@Description("Return an arbitrary non-null input value")
public final class ArbitraryAggregationFunction
{
    private ArbitraryAggregationFunction() {}

    @InputFunction
    @TypeParameter("T")
    public static void input(
            @AggregationState("T") InOut state,
            @BlockPosition @SqlType("T") Block block,
            @BlockIndex int position)
            throws Throwable
    {
        if (state.isNull()) {
            state.set(block, position);
        }
    }

    @CombineFunction
    public static void combine(
            @AggregationState("T") InOut state,
            @AggregationState("T") InOut otherState)
            throws Throwable
    {
        if (state.isNull()) {
            state.set(otherState);
        }
    }

    @OutputFunction("T")
    public static void output(@AggregationState("T") InOut state, BlockBuilder out)
    {
        state.get(out);
    }
}
