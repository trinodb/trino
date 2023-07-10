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
package io.trino.operator.aggregation.minmaxn;

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
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;

@AggregationFunction("min")
@Description("Returns the minimum values of the argument")
public final class MinNAggregationFunction
{
    private MinNAggregationFunction() {}

    @InputFunction
    @TypeParameter("E")
    public static void input(
            @AggregationState("E") MinNState state,
            @BlockPosition @SqlType("E") Block block,
            @SqlType("BIGINT") long n,
            @BlockIndex int blockIndex)
    {
        state.initialize(n);
        state.add(block, blockIndex);
    }

    @CombineFunction
    public static void combine(
            @AggregationState("E") MinNState state,
            @AggregationState("E") MinNState otherState)
    {
        state.merge(otherState);
    }

    @OutputFunction("array(E)")
    public static void output(@AggregationState("E") MinNState state, BlockBuilder out)
    {
        state.writeAll(out);
    }
}
