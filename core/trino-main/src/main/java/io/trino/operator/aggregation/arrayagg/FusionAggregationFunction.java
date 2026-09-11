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
package io.trino.operator.aggregation.arrayagg;

import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.SqlMultiset;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.Description;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;

/// Combines the input multisets into a single multiset whose multiplicity for each value is the sum
/// of its multiplicities across the inputs. It reuses the array_agg accumulator by adding every
/// element of every input multiset to the collected elements.
@AggregationFunction("fusion")
@Description("Combines the input multisets, summing element multiplicities")
public final class FusionAggregationFunction
{
    private FusionAggregationFunction() {}

    @InputFunction
    @TypeParameter("E")
    public static void input(
            @AggregationState("E") ArrayAggregationState state,
            @SqlType("multiset(E)") SqlMultiset multiset)
    {
        ValueBlock elements = multiset.getUnderlyingElementBlock();
        for (int i = 0; i < multiset.getSize(); i++) {
            state.add(elements, multiset.getUnderlyingElementPosition(i));
        }
    }

    @CombineFunction
    public static void combine(
            @AggregationState("E") ArrayAggregationState state,
            @AggregationState("E") ArrayAggregationState otherState)
    {
        state.merge(otherState);
    }

    @OutputFunction("multiset(E)")
    public static void output(
            @AggregationState("E") ArrayAggregationState state,
            BlockBuilder out)
    {
        if (state.isEmpty()) {
            out.appendNull();
        }
        else {
            ((ArrayBlockBuilder) out).buildEntry(state::writeAll);
        }
    }
}
