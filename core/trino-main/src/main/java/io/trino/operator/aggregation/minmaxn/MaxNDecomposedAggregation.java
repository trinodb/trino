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

import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.SqlRow;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.Decomposition;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;

// merges max_n intermediates, which carry the capacity and the collected values
@AggregationFunction
public final class MaxNDecomposedAggregation
{
    private MaxNDecomposedAggregation() {}

    @InputFunction
    @TypeParameter("E")
    public static void intermediateInput(
            @AggregationState("E") MaxNState state,
            @SqlType("row(bigint, array(E))") SqlRow value)
    {
        state.merge(value);
    }

    @AggregationFunction(value = "max_n$merge", hidden = true)
    @SqlNullable
    @OutputFunction(value = "row(bigint, array(E))", decomposition = @Decomposition(partial = "max_n$merge"))
    public static void intermediateOutput(@AggregationState("E") MaxNState state, BlockBuilder out)
    {
        state.serialize(out);
    }

    @AggregationFunction(value = "max_n$final", hidden = true)
    @SqlNullable
    @OutputFunction(value = "array(E)", decomposition = @Decomposition(partial = "max_n$merge", output = "max_n$final"))
    public static void output(@AggregationState("E") MaxNState state, BlockBuilder out)
    {
        state.writeAllSorted(out);
    }
}
