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
package io.trino.operator.aggregation.minmaxbyn;

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

// merges min_by_n intermediates, which carry the capacity and the collected keys and values
@AggregationFunction
public final class MinByNDecomposedAggregation
{
    private MinByNDecomposedAggregation() {}

    @InputFunction
    @TypeParameter("K")
    @TypeParameter("V")
    public static void intermediateInput(
            @AggregationState({"K", "V"}) MinByNState state,
            @SqlType("row(bigint, array(K), array(V))") SqlRow value)
    {
        state.merge(value);
    }

    @AggregationFunction(value = "min_by_n$merge", hidden = true)
    @SqlNullable
    @OutputFunction(value = "row(bigint, array(K), array(V))", decomposition = @Decomposition(partial = "min_by_n$merge"))
    public static void intermediateOutput(@AggregationState({"K", "V"}) MinByNState state, BlockBuilder out)
    {
        state.serialize(out);
    }

    @AggregationFunction(value = "min_by_n$final", hidden = true)
    @SqlNullable
    @OutputFunction(value = "array(V)", decomposition = @Decomposition(partial = "min_by_n$merge", output = "min_by_n$final"))
    public static void output(@AggregationState({"K", "V"}) MinByNState state, BlockBuilder out)
    {
        state.popAll(out);
    }
}
