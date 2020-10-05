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
package io.trino.plugin.datasketches.theta;

import io.airlift.slice.Slice;
import io.trino.plugin.datasketches.state.SketchState;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import org.apache.datasketches.Util;

/**
 * Merges a collection of sketches into a single sketch.
 */
@AggregationFunction("thetasketch_union")
public final class Union
{
    private Union() {}
    /**
     * Iterate over the sketchs and aggregate them.
     * @param state Current state of the sketch
     * @param inputValue Input to add to the aggregate
     */

    @InputFunction
    public static void input(@AggregationState SketchState state, @SqlType(StandardTypes.VARBINARY) Slice inputValue)
    {
        state.setNominalEntries(Util.DEFAULT_NOMINAL_ENTRIES);
        state.setSeed(Util.DEFAULT_UPDATE_SEED);
        state.setSketch(inputValue);
    }

    /**
     * Merge two aggregate sketchs.
     * @param state State that will contain the combined result
     * @param otherState Information from this state will be added to the other state
     */
    @CombineFunction
    public static void combine(@AggregationState SketchState state, SketchState otherState)
    {
        UnionWithParams.combine(state, otherState);
    }

    /**
     * Generate an output sketch from the aggregate sketch.
     * @param state Final sketch state
     * @param out Output
     */
    @OutputFunction(StandardTypes.VARBINARY)
    public static void output(@AggregationState SketchState state, BlockBuilder out)
    {
        UnionWithParams.output(state, out);
    }
}
