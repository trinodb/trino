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
import io.trino.plugin.datasketches.state.IntersectionState;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import static org.apache.datasketches.common.Util.DEFAULT_UPDATE_SEED;

@AggregationFunction("theta_sketch_intersection")
public final class Intersection
{
    private Intersection() {}

    @InputFunction
    public static void input(@AggregationState IntersectionState state, @SqlType(StandardTypes.VARBINARY) Slice inputValue)
    {
        state.setSeed(DEFAULT_UPDATE_SEED);
        state.addSketch(inputValue);
    }

    @CombineFunction
    public static void combine(@AggregationState IntersectionState state, IntersectionState otherState)
    {
        state.merge(otherState);
    }

    @OutputFunction(StandardTypes.VARBINARY)
    public static void output(@AggregationState IntersectionState state, BlockBuilder out)
    {
        Slice sketch = state.getSketch();
        if (sketch == null) {
            out.appendNull();
            return;
        }
        ((VariableWidthBlockBuilder) out).writeEntry(sketch, 0, sketch.length());
    }
}
