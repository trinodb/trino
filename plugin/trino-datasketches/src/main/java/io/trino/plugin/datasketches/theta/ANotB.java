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
import io.airlift.slice.Slices;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import org.apache.datasketches.theta.ThetaAnotB;
import org.apache.datasketches.theta.ThetaSetOperation;
import org.apache.datasketches.theta.ThetaSketch;

import java.lang.foreign.MemorySegment;

import static org.apache.datasketches.common.Util.DEFAULT_UPDATE_SEED;

public final class ANotB
{
    private ANotB() {}

    @ScalarFunction("theta_sketch_a_not_b")
    @Description("Returns a sketch representing the set difference A-and-not-B of two theta sketches")
    @SqlType(StandardTypes.VARBINARY)
    public static Slice aNotB(@SqlType(StandardTypes.VARBINARY) Slice sketchA, @SqlType(StandardTypes.VARBINARY) Slice sketchB)
    {
        return aNotB(sketchA, sketchB, DEFAULT_UPDATE_SEED);
    }

    @ScalarFunction("theta_sketch_a_not_b")
    @Description("Returns a sketch representing the set difference A-and-not-B using the supplied seed")
    @SqlType(StandardTypes.VARBINARY)
    public static Slice aNotB(@SqlType(StandardTypes.VARBINARY) Slice sketchA, @SqlType(StandardTypes.VARBINARY) Slice sketchB, @SqlType(StandardTypes.BIGINT) long seed)
    {
        // An empty A yields an empty difference; an empty B subtracts nothing. Both cases return A
        // unchanged, matching the plugin convention that a zero-length varbinary is an absent sketch.
        if (sketchA.length() == 0 || sketchB.length() == 0) {
            return sketchA;
        }
        ThetaSketch a = ThetaSketch.wrap(MemorySegment.ofArray(sketchA.getBytes()), seed);
        ThetaSketch b = ThetaSketch.wrap(MemorySegment.ofArray(sketchB.getBytes()), seed);
        ThetaAnotB operation = ThetaSetOperation.builder().setSeed(seed).buildANotB();
        return Slices.wrappedBuffer(operation.aNotB(a, b).toByteArray());
    }
}
