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
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import org.apache.datasketches.theta.JaccardSimilarity;
import org.apache.datasketches.theta.ThetaSketch;

import java.lang.foreign.MemorySegment;

import static org.apache.datasketches.common.Util.DEFAULT_UPDATE_SEED;

public final class Jaccard
{
    private Jaccard() {}

    @ScalarFunction("theta_sketch_jaccard_similarity")
    @Description("Returns the Jaccard similarity index of two theta sketches built with the default seed")
    @SqlType(StandardTypes.DOUBLE)
    public static double jaccardSimilarity(@SqlType(StandardTypes.VARBINARY) Slice sketchA, @SqlType(StandardTypes.VARBINARY) Slice sketchB)
    {
        boolean emptyA = sketchA.length() == 0;
        boolean emptyB = sketchB.length() == 0;
        if (emptyA && emptyB) {
            return 1;
        }
        if (emptyA || emptyB) {
            return 0;
        }
        ThetaSketch a = ThetaSketch.wrap(MemorySegment.ofArray(sketchA.getBytes()), DEFAULT_UPDATE_SEED);
        ThetaSketch b = ThetaSketch.wrap(MemorySegment.ofArray(sketchB.getBytes()), DEFAULT_UPDATE_SEED);
        // JaccardSimilarity.jaccard returns {lowerBound, estimate, upperBound}; index 1 is the point estimate.
        return JaccardSimilarity.jaccard(a, b)[1];
    }
}
