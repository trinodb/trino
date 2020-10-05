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
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Sketches;

import static org.apache.datasketches.Util.DEFAULT_UPDATE_SEED;

/**
 * Take the first value from a list of strings.
 */
public class Estimate
{
    private Estimate() {}

    /**
     * Estimate the unique count from sketch binary.
     * @param inputValue The sketch binary
     */
    @ScalarFunction("thetasketch_estimate")
    @Description("Converts sketch bytearrays to double estimate")
    @SqlType(StandardTypes.DOUBLE)
    public static double estimate(@SqlType(StandardTypes.VARBINARY) Slice inputValue)
    {
        return estimate(inputValue, DEFAULT_UPDATE_SEED);
    }

    /**
     * Estimate the unique count from sketch binary.
     * @param inputValue The sketch binary
     * @param seed Seed value used to create this sketch
     */
    @ScalarFunction("thetasketch_estimate")
    @Description("Converts sketch bytearrays to double estimate")
    @SqlType(StandardTypes.DOUBLE)
    public static double estimate(@SqlType(StandardTypes.VARBINARY) Slice inputValue, @SqlType(StandardTypes.BIGINT) long seed)
    {
        if (inputValue.getBytes() == null || inputValue.getBytes().length == 0) {
            return 0;
        }

        Sketch sketch = Sketches.wrapSketch(Memory.wrap(inputValue.getBytes()), seed);

        return sketch.getEstimate();
    }
}
