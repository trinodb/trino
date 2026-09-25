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
import io.trino.spi.TrinoException;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import org.apache.datasketches.theta.ThetaSketch;

import java.lang.foreign.MemorySegment;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.lang.Math.toIntExact;
import static org.apache.datasketches.common.Util.DEFAULT_UPDATE_SEED;

public final class CardinalityBounds
{
    private CardinalityBounds() {}

    @ScalarFunction("theta_sketch_cardinality_lower_bound")
    @Description("Returns the lower bound of the sketch cardinality estimate for the given number of standard deviations (1, 2, or 3)")
    @SqlType(StandardTypes.DOUBLE)
    public static double lowerBound(@SqlType(StandardTypes.VARBINARY) Slice inputValue, @SqlType(StandardTypes.INTEGER) long numStdDev)
    {
        return lowerBound(inputValue, numStdDev, DEFAULT_UPDATE_SEED);
    }

    @ScalarFunction("theta_sketch_cardinality_lower_bound")
    @Description("Returns the lower bound of the sketch cardinality estimate using the supplied seed")
    @SqlType(StandardTypes.DOUBLE)
    public static double lowerBound(@SqlType(StandardTypes.VARBINARY) Slice inputValue, @SqlType(StandardTypes.INTEGER) long numStdDev, @SqlType(StandardTypes.BIGINT) long seed)
    {
        int stdDev = checkNumStdDev(numStdDev);
        if (inputValue.length() == 0) {
            return 0;
        }
        return ThetaSketch.wrap(MemorySegment.ofArray(inputValue.getBytes()), seed).getLowerBound(stdDev);
    }

    @ScalarFunction("theta_sketch_cardinality_upper_bound")
    @Description("Returns the upper bound of the sketch cardinality estimate for the given number of standard deviations (1, 2, or 3)")
    @SqlType(StandardTypes.DOUBLE)
    public static double upperBound(@SqlType(StandardTypes.VARBINARY) Slice inputValue, @SqlType(StandardTypes.INTEGER) long numStdDev)
    {
        return upperBound(inputValue, numStdDev, DEFAULT_UPDATE_SEED);
    }

    @ScalarFunction("theta_sketch_cardinality_upper_bound")
    @Description("Returns the upper bound of the sketch cardinality estimate using the supplied seed")
    @SqlType(StandardTypes.DOUBLE)
    public static double upperBound(@SqlType(StandardTypes.VARBINARY) Slice inputValue, @SqlType(StandardTypes.INTEGER) long numStdDev, @SqlType(StandardTypes.BIGINT) long seed)
    {
        int stdDev = checkNumStdDev(numStdDev);
        if (inputValue.length() == 0) {
            return 0;
        }
        return ThetaSketch.wrap(MemorySegment.ofArray(inputValue.getBytes()), seed).getUpperBound(stdDev);
    }

    private static int checkNumStdDev(long numStdDev)
    {
        if (numStdDev < 1 || numStdDev > 3) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "numStdDev must be 1, 2, or 3: " + numStdDev);
        }
        return toIntExact(numStdDev);
    }
}
