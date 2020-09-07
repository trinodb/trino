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
package io.prestosql.operator.scalar;

import io.airlift.stats.TDigest;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.util.Failures.checkCondition;

public final class TDigestFunctions
{
    public static final double DEFAULT_WEIGHT = 1.0;

    private TDigestFunctions() {}

    @ScalarFunction("value_at_quantile")
    @Description("Given an input q between [0, 1], find the value whose rank in the sorted sequence of the n values represented by the tdigest is qn.")
    @SqlType(StandardTypes.DOUBLE)
    public static double valueAtQuantile(@SqlType(StandardTypes.TDIGEST) TDigest input, @SqlType(StandardTypes.DOUBLE) double quantile)
    {
        return input.valueAt(quantile);
    }

    @ScalarFunction("values_at_quantiles")
    @Description("For each input q between [0, 1], find the value whose rank in the sorted sequence of the n values represented by the tdigest is qn.")
    @SqlType("array(double)")
    public static Block valuesAtQuantiles(@SqlType(StandardTypes.TDIGEST) TDigest input, @SqlType("array(double)") Block percentilesArrayBlock)
    {
        BlockBuilder output = DOUBLE.createBlockBuilder(null, percentilesArrayBlock.getPositionCount());
        for (int i = 0; i < percentilesArrayBlock.getPositionCount(); i++) {
            DOUBLE.writeDouble(output, input.valueAt(DOUBLE.getDouble(percentilesArrayBlock, i)));
        }
        return output.build();
    }

    public static double verifyWeight(double weight)
    {
        checkCondition(weight >= 1, INVALID_FUNCTION_ARGUMENT, "weight must be >= 1, was %s", weight);
        return weight;
    }
}
