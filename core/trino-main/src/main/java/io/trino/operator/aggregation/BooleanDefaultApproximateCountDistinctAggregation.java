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
package io.trino.operator.aggregation;

import io.trino.operator.aggregation.state.BooleanDistinctState;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.Decomposition;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import static io.trino.spi.type.TinyintType.TINYINT;

@AggregationFunction("approx_distinct")
public final class BooleanDefaultApproximateCountDistinctAggregation
{
    // this value is ignored for boolean, but this is left here for completeness
    private static final double DEFAULT_STANDARD_ERROR = 0.023;

    private BooleanDefaultApproximateCountDistinctAggregation() {}

    @InputFunction
    public static void input(BooleanDistinctState state, @SqlType(StandardTypes.BOOLEAN) boolean value)
    {
        BooleanApproximateCountDistinctAggregation.input(state, value, DEFAULT_STANDARD_ERROR);
    }

    @AggregationFunction(value = "approx_distinct_boolean$partial", hidden = true)
    @OutputFunction(value = StandardTypes.TINYINT, decomposition = @Decomposition(partial = "approx_distinct_boolean$partial", output = "approx_distinct_boolean$merge"))
    public static void intermediateOutput(BooleanDistinctState state, BlockBuilder out)
    {
        TINYINT.writeByte(out, state.getByte());
    }

    @OutputFunction(value = StandardTypes.BIGINT, decomposition = @Decomposition(partial = "approx_distinct_boolean$partial", output = "approx_distinct_boolean$final"))
    public static void evaluateFinal(BooleanDistinctState state, BlockBuilder out)
    {
        BooleanApproximateCountDistinctAggregation.evaluateFinal(state, out);
    }
}
