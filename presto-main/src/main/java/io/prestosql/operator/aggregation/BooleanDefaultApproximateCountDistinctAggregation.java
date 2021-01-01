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
package io.prestosql.operator.aggregation;

import io.prestosql.operator.aggregation.state.BooleanDistinctState;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AggregationFunction;
import io.prestosql.spi.function.CombineFunction;
import io.prestosql.spi.function.InputFunction;
import io.prestosql.spi.function.OutputFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

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

    @CombineFunction
    public static void combineState(BooleanDistinctState state, BooleanDistinctState otherState)
    {
        BooleanApproximateCountDistinctAggregation.combineState(state, otherState);
    }

    @OutputFunction(StandardTypes.BIGINT)
    public static void evaluateFinal(BooleanDistinctState state, BlockBuilder out)
    {
        BooleanApproximateCountDistinctAggregation.evaluateFinal(state, out);
    }
}
