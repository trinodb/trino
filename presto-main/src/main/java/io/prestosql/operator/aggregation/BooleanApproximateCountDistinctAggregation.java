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

import static io.prestosql.spi.type.BigintType.BIGINT;

@AggregationFunction("approx_distinct")
public final class BooleanApproximateCountDistinctAggregation
{
    private BooleanApproximateCountDistinctAggregation() {}

    @InputFunction
    public static void input(BooleanDistinctState state, @SqlType(StandardTypes.BOOLEAN) boolean value, @SqlType(StandardTypes.DOUBLE) double maxStandardError)
    {
        @SuppressWarnings("NumericCastThatLosesPrecision")
        byte newState = (byte) (state.getByte() | (value ? 1 : 2));
        state.setByte(newState);
    }

    @CombineFunction
    public static void combineState(BooleanDistinctState state, BooleanDistinctState otherState)
    {
        state.setByte((byte) (state.getByte() | otherState.getByte()));
    }

    @OutputFunction(StandardTypes.BIGINT)
    public static void evaluateFinal(BooleanDistinctState state, BlockBuilder out)
    {
        BIGINT.writeLong(out, Integer.bitCount(state.getByte()));
    }
}
