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

import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.LongTimestamp;

import static io.trino.spi.type.BigintType.BIGINT;

public final class LongTimestampAggregation
{
    private LongTimestampAggregation() {}

    public static void input(LongTimestampAggregationState state, LongTimestamp value)
    {
        state.setValue(state.getValue() + 1);
    }

    public static void combine(LongTimestampAggregationState stateA, LongTimestampAggregationState stateB)
    {
        stateA.setValue(stateA.getValue() + stateB.getValue());
    }

    public static void output(LongTimestampAggregationState state, BlockBuilder blockBuilder)
    {
        BIGINT.writeLong(blockBuilder, state.getValue());
    }
}
