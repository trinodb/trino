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

import io.trino.spi.block.Block;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateMetadata;

@AccumulatorStateMetadata(
        stateFactoryClass = MapAggregationStateFactory.class,
        stateSerializerClass = MapAggregationStateSerializer.class,
        typeParameters = {"K", "V"},
        serializedType = "map(K, V)")
public interface MapAggregationState
        extends AccumulatorState
{
    void add(ValueBlock keyBlock, int keyPosition, ValueBlock valueBlock, int valuePosition);

    default void merge(MapAggregationState other)
    {
        SqlMap serializedState = ((SingleMapAggregationState) other).removeTempSerializedState();
        int rawOffset = serializedState.getRawOffset();
        Block rawKeyBlock = serializedState.getRawKeyBlock();
        Block rawValueBlock = serializedState.getRawValueBlock();

        ValueBlock rawKeyValues = rawKeyBlock.getUnderlyingValueBlock();
        ValueBlock rawValueValues = rawValueBlock.getUnderlyingValueBlock();
        for (int i = 0; i < serializedState.getSize(); i++) {
            add(rawKeyValues, rawKeyBlock.getUnderlyingValuePosition(rawOffset + i), rawValueValues, rawValueBlock.getUnderlyingValuePosition(rawOffset + i));
        }
    }

    void writeAll(MapBlockBuilder out);
}
