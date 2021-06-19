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
package io.trino.operator.aggregation.state;

import io.trino.operator.aggregation.KeyValuePairs;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.type.BlockTypeOperators.BlockPositionEqual;
import io.trino.type.BlockTypeOperators.BlockPositionHashCode;

public class KeyValuePairStateSerializer
        implements AccumulatorStateSerializer<KeyValuePairsState>
{
    private final MapType mapType;
    private final BlockPositionEqual keyEqual;
    private final BlockPositionHashCode keyHashCode;

    public KeyValuePairStateSerializer(MapType mapType, BlockPositionEqual keyEqual, BlockPositionHashCode keyHashCode)
    {
        this.mapType = mapType;
        this.keyEqual = keyEqual;
        this.keyHashCode = keyHashCode;
    }

    @Override
    public Type getSerializedType()
    {
        return mapType;
    }

    @Override
    public void serialize(KeyValuePairsState state, BlockBuilder out)
    {
        if (state.get() == null) {
            out.appendNull();
        }
        else {
            state.get().serialize(out);
        }
    }

    @Override
    public void deserialize(Block block, int index, KeyValuePairsState state)
    {
        state.set(new KeyValuePairs(mapType.getObject(block, index), state.getKeyType(), keyEqual, keyHashCode, state.getValueType()));
    }
}
