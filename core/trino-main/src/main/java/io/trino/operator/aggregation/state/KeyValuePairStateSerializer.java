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
import io.trino.spi.function.Convention;
import io.trino.spi.function.OperatorDependency;
import io.trino.spi.function.OperatorType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.Type;
import io.trino.type.BlockTypeOperators.BlockPositionEqual;
import io.trino.type.BlockTypeOperators.BlockPositionHashCode;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static java.util.Objects.requireNonNull;

public class KeyValuePairStateSerializer
        implements AccumulatorStateSerializer<KeyValuePairsState>
{
    private final Type mapType;
    private final BlockPositionEqual keyEqual;
    private final BlockPositionHashCode keyHashCode;

    public KeyValuePairStateSerializer(
            @TypeParameter("MAP(K, V)") Type mapType,
            @OperatorDependency(
                    operator = OperatorType.EQUAL,
                    argumentTypes = {"K", "K"},
                    convention = @Convention(arguments = {BLOCK_POSITION, BLOCK_POSITION}, result = NULLABLE_RETURN))
                    BlockPositionEqual keyEqual,
            @OperatorDependency(
                    operator = OperatorType.HASH_CODE,
                    argumentTypes = "K",
                    convention = @Convention(arguments = BLOCK_POSITION, result = FAIL_ON_NULL))
                    BlockPositionHashCode keyHashCode)
    {
        this.mapType = requireNonNull(mapType, "mapType is null");
        this.keyEqual = requireNonNull(keyEqual, "keyEqual is null");
        this.keyHashCode = requireNonNull(keyHashCode, "keyHashCode is null");
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
        state.set(new KeyValuePairs((Block) mapType.getObject(block, index), state.getKeyType(), keyEqual, keyHashCode, state.getValueType()));
    }
}
