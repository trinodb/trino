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

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.Type;

import static io.trino.operator.aggregation.state.TriStateBooleanState.FALSE_VALUE;
import static io.trino.operator.aggregation.state.TriStateBooleanState.NULL_VALUE;
import static io.trino.operator.aggregation.state.TriStateBooleanState.TRUE_VALUE;
import static io.trino.spi.type.BooleanType.BOOLEAN;

public class TriStateBooleanStateSerializer
        implements AccumulatorStateSerializer<TriStateBooleanState>
{
    @Override
    public Type getSerializedType()
    {
        return BOOLEAN;
    }

    @Override
    public void serialize(TriStateBooleanState state, BlockBuilder out)
    {
        if (state.getValue() == NULL_VALUE) {
            out.appendNull();
        }
        else {
            BOOLEAN.writeBoolean(out, state.getValue() == TRUE_VALUE);
        }
    }

    @Override
    public void deserialize(Block block, int index, TriStateBooleanState state)
    {
        state.setValue(BOOLEAN.getBoolean(block, index) ? TRUE_VALUE : FALSE_VALUE);
    }
}
