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
package io.trino.operator.aggregation.arrayagg;

import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.MultisetType;
import io.trino.spi.type.Type;

public class MultisetAggregationStateSerializer
        implements AccumulatorStateSerializer<MultisetAggregationState>
{
    private final MultisetType multisetType;

    public MultisetAggregationStateSerializer(@TypeParameter("multiset(E)") Type multisetType)
    {
        this.multisetType = (MultisetType) multisetType;
    }

    @Override
    public Type getSerializedType()
    {
        return multisetType;
    }

    @Override
    public void serialize(MultisetAggregationState state, BlockBuilder out)
    {
        Block multiset = state.get();
        if (multiset == null) {
            out.appendNull();
        }
        else {
            ((ArrayBlockBuilder) out).buildEntry(elementBuilder -> {
                ValueBlock values = multiset.getUnderlyingValueBlock();
                for (int i = 0; i < multiset.getPositionCount(); i++) {
                    elementBuilder.append(values, multiset.getUnderlyingValuePosition(i));
                }
            });
        }
    }

    @Override
    public void deserialize(Block block, int index, MultisetAggregationState state)
    {
        // the accumulator holds the elements as a plain block; unwrap the SqlMultiset view
        state.set(multisetType.getObject(block, index).getElementBlock());
    }
}
