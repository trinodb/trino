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
package io.trino.operator.aggregation.multimapagg;

import com.google.common.collect.ImmutableList;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ColumnarRow;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import static io.trino.operator.aggregation.multimapagg.GroupedMultimapAggregationState.KEY_CHANNEL;
import static io.trino.operator.aggregation.multimapagg.GroupedMultimapAggregationState.VALUE_CHANNEL;
import static io.trino.spi.block.ColumnarRow.toColumnarRow;
import static java.util.Objects.requireNonNull;

public class MultimapAggregationStateSerializer
        implements AccumulatorStateSerializer<MultimapAggregationState>
{
    private final Type keyType;
    private final Type valueType;
    private final ArrayType arrayType;

    public MultimapAggregationStateSerializer(@TypeParameter("K") Type keyType, @TypeParameter("V") Type valueType)
    {
        this.keyType = requireNonNull(keyType);
        this.valueType = requireNonNull(valueType);
        this.arrayType = new ArrayType(RowType.anonymous(ImmutableList.of(valueType, keyType)));
    }

    @Override
    public Type getSerializedType()
    {
        return arrayType;
    }

    @Override
    public void serialize(MultimapAggregationState state, BlockBuilder out)
    {
        if (state.isEmpty()) {
            out.appendNull();
            return;
        }
        ((ArrayBlockBuilder) out).buildEntry(elementBuilder -> {
            RowBlockBuilder rowBlockBuilder = (RowBlockBuilder) elementBuilder;
            state.forEach((keyBlock, valueBlock, position) -> rowBlockBuilder.buildEntry(fieldBuilders -> {
                valueType.appendTo(valueBlock, position, fieldBuilders.get(0));
                keyType.appendTo(keyBlock, position, fieldBuilders.get(1));
            }));
        });
    }

    @Override
    public void deserialize(Block block, int index, MultimapAggregationState state)
    {
        state.reset();
        ColumnarRow columnarRow = toColumnarRow(arrayType.getObject(block, index));
        Block keys = columnarRow.getField(KEY_CHANNEL);
        Block values = columnarRow.getField(VALUE_CHANNEL);
        for (int i = 0; i < columnarRow.getPositionCount(); i++) {
            state.add(keys, values, i);
        }
    }
}
