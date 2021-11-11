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
package io.trino.operator.aggregation.listagg;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.spi.block.AbstractRowBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ColumnarRow;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.block.ColumnarRow.toColumnarRow;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class ListaggAggregationStateSerializer
        implements AccumulatorStateSerializer<ListaggAggregationState>
{
    private final Type elementType;
    private final Type arrayType;
    private final Type serializedType;

    public ListaggAggregationStateSerializer(Type elementType)
    {
        this.elementType = elementType;
        this.arrayType = new ArrayType(elementType);
        this.serializedType = RowType.anonymous(ImmutableList.of(VARCHAR, BOOLEAN, VARCHAR, BOOLEAN, arrayType));
    }

    @Override
    public Type getSerializedType()
    {
        return serializedType;
    }

    @Override
    public void serialize(ListaggAggregationState state, BlockBuilder out)
    {
        if (state.isEmpty()) {
            out.appendNull();
        }
        else {
            BlockBuilder rowBlockBuilder = out.beginBlockEntry();
            VARCHAR.writeSlice(rowBlockBuilder, state.getSeparator());
            BOOLEAN.writeBoolean(rowBlockBuilder, state.isOverflowError());
            VARCHAR.writeSlice(rowBlockBuilder, state.getOverflowFiller());
            BOOLEAN.writeBoolean(rowBlockBuilder, state.showOverflowEntryCount());

            BlockBuilder stateElementsBlockBuilder = rowBlockBuilder.beginBlockEntry();
            state.forEach((block, position) -> {
                elementType.appendTo(block, position, stateElementsBlockBuilder);
                return true;
            });
            rowBlockBuilder.closeEntry();

            out.closeEntry();
        }
    }

    @Override
    public void deserialize(Block block, int index, ListaggAggregationState state)
    {
        checkArgument(block instanceof AbstractRowBlock);
        ColumnarRow columnarRow = toColumnarRow(block);

        Slice separator = VARCHAR.getSlice(columnarRow.getField(0), index);
        boolean overflowError = BOOLEAN.getBoolean(columnarRow.getField(1), index);
        Slice overflowFiller = VARCHAR.getSlice(columnarRow.getField(2), index);
        boolean showOverflowEntryCount = BOOLEAN.getBoolean(columnarRow.getField(3), index);
        Block stateBlock = (Block) arrayType.getObject(columnarRow.getField(4), index);

        state.reset();
        state.setSeparator(separator);
        state.setOverflowError(overflowError);
        state.setOverflowFiller(overflowFiller);
        state.setShowOverflowEntryCount(showOverflowEntryCount);
        for (int i = 0; i < stateBlock.getPositionCount(); i++) {
            state.add(stateBlock, i);
        }
    }
}
