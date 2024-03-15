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
package io.trino.plugin.deltalake.kernel.data;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StructType;
import io.trino.spi.block.Block;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.ColumnarArray;
import io.trino.spi.block.ColumnarMap;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.VariableWidthBlock;

import java.math.BigDecimal;

import static java.util.Objects.requireNonNull;

public class TrinoColumnVectorWrapper
        extends AbstractTrinoColumnVectorWrapper
{
    private final DataType deltaType;
    private final Block trinoBlock;

    public TrinoColumnVectorWrapper(DataType deltaType, Block trinoBlock)
    {
        this.trinoBlock = requireNonNull(trinoBlock, "trinoBlock is null");
        this.deltaType = requireNonNull(deltaType, "deltaType is null");
    }

    @Override
    public Block getTrinoBlock()
    {
        return trinoBlock;
    }

    @Override
    public int getSize()
    {
        return trinoBlock.getPositionCount();
    }

    @Override
    public DataType getDataType()
    {
        return deltaType;
    }

    @Override
    public boolean isNullAt(int rowId)
    {
        return trinoBlock.isNull(rowId);
    }

    @Override
    public boolean getBoolean(int rowId)
    {
        return ((ByteArrayBlock) trinoBlock.getSingleValueBlock(rowId)).getByte(0) != 0;
    }

    @Override
    public byte getByte(int rowId)
    {
        return ((ByteArrayBlock) trinoBlock.getSingleValueBlock(rowId)).getByte(0);
    }

    @Override
    public short getShort(int rowId)
    {
        return (short) ((IntArrayBlock) trinoBlock.getSingleValueBlock(rowId)).getInt(0);
    }

    @Override
    public long getLong(int rowId)
    {
        return ((LongArrayBlock) trinoBlock.getSingleValueBlock(rowId)).getLong(0);
    }

    @Override
    public int getInt(int rowId)
    {
        return ((IntArrayBlock) trinoBlock.getSingleValueBlock(rowId)).getInt(0);
    }

    @Override
    public float getFloat(int rowId)
    {
        return Float.intBitsToFloat(((IntArrayBlock) trinoBlock.getSingleValueBlock(rowId)).getInt(0));
    }

    @Override
    public double getDouble(int rowId)
    {
        return Double.longBitsToDouble(((LongArrayBlock) trinoBlock.getSingleValueBlock(rowId)).getLong(0));
    }

    @Override
    public byte[] getBinary(int rowId)
    {
        VariableWidthBlock block = (VariableWidthBlock) trinoBlock.getSingleValueBlock(rowId);
        return block.getSlice(0).getBytes();
    }

    @Override
    public String getString(int rowId)
    {
        VariableWidthBlock block = (VariableWidthBlock) trinoBlock.getSingleValueBlock(rowId);
        return block.getSlice(0).toStringUtf8();
    }

    @Override
    public BigDecimal getDecimal(int rowId)
    {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public MapValue getMap(int rowId)
    {
        ColumnarMap columnarMap = ColumnarMap.toColumnarMap(trinoBlock.getSingleValueBlock(rowId));
        DataType keyType = ((MapType) deltaType).getKeyType();
        DataType valueType = ((MapType) deltaType).getValueType();

        return new MapValue()
        {
            @Override
            public int getSize()
            {
                return columnarMap.getEntryCount(0);
            }

            @Override
            public ColumnVector getKeys()
            {
                return new TrinoColumnVectorWrapper(keyType, columnarMap.getKeysBlock());
            }

            @Override
            public ColumnVector getValues()
            {
                return new TrinoColumnVectorWrapper(valueType, columnarMap.getValuesBlock());
            }
        };
    }

    @Override
    public ArrayValue getArray(int rowId)
    {
        ColumnarArray columnarArray = ColumnarArray.toColumnarArray(trinoBlock.getSingleValueBlock(rowId));
        DataType elementType = ((ArrayType) deltaType).getElementType();

        return new ArrayValue()
        {
            @Override
            public int getSize()
            {
                return columnarArray.getLength(0);
            }

            @Override
            public ColumnVector getElements()
            {
                return new TrinoColumnVectorWrapper(elementType, columnarArray.getElementsBlock());
            }
        };
    }

    @Override
    public ColumnVector getChild(int ordinal)
    {
        RowBlock rowBlock = (RowBlock) trinoBlock.getLoadedBlock();
        StructType structType = (StructType) deltaType;
        return new TrinoColumnVectorWrapper(
                structType.at(ordinal).getDataType(),
                rowBlock.getFieldBlock(ordinal));
    }
}
