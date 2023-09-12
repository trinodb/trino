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
package io.trino.plugin.geospatial;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.geospatial.KdbTree;
import io.trino.geospatial.KdbTreeUtils;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.BlockIndex;
import io.trino.spi.function.BlockPosition;
import io.trino.spi.function.FlatFixed;
import io.trino.spi.function.FlatFixedOffset;
import io.trino.spi.function.FlatVariableWidth;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.type.AbstractVariableWidthType;
import io.trino.spi.type.TypeOperatorDeclaration;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.TypeSignature;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

import static io.trino.spi.function.OperatorType.READ_VALUE;
import static io.trino.spi.type.TypeOperatorDeclaration.extractOperatorDeclaration;
import static java.lang.invoke.MethodHandles.lookup;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class KdbTreeType
        extends AbstractVariableWidthType
{
    private static final TypeOperatorDeclaration TYPE_OPERATOR_DECLARATION = extractOperatorDeclaration(KdbTreeType.class, lookup(), Object.class);
    private static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);

    public static final KdbTreeType KDB_TREE = new KdbTreeType();
    public static final String NAME = "KdbTree";

    private KdbTreeType()
    {
        // The KDB tree type should be KdbTree but can not be since KdbTree is in
        // both the plugin class loader and the system class loader.  This was done
        // so the plan optimizer can process geo spatial joins.
        super(new TypeSignature(NAME), Object.class);
    }

    @Override
    public TypeOperatorDeclaration getTypeOperatorDeclaration(TypeOperators typeOperators)
    {
        return TYPE_OPERATOR_DECLARATION;
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        return getObject(block, position);
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            ((VariableWidthBlockBuilder) blockBuilder).buildEntry(valueBuilder -> block.writeSliceTo(position, 0, block.getSliceLength(position), valueBuilder));
        }
    }

    @Override
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        byte[] jsonBytes = KdbTreeUtils.toJsonBytes(((KdbTree) value));
        ((VariableWidthBlockBuilder) blockBuilder).writeEntry(jsonBytes, 0, jsonBytes.length);
    }

    @Override
    public Object getObject(Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        Slice bytes = block.getSlice(position, 0, block.getSliceLength(position));
        KdbTree kdbTree = KdbTreeUtils.fromJson(bytes.toStringUtf8());
        return kdbTree;
    }

    @Override
    public int getFlatFixedSize()
    {
        return 8;
    }

    @Override
    public int getFlatVariableWidthSize(Block block, int position)
    {
        return block.getSliceLength(position);
    }

    @Override
    public int relocateFlatVariableWidthOffsets(byte[] fixedSizeSlice, int fixedSizeOffset, byte[] variableSizeSlice, int variableSizeOffset)
    {
        INT_HANDLE.set(fixedSizeSlice, fixedSizeOffset + Integer.BYTES, variableSizeOffset);
        return (int) INT_HANDLE.get(fixedSizeSlice, fixedSizeOffset);
    }

    @ScalarOperator(READ_VALUE)
    private static Object readFlat(
            @FlatFixed byte[] fixedSizeSlice,
            @FlatFixedOffset int fixedSizeOffset,
            @FlatVariableWidth byte[] variableSizeSlice)
    {
        int length = (int) INT_HANDLE.get(fixedSizeSlice, fixedSizeOffset);
        int offset = (int) INT_HANDLE.get(fixedSizeSlice, fixedSizeOffset + Integer.BYTES);

        return KdbTreeUtils.fromJson(new String(variableSizeSlice, offset, length, UTF_8));
    }

    @ScalarOperator(READ_VALUE)
    private static void readFlatToBlock(
            @FlatFixed byte[] fixedSizeSlice,
            @FlatFixedOffset int fixedSizeOffset,
            @FlatVariableWidth byte[] variableSizeSlice,
            BlockBuilder blockBuilder)
    {
        int length = (int) INT_HANDLE.get(fixedSizeSlice, fixedSizeOffset);
        int offset = (int) INT_HANDLE.get(fixedSizeSlice, fixedSizeOffset + Integer.BYTES);

        ((VariableWidthBlockBuilder) blockBuilder).writeEntry(Slices.wrappedBuffer(variableSizeSlice, offset, length));
    }

    @ScalarOperator(READ_VALUE)
    private static void writeFlat(
            Object value,
            byte[] fixedSizeSlice,
            int fixedSizeOffset,
            byte[] variableWidthSlice,
            int variableSizeOffset)
    {
        byte[] bytes = KdbTreeUtils.toJsonBytes(((KdbTree) value));
        System.arraycopy(bytes, 0, variableWidthSlice, variableSizeOffset, bytes.length);

        INT_HANDLE.set(fixedSizeSlice, fixedSizeOffset, bytes.length);
        INT_HANDLE.set(fixedSizeSlice, fixedSizeOffset + Integer.BYTES, variableSizeOffset);
    }

    @ScalarOperator(READ_VALUE)
    private static void writeBlockToFlat(
            @BlockPosition Block block,
            @BlockIndex int position,
            byte[] fixedSizeSlice,
            int fixedSizeOffset,
            byte[] variableSizeSlice,
            int variableSizeOffset)
    {
        Slice bytes = block.getSlice(position, 0, block.getSliceLength(position));
        bytes.getBytes(0, variableSizeSlice, variableSizeOffset, bytes.length());

        INT_HANDLE.set(fixedSizeSlice, fixedSizeOffset, bytes.length());
        INT_HANDLE.set(fixedSizeSlice, fixedSizeOffset + Integer.BYTES, variableSizeOffset);
    }
}
