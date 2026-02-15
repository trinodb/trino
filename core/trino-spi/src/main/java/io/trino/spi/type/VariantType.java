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
package io.trino.spi.type;

import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.block.PageBuilderStatus;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.block.VariantBlock;
import io.trino.spi.block.VariantBlockBuilder;
import io.trino.spi.function.BlockIndex;
import io.trino.spi.function.BlockPosition;
import io.trino.spi.function.FlatFixed;
import io.trino.spi.function.FlatFixedOffset;
import io.trino.spi.function.FlatVariableOffset;
import io.trino.spi.function.FlatVariableWidth;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.variant.Header;
import io.trino.spi.variant.Metadata;
import io.trino.spi.variant.Variant;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.READ_VALUE;
import static io.trino.spi.function.OperatorType.XX_HASH_64;
import static io.trino.spi.type.TypeOperatorDeclaration.extractOperatorDeclaration;
import static io.trino.spi.variant.Metadata.EMPTY_METADATA;
import static io.trino.spi.variant.Metadata.EMPTY_METADATA_SLICE;
import static java.lang.Math.abs;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.lang.invoke.MethodHandles.lookup;

public class VariantType
        extends AbstractType
        implements VariableWidthType
{
    public static final String NAME = "VARIANT";
    public static final VariantType VARIANT = new VariantType();

    private static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);

    private static final int EXPECTED_BYTES_PER_ENTRY = 32;
    private static final TypeOperatorDeclaration DEFAULT_READ_OPERATORS = extractOperatorDeclaration(VariantType.class, lookup(), Variant.class);

    private VariantType()
    {
        super(new TypeSignature(StandardTypes.VARIANT), Variant.class, VariantBlock.class);
    }

    @Override
    public String getDisplayName()
    {
        return NAME;
    }

    @Override
    public VariantBlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        int maxBlockSizeInBytes;
        if (blockBuilderStatus == null) {
            maxBlockSizeInBytes = PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
        }
        else {
            maxBlockSizeInBytes = blockBuilderStatus.getMaxPageSizeInBytes();
        }

        // it is guaranteed Math.min will not overflow; safe to cast
        int expectedBytes = (int) min((long) expectedEntries * expectedBytesPerEntry, maxBlockSizeInBytes);
        return new VariantBlockBuilder(
                blockBuilderStatus,
                expectedBytesPerEntry == 0 ? expectedEntries : min(expectedEntries, maxBlockSizeInBytes / expectedBytesPerEntry),
                expectedBytes);
    }

    @Override
    public VariantBlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return createBlockBuilder(blockBuilderStatus, expectedEntries, EXPECTED_BYTES_PER_ENTRY);
    }

    @Override
    public boolean isComparable()
    {
        return true;
    }

    @Override
    public TypeOperatorDeclaration getTypeOperatorDeclaration(TypeOperators typeOperators)
    {
        return DEFAULT_READ_OPERATORS;
    }

    @Override
    public Variant getObject(Block block, int position)
    {
        VariantBlock valueBlock = (VariantBlock) block.getUnderlyingValueBlock();
        int valuePosition = block.getUnderlyingValuePosition(position);
        return valueBlock.getVariant(valuePosition);
    }

    @Override
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        ((VariantBlockBuilder) blockBuilder).writeEntry((Variant) value);
    }

    @Override
    public Variant getObjectValue(Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        VariantBlock valueBlock = (VariantBlock) block.getUnderlyingValueBlock();
        int valuePosition = block.getUnderlyingValuePosition(position);
        return valueBlock.getVariant(valuePosition);
    }

    // There are two layouts, one for primitive values and one for nested value that may need metadata.
    //  fixed:
    //      total variable length (4 bytes) high bit indicates presence of metadata
    //  variable:
    //      metadata length (4 bytes) (if present)
    //      metadata bytes (metadata length bytes) (if present)
    //      value bytes (total length - metadata length bytes)

    @Override
    public int getFlatFixedSize()
    {
        return Integer.BYTES;
    }

    @Override
    public boolean isFlatVariableWidth()
    {
        return true;
    }

    @Override
    public int getFlatVariableWidthSize(Block block, int position)
    {
        VariantBlock variantBlock = (VariantBlock) block.getUnderlyingValueBlock();
        int rawPosition = block.getUnderlyingValuePosition(position) + variantBlock.getRawOffset();
        if (variantBlock.getBasicType(rawPosition).isContainer()) {
            long length = Integer.BYTES;
            length += getSliceLength(variantBlock.getRawMetadata(), rawPosition);
            length += getSliceLength(variantBlock.getValues(), rawPosition);
            return toIntExact(length);
        }
        return getSliceLength(variantBlock.getValues(), rawPosition);
    }

    private static int getSliceLength(Block nestedBlock, int position)
    {
        VariableWidthBlock variableWidthBlock = (VariableWidthBlock) nestedBlock.getUnderlyingValueBlock();
        return variableWidthBlock.getSliceLength(nestedBlock.getUnderlyingValuePosition(position));
    }

    @Override
    public int getFlatVariableWidthLength(byte[] fixedSizeSlice, int fixedSizeOffset)
    {
        return abs((int) INT_HANDLE.get(fixedSizeSlice, fixedSizeOffset));
    }

    @ScalarOperator(READ_VALUE)
    private static Variant readFlatToStack(
            @FlatFixed byte[] fixedSizeSlice,
            @FlatFixedOffset int fixedSizeOffset,
            @FlatVariableWidth byte[] variableSizeSlice,
            @FlatVariableOffset int variableSizeOffset)
    {
        int fixedValue = (int) INT_HANDLE.get(fixedSizeSlice, fixedSizeOffset);
        if (fixedValue > 0) {
            return Variant.from(EMPTY_METADATA, wrappedBuffer(variableSizeSlice, variableSizeOffset, fixedValue));
        }

        int metadataLength = (int) INT_HANDLE.get(variableSizeSlice, variableSizeOffset);
        Slice metadataSlice = wrappedBuffer(variableSizeSlice, variableSizeOffset + Integer.BYTES, metadataLength);
        Metadata metadata = Metadata.from(metadataSlice);

        Slice valueSlice = wrappedBuffer(
                variableSizeSlice,
                variableSizeOffset + Integer.BYTES + metadataLength,
                abs(fixedValue) - Integer.BYTES - metadataLength);

        return Variant.from(metadata, valueSlice);
    }

    @ScalarOperator(READ_VALUE)
    private static void readFlatToBlock(
            @FlatFixed byte[] fixedSizeSlice,
            @FlatFixedOffset int fixedSizeOffset,
            @FlatVariableWidth byte[] variableSizeSlice,
            @FlatVariableOffset int variableSizeOffset,
            BlockBuilder blockBuilder)
    {
        int fixedValue = (int) INT_HANDLE.get(fixedSizeSlice, fixedSizeOffset);
        if (fixedValue > 0) {
            ((VariantBlockBuilder) blockBuilder).writeEntry(
                    EMPTY_METADATA_SLICE,
                    wrappedBuffer(variableSizeSlice, variableSizeOffset, fixedValue));
            return;
        }

        int metadataLength = (int) INT_HANDLE.get(variableSizeSlice, variableSizeOffset);
        Slice metadataSlice = wrappedBuffer(variableSizeSlice, variableSizeOffset + Integer.BYTES, metadataLength);

        Slice valueSlice = wrappedBuffer(
                variableSizeSlice,
                variableSizeOffset + Integer.BYTES + metadataLength,
                abs(fixedValue) - Integer.BYTES - metadataLength);

        ((VariantBlockBuilder) blockBuilder).writeEntry(metadataSlice, valueSlice);
    }

    @ScalarOperator(READ_VALUE)
    private static void writeFlatFromStack(
            Variant value,
            @FlatFixed byte[] fixedSizeSlice,
            @FlatFixedOffset int fixedSizeOffset,
            @FlatVariableWidth byte[] variableSizeSlice,
            @FlatVariableOffset int variableSizeOffset)
    {
        Metadata metadata = value.metadata();
        Slice metadataSlice;
        metadataSlice = metadata == EMPTY_METADATA ? null : metadata.toSlice();
        Slice data = value.data();
        writeFlat(metadataSlice, data, fixedSizeSlice, fixedSizeOffset, variableSizeSlice, variableSizeOffset);
    }

    @ScalarOperator(READ_VALUE)
    private static void writeFlatFromBlock(
            @BlockPosition VariantBlock block,
            @BlockIndex int position,
            @FlatFixed byte[] fixedSizeSlice,
            @FlatFixedOffset int fixedSizeOffset,
            @FlatVariableWidth byte[] variableSizeSlice,
            @FlatVariableOffset int variableSizeOffset)
    {
        int rawPosition = position + block.getRawOffset();
        Slice metadataSlice = ((VariableWidthBlock) block.getRawMetadata().getUnderlyingValueBlock())
                .getSlice(block.getRawMetadata().getUnderlyingValuePosition(rawPosition));
        Slice data = ((VariableWidthBlock) block.getRawValues().getUnderlyingValueBlock())
                .getSlice(block.getRawValues().getUnderlyingValuePosition(rawPosition));
        writeFlat(metadataSlice, data, fixedSizeSlice, fixedSizeOffset, variableSizeSlice, variableSizeOffset);
    }

    private static void writeFlat(Slice metadataSlice, Slice data, byte[] fixedSizeSlice, int fixedSizeOffset, byte[] variableSizeSlice, int variableSizeOffset)
    {
        if (metadataSlice == null || !Header.getBasicType(data.getByte(0)).isContainer()) {
            int length = data.length();
            INT_HANDLE.set(fixedSizeSlice, fixedSizeOffset, length);
            data.getBytes(0, variableSizeSlice, variableSizeOffset, length);
            return;
        }

        int fixedValue = -(Integer.BYTES + metadataSlice.length() + data.length());

        // fixed part is negative total length
        INT_HANDLE.set(fixedSizeSlice, fixedSizeOffset, fixedValue);

        // variable part is: metadata length, metadata, data
        INT_HANDLE.set(variableSizeSlice, variableSizeOffset, metadataSlice.length());
        metadataSlice.getBytes(0, variableSizeSlice, variableSizeOffset + Integer.BYTES, metadataSlice.length());
        data.getBytes(0, variableSizeSlice, variableSizeOffset + Integer.BYTES + metadataSlice.length(), data.length());
    }

    @ScalarOperator(EQUAL)
    private static boolean equalOperator(Variant left, Variant right)
    {
        return left.equals(right);
    }

    @ScalarOperator(XX_HASH_64)
    private static long xxHash64Operator(Variant value)
    {
        return value.longHashCode();
    }
}
