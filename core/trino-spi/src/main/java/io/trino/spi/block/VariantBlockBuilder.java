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

package io.trino.spi.block;

import io.airlift.slice.Slice;
import io.trino.spi.variant.Variant;
import jakarta.annotation.Nullable;

import java.util.Arrays;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.checkIndex;
import static java.util.Objects.requireNonNull;

public class VariantBlockBuilder
        implements BlockBuilder
{
    private static final int INSTANCE_SIZE = instanceSize(VariantBlockBuilder.class);
    private static final VariantBlock NULL_VALUE_BLOCK = VariantBlock.createInternal(
            0,
            1,
            new boolean[] {true},
            VariableWidthBlockBuilder.NULL_VALUE_BLOCK,
            VariableWidthBlockBuilder.NULL_VALUE_BLOCK);

    @Nullable
    private final BlockBuilderStatus blockBuilderStatus;

    private int positionCount;
    private boolean[] variantIsNull;
    private final VariableWidthBlockBuilder metadataBlockBuilder;
    private final VariableWidthBlockBuilder valuesBlockBuilder;

    private boolean hasNullVariant;
    private boolean hasNonNullVariant;

    public VariantBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        this(blockBuilderStatus, expectedEntries, expectedEntries * 9);
    }

    public VariantBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytes)
    {
        this(
                blockBuilderStatus,
                new VariableWidthBlockBuilder(blockBuilderStatus, expectedEntries, 64),
                new VariableWidthBlockBuilder(blockBuilderStatus, expectedEntries, expectedBytes),
                new boolean[expectedEntries]);
    }

    private VariantBlockBuilder(
            @Nullable BlockBuilderStatus blockBuilderStatus,
            VariableWidthBlockBuilder metadataBlockBuilder,
            VariableWidthBlockBuilder valuesBlockBuilder,
            boolean[] variantIsNull)
    {
        this.blockBuilderStatus = blockBuilderStatus;
        this.positionCount = 0;
        this.variantIsNull = requireNonNull(variantIsNull, "variantIsNull is null");
        this.metadataBlockBuilder = requireNonNull(metadataBlockBuilder, "metadataBlockBuilder is null");
        this.valuesBlockBuilder = requireNonNull(valuesBlockBuilder, "valuesBlockBuilder is null");
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getSizeInBytes()
    {
        long sizeInBytes = (Integer.BYTES + Byte.BYTES) * (long) positionCount;
        sizeInBytes += metadataBlockBuilder.getSizeInBytes();
        sizeInBytes += valuesBlockBuilder.getSizeInBytes();
        return sizeInBytes;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        long size = INSTANCE_SIZE + sizeOf(variantIsNull);
        size += metadataBlockBuilder.getRetainedSizeInBytes();
        size += valuesBlockBuilder.getRetainedSizeInBytes();
        if (blockBuilderStatus != null) {
            size += BlockBuilderStatus.INSTANCE_SIZE;
        }
        return size;
    }

    public void writeEntry(Variant variant)
    {
        metadataBlockBuilder.writeEntry(variant.metadata().toSlice());
        valuesBlockBuilder.writeEntry(variant.data());
        entryAdded(false);
    }

    public void writeEntry(Slice metadata, Slice value)
    {
        metadataBlockBuilder.writeEntry(metadata);
        valuesBlockBuilder.writeEntry(value);
        entryAdded(false);
    }

    @Override
    public void append(ValueBlock block, int position)
    {
        VariantBlock variantBlock = (VariantBlock) block;
        if (block.isNull(position)) {
            appendNull();
            return;
        }

        Block rawMetadataBlock = variantBlock.getRawMetadata();
        Block rawValuesBlock = variantBlock.getRawValues();
        int startOffset = variantBlock.getOffsetBase();

        appendToField(rawMetadataBlock, startOffset + position, metadataBlockBuilder);
        appendToField(rawValuesBlock, startOffset + position, valuesBlockBuilder);
        entryAdded(false);
    }

    private static void appendToField(Block fieldBlock, int position, BlockBuilder fieldBlockBuilder)
    {
        switch (fieldBlock) {
            case RunLengthEncodedBlock rleBlock -> fieldBlockBuilder.append(rleBlock.getValue(), 0);
            case DictionaryBlock dictionaryBlock -> fieldBlockBuilder.append(dictionaryBlock.getDictionary(), dictionaryBlock.getId(position));
            case ValueBlock valueBlock -> fieldBlockBuilder.append(valueBlock, position);
        }
    }

    @Override
    public void appendRange(ValueBlock block, int offset, int length)
    {
        if (length == 0) {
            return;
        }

        VariantBlock variantBlock = (VariantBlock) block;
        ensureCapacity(positionCount + length);

        Block rawMetadataBlock = variantBlock.getRawMetadata();
        Block rawValuesBlock = variantBlock.getRawValues();
        int startOffset = variantBlock.getOffsetBase();

        appendRangeToField(rawMetadataBlock, startOffset + offset, length, metadataBlockBuilder);
        appendRangeToField(rawValuesBlock, startOffset + offset, length, valuesBlockBuilder);

        boolean[] rawVariantIsNull = variantBlock.getRawIsNull();
        if (rawVariantIsNull != null) {
            for (int i = 0; i < length; i++) {
                boolean isNull = rawVariantIsNull[startOffset + offset + i];
                hasNullVariant |= isNull;
                hasNonNullVariant |= !isNull;
                if (hasNullVariant & hasNonNullVariant) {
                    System.arraycopy(rawVariantIsNull, startOffset + offset + i, variantIsNull, positionCount + i, length - i);
                    break;
                }
                else {
                    variantIsNull[positionCount + i] = isNull;
                }
            }
        }
        else {
            hasNonNullVariant = true;
        }
        positionCount += length;
    }

    private static void appendRangeToField(Block fieldBlock, int offset, int length, BlockBuilder fieldBlockBuilder)
    {
        switch (fieldBlock) {
            case RunLengthEncodedBlock rleBlock -> fieldBlockBuilder.appendRepeated(rleBlock.getValue(), 0, length);
            case DictionaryBlock dictionaryBlock -> {
                int[] rawIds = dictionaryBlock.getRawIds();
                int rawIdsOffset = dictionaryBlock.getRawIdsOffset();
                fieldBlockBuilder.appendPositions(dictionaryBlock.getDictionary(), rawIds, rawIdsOffset + offset, length);
            }
            case ValueBlock valueBlock -> fieldBlockBuilder.appendRange(valueBlock, offset, length);
        }
    }

    @Override
    public void appendRepeated(ValueBlock block, int position, int count)
    {
        if (count == 0) {
            return;
        }

        VariantBlock variantBlock = (VariantBlock) block;
        ensureCapacity(positionCount + count);

        Block rawMetadataBlock = variantBlock.getRawMetadata();
        Block rawValuesBlock = variantBlock.getRawValues();
        int startOffset = variantBlock.getOffsetBase();

        appendRepeatedToField(rawMetadataBlock, startOffset + position, count, metadataBlockBuilder);
        appendRepeatedToField(rawValuesBlock, startOffset + position, count, valuesBlockBuilder);

        if (variantBlock.isNull(position)) {
            Arrays.fill(variantIsNull, positionCount, positionCount + count, true);
            hasNullVariant = true;
        }
        else {
            hasNonNullVariant = true;
        }

        positionCount += count;

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(Integer.BYTES + Byte.BYTES);
        }
    }

    private static void appendRepeatedToField(Block fieldBlock, int position, int count, BlockBuilder fieldBlockBuilder)
    {
        switch (fieldBlock) {
            case RunLengthEncodedBlock rleBlock -> fieldBlockBuilder.appendRepeated(rleBlock.getValue(), 0, count);
            case DictionaryBlock dictionaryBlock -> fieldBlockBuilder.appendRepeated(dictionaryBlock.getDictionary(), dictionaryBlock.getId(position), count);
            case ValueBlock valueBlock -> fieldBlockBuilder.appendRepeated(valueBlock, position, count);
        }
    }

    @Override
    public void appendPositions(ValueBlock block, int[] positions, int offset, int length)
    {
        if (length == 0) {
            return;
        }

        VariantBlock variantBlock = (VariantBlock) block;
        ensureCapacity(positionCount + length);

        Block rawMetadataBlock = variantBlock.getRawMetadata();
        Block rawValuesBlock = variantBlock.getRawValues();
        int startOffset = variantBlock.getOffsetBase();

        if (startOffset == 0) {
            appendPositionsToField(rawMetadataBlock, positions, offset, length, metadataBlockBuilder);
            appendPositionsToField(rawValuesBlock, positions, offset, length, valuesBlockBuilder);
        }
        else {
            int[] adjustedPositions = new int[length];
            for (int i = offset; i < offset + length; i++) {
                adjustedPositions[i - offset] = startOffset + positions[i];
            }

            appendPositionsToField(rawMetadataBlock, adjustedPositions, 0, length, metadataBlockBuilder);
            appendPositionsToField(rawValuesBlock, adjustedPositions, 0, length, valuesBlockBuilder);
        }

        boolean[] rawVariantIsNull = variantBlock.getRawIsNull();
        if (rawVariantIsNull != null) {
            for (int i = 0; i < length; i++) {
                if (rawVariantIsNull[startOffset + positions[offset + i]]) {
                    variantIsNull[positionCount + i] = true;
                    hasNullVariant = true;
                }
                else {
                    hasNonNullVariant = true;
                }
            }
        }
        else {
            hasNonNullVariant = true;
        }
        positionCount += length;
    }

    private static void appendPositionsToField(Block fieldBlock, int[] positions, int offset, int length, BlockBuilder fieldBlockBuilder)
    {
        switch (fieldBlock) {
            case RunLengthEncodedBlock rleBlock -> fieldBlockBuilder.appendRepeated(rleBlock.getValue(), 0, length);
            case DictionaryBlock dictionaryBlock -> {
                int[] newPositions = new int[length];
                for (int i = 0; i < newPositions.length; i++) {
                    newPositions[i] = dictionaryBlock.getId(positions[offset + i]);
                }
                fieldBlockBuilder.appendPositions(dictionaryBlock.getDictionary(), newPositions, 0, length);
            }
            case ValueBlock valueBlock -> fieldBlockBuilder.appendPositions(valueBlock, positions, offset, length);
        }
    }

    @Override
    public BlockBuilder appendNull()
    {
        metadataBlockBuilder.appendNull();
        valuesBlockBuilder.appendNull();
        entryAdded(true);
        return this;
    }

    @Override
    public void resetTo(int position)
    {
        checkIndex(position, positionCount + 1);
        positionCount = position;
        metadataBlockBuilder.resetTo(position);
        valuesBlockBuilder.resetTo(position);
    }

    private void entryAdded(boolean isNull)
    {
        ensureCapacity(positionCount + 1);

        variantIsNull[positionCount] = isNull;
        hasNullVariant |= isNull;
        hasNonNullVariant |= !isNull;
        positionCount++;

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(Integer.BYTES + Byte.BYTES);
        }
    }

    @Override
    public Block build()
    {
        if (!hasNonNullVariant) {
            return RunLengthEncodedBlock.create(NULL_VALUE_BLOCK, positionCount);
        }
        return buildValueBlock();
    }

    @Override
    public VariantBlock buildValueBlock()
    {
        Block metadataBlock = metadataBlockBuilder.buildValueBlock();
        Block valuesBlock = valuesBlockBuilder.buildValueBlock();
        return VariantBlock.createInternal(0, positionCount, hasNullVariant ? variantIsNull : null, metadataBlock, valuesBlock);
    }

    private void ensureCapacity(int capacity)
    {
        if (variantIsNull.length >= capacity) {
            return;
        }

        int newSize = BlockUtil.calculateNewArraySize(variantIsNull.length, capacity);
        variantIsNull = Arrays.copyOf(variantIsNull, newSize);
    }

    @Override
    public String toString()
    {
        return "VariantBlockBuilder{metadataBlockBuilder=%s, valuesBlockBuilder=%s}".formatted(metadataBlockBuilder, valuesBlockBuilder);
    }

    @Override
    public BlockBuilder newBlockBuilderLike(int expectedEntries, BlockBuilderStatus blockBuilderStatus)
    {
        return new VariantBlockBuilder(
                blockBuilderStatus,
                (VariableWidthBlockBuilder) metadataBlockBuilder.newBlockBuilderLike(blockBuilderStatus),
                (VariableWidthBlockBuilder) valuesBlockBuilder.newBlockBuilderLike(blockBuilderStatus),
                new boolean[expectedEntries]);
    }
}
