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

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.block.Bitmap.clear;
import static io.trino.spi.block.Bitmap.clearBits;
import static io.trino.spi.block.Bitmap.copyBits;
import static io.trino.spi.block.Bitmap.hasSetBit;
import static io.trino.spi.block.Bitmap.hasUnsetBit;
import static io.trino.spi.block.Bitmap.isSet;
import static io.trino.spi.block.Bitmap.set;
import static io.trino.spi.block.Bitmap.setBits;
import static java.util.Objects.checkIndex;
import static java.util.Objects.requireNonNull;

public class VariantBlockBuilder
        implements BlockBuilder
{
    private static final int INSTANCE_SIZE = instanceSize(VariantBlockBuilder.class);
    private static final int VARIANT_ENTRY_SIZE = Integer.BYTES + Byte.BYTES;
    private static final VariantBlock NULL_VALUE_BLOCK = VariantBlock.createInternal(
            0,
            1,
            new long[] {0},
            VariableWidthBlockBuilder.NULL_VALUE_BLOCK,
            VariableWidthBlockBuilder.NULL_VALUE_BLOCK);

    @Nullable
    private final BlockBuilderStatus blockBuilderStatus;

    private int positionCount;
    private int positionCapacity;
    @Nullable
    private long[] valueIsValid;
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
        this(blockBuilderStatus,
                new VariableWidthBlockBuilder(blockBuilderStatus, expectedEntries, 64),
                new VariableWidthBlockBuilder(blockBuilderStatus, expectedEntries, expectedBytes),
                expectedEntries,
                null);
    }

    private VariantBlockBuilder(
            @Nullable BlockBuilderStatus blockBuilderStatus,
            VariableWidthBlockBuilder metadataBlockBuilder,
            VariableWidthBlockBuilder valuesBlockBuilder,
            int positionCapacity,
            @Nullable long[] valueIsValid)
    {
        this.blockBuilderStatus = blockBuilderStatus;
        this.positionCount = 0;
        this.positionCapacity = positionCapacity;
        this.valueIsValid = valueIsValid;
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
        long sizeInBytes = VARIANT_ENTRY_SIZE * (long) positionCount;
        sizeInBytes += metadataBlockBuilder.getSizeInBytes();
        sizeInBytes += valuesBlockBuilder.getSizeInBytes();
        return sizeInBytes;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        long size = INSTANCE_SIZE + sizeOf(valueIsValid);
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

        long[] rawValueIsValid = variantBlock.getRawValueIsValid();
        if (rawValueIsValid == null || !hasUnsetBit(rawValueIsValid, startOffset + offset, length)) {
            if (valueIsValid != null) {
                setBits(valueIsValid, 0, positionCount, length);
            }
            hasNonNullVariant = true;
        }
        else {
            initializeValidityForFirstNull();
            copyBits(rawValueIsValid, startOffset + offset, valueIsValid, positionCount, length);
            hasNullVariant = true;
            hasNonNullVariant |= hasSetBit(rawValueIsValid, startOffset + offset, length);
        }
        positionCount += length;

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(VARIANT_ENTRY_SIZE * length);
        }
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
            initializeValidityForFirstNull();
            clearBits(valueIsValid, 0, positionCount, count);
            hasNullVariant = true;
        }
        else {
            if (valueIsValid != null) {
                setBits(valueIsValid, 0, positionCount, count);
            }
            hasNonNullVariant = true;
        }

        positionCount += count;

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(VARIANT_ENTRY_SIZE * count);
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

        long[] rawValueIsValid = variantBlock.getRawValueIsValid();
        if (rawValueIsValid == null || !hasUnsetBit(rawValueIsValid, startOffset, positions, offset, length)) {
            if (valueIsValid != null) {
                setBits(valueIsValid, 0, positionCount, length);
            }
            hasNonNullVariant = true;
        }
        else {
            initializeValidityForFirstNull();
            copyBits(rawValueIsValid, startOffset, positions, offset, valueIsValid, positionCount, length);
            hasNullVariant = true;
            hasNonNullVariant |= hasSetBit(rawValueIsValid, startOffset, positions, offset, length);
        }
        positionCount += length;

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(VARIANT_ENTRY_SIZE * length);
        }
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

        if (position == 0) {
            hasNullVariant = false;
            hasNonNullVariant = false;
            return;
        }

        hasNullVariant = false;
        hasNonNullVariant = false;
        for (int index = 0; index < position; index++) {
            boolean isNull = valueIsValid != null && !isSet(valueIsValid, 0, index);
            hasNullVariant |= isNull;
            hasNonNullVariant |= !isNull;
        }
    }

    private void entryAdded(boolean isNull)
    {
        ensureCapacity(positionCount + 1);

        if (isNull) {
            initializeValidityForFirstNull();
            clear(valueIsValid, 0, positionCount);
        }
        else if (valueIsValid != null) {
            set(valueIsValid, 0, positionCount);
        }
        hasNullVariant |= isNull;
        hasNonNullVariant |= !isNull;
        positionCount++;

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(VARIANT_ENTRY_SIZE);
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
        return VariantBlock.createInternal(0, positionCount, hasNullVariant ? valueIsValid : null, metadataBlock, valuesBlock);
    }

    private void ensureCapacity(int capacity)
    {
        if (positionCapacity >= capacity) {
            return;
        }

        int newSize = BlockUtil.calculateNewArraySize(positionCapacity, capacity);
        positionCapacity = newSize;
        if (valueIsValid != null) {
            valueIsValid = Bitmap.ensureCapacity(valueIsValid, newSize);
        }
    }

    private boolean initializeValidityForFirstNull()
    {
        if (valueIsValid != null) {
            return false;
        }
        valueIsValid = Bitmap.allocateWords(positionCapacity, false);
        setBits(valueIsValid, 0, 0, positionCount);
        return true;
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
                expectedEntries,
                null);
    }
}
