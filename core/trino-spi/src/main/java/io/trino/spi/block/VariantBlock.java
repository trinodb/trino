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
import io.trino.spi.variant.Header;
import io.trino.spi.variant.Metadata;
import io.trino.spi.variant.Variant;
import jakarta.annotation.Nullable;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.ObjLongConsumer;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.block.BlockUtil.checkArrayRange;
import static io.trino.spi.block.BlockUtil.checkReadablePosition;
import static io.trino.spi.block.BlockUtil.checkValidRegion;
import static io.trino.spi.block.BlockUtil.compactArray;
import static java.util.Objects.requireNonNull;

public final class VariantBlock
        implements ValueBlock
{
    private static final int INSTANCE_SIZE = instanceSize(VariantBlock.class);

    private final int startOffset;
    private final int positionCount;
    @Nullable
    private final boolean[] isNull;
    /**
     * Metadata and value blocks have the same position count as this variant block. The field value of a null variant must be null.
     */
    private final Block metadata;
    private final Block values;

    private volatile long sizeInBytes = -1;
    private volatile long retainedSizeInBytes = -1;

    /**
     * Creates a variant block directly from metadata and value blocks.
     */
    public static VariantBlock create(int positionCount, Block metadata, Block values, Optional<boolean[]> isNullOptional)
    {
        // verify that field values for null variants are null
        if (isNullOptional.isPresent()) {
            boolean[] isNull = isNullOptional.get();
            checkArrayRange(isNull, 0, positionCount);
            verifyPositionsAreNull(metadata, isNull, positionCount, "Metadata");
            verifyPositionsAreNull(values, isNull, positionCount, "Metadata");
        }

        return createInternal(0, positionCount, isNullOptional.orElse(null), metadata, values);
    }

    private static void verifyPositionsAreNull(Block block, boolean[] isNull, int positionCount, String name)
    {
        for (int position = 0; position < positionCount; position++) {
            if (isNull[position] && !block.isNull(position)) {
                throw new IllegalArgumentException("%s for null variant must be null: position %d".formatted(name, position));
            }
        }
    }

    static VariantBlock createInternal(int startOffset, int positionCount, @Nullable boolean[] isNull, Block metadata, Block values)
    {
        return new VariantBlock(startOffset, positionCount, isNull, metadata, values);
    }

    /**
     * Use createInternal or fromMetadataValuesBlocks instead of this method. The caller of this method is assumed to have
     * validated the arguments with validateConstructorArguments.
     */
    private VariantBlock(int startOffset, int positionCount, @Nullable boolean[] isNull, Block metadata, Block values)
    {
        if (startOffset < 0) {
            throw new IllegalArgumentException("startOffset is negative");
        }

        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }

        if (isNull != null && isNull.length - startOffset < positionCount) {
            throw new IllegalArgumentException("isNull length is less than positionCount");
        }

        requireNonNull(metadata, "metadata is null");
        requireNonNull(values, "values is null");
        if (metadata.getPositionCount() != values.getPositionCount()) {
            throw new IllegalArgumentException("metadata and values blocks must have the same position count");
        }
        if (metadata.getPositionCount() - startOffset < positionCount) {
            throw new IllegalArgumentException("fieldBlock length is less than positionCount");
        }

        this.startOffset = startOffset;
        this.positionCount = positionCount;
        this.isNull = positionCount == 0 ? null : isNull;
        this.metadata = metadata;
        this.values = values;
    }

    public Block getMetadata()
    {
        if ((startOffset == 0) && (metadata.getPositionCount() == positionCount)) {
            return metadata;
        }
        return metadata.getRegion(startOffset, positionCount);
    }

    public Block getValues()
    {
        if ((startOffset == 0) && (values.getPositionCount() == positionCount)) {
            return values;
        }
        return values.getRegion(startOffset, positionCount);
    }

    public Header.BasicType getBasicType(int position)
    {
        VariableWidthBlock variableWidthBlock = (VariableWidthBlock) values.getUnderlyingValueBlock();
        position = values.getUnderlyingValuePosition(position);

        Slice rawSlice = variableWidthBlock.getRawSlice();
        int rawSliceOffset = variableWidthBlock.getRawSliceOffset(values.getUnderlyingValuePosition(position));
        return Header.getBasicType(rawSlice.getByte(rawSliceOffset));
    }

    public int getRawOffset()
    {
        return startOffset;
    }

    public Block getRawMetadata()
    {
        return metadata;
    }

    public Block getRawValues()
    {
        return values;
    }

    public int getOffsetBase()
    {
        return startOffset;
    }

    @Override
    public boolean mayHaveNull()
    {
        return isNull != null;
    }

    @Override
    public boolean hasNull()
    {
        if (isNull == null) {
            return false;
        }
        for (int i = 0; i < positionCount; i++) {
            if (isNull[startOffset + i]) {
                return true;
            }
        }
        return false;
    }

    @Nullable
    public boolean[] getRawIsNull()
    {
        return isNull;
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getSizeInBytes()
    {
        if (sizeInBytes >= 0) {
            return sizeInBytes;
        }

        long sizeInBytes = Byte.BYTES * (long) positionCount;
        sizeInBytes += metadata.getRegionSizeInBytes(startOffset, positionCount);
        sizeInBytes += values.getRegionSizeInBytes(startOffset, positionCount);
        this.sizeInBytes = sizeInBytes;
        return sizeInBytes;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        long retainedSizeInBytes = this.retainedSizeInBytes;
        if (retainedSizeInBytes < 0) {
            retainedSizeInBytes = INSTANCE_SIZE + sizeOf(isNull);
            retainedSizeInBytes += metadata.getRetainedSizeInBytes();
            retainedSizeInBytes += values.getRetainedSizeInBytes();
            this.retainedSizeInBytes = retainedSizeInBytes;
        }
        return retainedSizeInBytes;
    }

    @Override
    public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer)
    {
        consumer.accept(metadata, metadata.getRetainedSizeInBytes());
        consumer.accept(values, values.getRetainedSizeInBytes());
        if (isNull != null) {
            consumer.accept(isNull, sizeOf(isNull));
        }
        consumer.accept(this, INSTANCE_SIZE);
    }

    @Override
    public String toString()
    {
        return "VariantBlock{startOffset=%d, positionCount=%d}".formatted(startOffset, positionCount);
    }

    @Override
    public VariantBlock copyWithAppendedNull()
    {
        boolean[] newIsNull;
        if (isNull != null) {
            newIsNull = Arrays.copyOf(isNull, startOffset + positionCount + 1);
        }
        else {
            newIsNull = new boolean[startOffset + positionCount + 1];
        }
        // mark the (new) last element as null
        newIsNull[startOffset + positionCount] = true;

        Block newMetadata = metadata.copyWithAppendedNull();
        Block newValues = values.copyWithAppendedNull();
        return new VariantBlock(startOffset, positionCount + 1, newIsNull, newMetadata, newValues);
    }

    @Override
    public VariantBlock copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        Block newMetadata = copyBlockPositions(positions, offset, length, metadata, startOffset, positionCount);
        Block newValues = copyBlockPositions(positions, offset, length, values, startOffset, positionCount);

        boolean[] newIsNull = null;
        if (isNull != null) {
            boolean hasNull = false;
            newIsNull = new boolean[length];
            for (int i = 0; i < length; i++) {
                boolean isNull = this.isNull[startOffset + positions[offset + i]];
                newIsNull[i] = isNull;
                hasNull |= isNull;
            }
            if (!hasNull) {
                newIsNull = null;
            }
        }

        return new VariantBlock(0, length, newIsNull, newMetadata, newValues);
    }

    private static Block copyBlockPositions(int[] positions, int offset, int length, Block block, int blockOffset, int blockLength)
    {
        // If the variant block has a non-zero starting offset, we have to create a temporary block starting
        // from the correct offset before copying positions
        if (blockOffset != 0) {
            block = block.getRegion(blockOffset, blockLength);
        }
        return block.copyPositions(positions, offset, length);
    }

    @Override
    public VariantBlock getRegion(int positionOffset, int length)
    {
        checkValidRegion(positionCount, positionOffset, length);

        return new VariantBlock(startOffset + positionOffset, length, isNull, metadata, values);
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        checkValidRegion(positionCount, position, length);

        long regionSizeInBytes = Byte.BYTES * (long) length;
        regionSizeInBytes += metadata.getRegionSizeInBytes(startOffset + position, length);
        regionSizeInBytes += values.getRegionSizeInBytes(startOffset + position, length);
        return regionSizeInBytes;
    }

    @Override
    public VariantBlock copyRegion(int positionOffset, int length)
    {
        checkValidRegion(positionCount, positionOffset, length);

        Block newMetadata = metadata.copyRegion(startOffset + positionOffset, length);
        Block newValues = values.copyRegion(startOffset + positionOffset, length);

        boolean[] newIsNull = isNull == null ? null : compactArray(isNull, startOffset + positionOffset, length);
        if (startOffset == 0 && newIsNull == isNull && metadata == newMetadata && values == newValues) {
            return this;
        }
        return new VariantBlock(0, length, newIsNull, newMetadata, newValues);
    }

    public Variant getVariant(int position)
    {
        checkReadablePosition(this, position);
        if (isNull(position)) {
            throw new IllegalStateException("Position is null");
        }
        return Variant.from(Metadata.from(getSlice(metadata, startOffset + position)), getSlice(values, startOffset + position));
    }

    private static Slice getSlice(Block block, int position)
    {
        VariableWidthBlock valueBlock = (VariableWidthBlock) block.getUnderlyingValueBlock();
        int valuePosition = block.getUnderlyingValuePosition(position);
        return valueBlock.getSlice(valuePosition);
    }

    @Override
    public VariantBlock getSingleValueBlock(int position)
    {
        checkReadablePosition(this, position);

        Block newMetadata = metadata.getSingleValueBlock(startOffset + position);
        Block newValues = values.getSingleValueBlock(startOffset + position);
        boolean[] newIsNull = isNull(position) ? new boolean[] {true} : null;
        return new VariantBlock(0, 1, newIsNull, newMetadata, newValues);
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        checkReadablePosition(this, position);

        if (isNull(position)) {
            return 0;
        }

        return metadata.getEstimatedDataSizeForStats(startOffset + position) +
                values.getEstimatedDataSizeForStats(startOffset + position);
    }

    @Override
    public boolean isNull(int position)
    {
        if (!mayHaveNull()) {
            return false;
        }
        checkReadablePosition(this, position);
        return isNull[startOffset + position];
    }

    public record VariantNestedBlocks(Block metadataBlock, Block valueBlock)
    {
        public VariantNestedBlocks
        {
            requireNonNull(metadataBlock, "metadataBlock is null");
            requireNonNull(valueBlock, "valueBlock is null");
        }
    }

    /**
     * Returns the nested variant fields from the specified block. The block maybe a RunLengthEncodedBlock, or
     * DictionaryBlock, but the underlying block must be a VariantBlock. The returned nested blocks will be the same
     * length as the specified block, which means they are not null suppressed.
     */
    // this code was copied from RowBlock
    public static VariantNestedBlocks getNestedFields(Block block)
    {
        if (block instanceof RunLengthEncodedBlock runLengthEncodedBlock) {
            VariantBlock variantBlock = (VariantBlock) runLengthEncodedBlock.getValue();
            return new VariantNestedBlocks(
                    RunLengthEncodedBlock.create(variantBlock.getMetadata(), runLengthEncodedBlock.getPositionCount()),
                    RunLengthEncodedBlock.create(variantBlock.getValues(), runLengthEncodedBlock.getPositionCount()));
        }
        if (block instanceof DictionaryBlock dictionaryBlock) {
            VariantBlock variantBlock = (VariantBlock) dictionaryBlock.getDictionary();
            return new VariantNestedBlocks(
                    dictionaryBlock.createProjection(variantBlock.getMetadata()),
                    dictionaryBlock.createProjection(variantBlock.getValues()));
        }
        if (block instanceof VariantBlock variantBlock) {
            return new VariantNestedBlocks(variantBlock.getMetadata(), variantBlock.getValues());
        }
        throw new IllegalArgumentException("Unexpected block type: " + block.getClass().getSimpleName());
    }

    /**
     * Returns the nested variant fields from the specified block with null variants suppressed. The block maybe a RunLengthEncodedBlock, or
     * DictionaryBlock, but the underlying block must be a VariantBlock. The returned nested blocks will not be the same
     * length as the specified block if it contains null variants.
     */
    // this code was copied from RowBlock
    public static VariantNestedBlocks getNullSuppressedNestedFields(Block block)
    {
        if (!block.mayHaveNull()) {
            return getNestedFields(block);
        }

        return switch (block) {
            case RunLengthEncodedBlock runLengthEncodedBlock -> {
                VariantBlock variantBlock = (VariantBlock) runLengthEncodedBlock.getValue();
                if (!variantBlock.isNull(0)) {
                    throw new IllegalStateException("Expected run length encoded block value to be null");
                }
                // all values are null, so return a zero-length block of the correct type
                yield new VariantNestedBlocks(
                        variantBlock.getMetadata().getRegion(0, 0),
                        variantBlock.getValues().getRegion(0, 0));
            }
            case DictionaryBlock dictionaryBlock -> {
                int[] newIds = new int[dictionaryBlock.getPositionCount()];
                int idCount = 0;
                for (int position = 0; position < newIds.length; position++) {
                    if (!dictionaryBlock.isNull(position)) {
                        newIds[idCount] = dictionaryBlock.getId(position);
                        idCount++;
                    }
                }
                int nonNullPositionCount = idCount;
                VariantBlock variantBlock = (VariantBlock) dictionaryBlock.getDictionary();
                yield new VariantNestedBlocks(
                        DictionaryBlock.create(nonNullPositionCount, variantBlock.getMetadata(), newIds),
                        DictionaryBlock.create(nonNullPositionCount, variantBlock.getValues(), newIds));
            }
            case VariantBlock variantBlock -> {
                int[] nonNullPositions = new int[variantBlock.getPositionCount()];
                int idCount = 0;
                for (int position = 0; position < nonNullPositions.length; position++) {
                    if (!variantBlock.isNull(position)) {
                        nonNullPositions[idCount] = position;
                        idCount++;
                    }
                }
                int nonNullPositionCount = idCount;
                yield new VariantNestedBlocks(
                        DictionaryBlock.create(nonNullPositionCount, variantBlock.getMetadata(), nonNullPositions),
                        DictionaryBlock.create(nonNullPositionCount, variantBlock.getValues(), nonNullPositions));
            }
            default -> throw new IllegalArgumentException("Unexpected block type: " + block.getClass().getSimpleName());
        };
    }

    @Override
    public VariantBlock getUnderlyingValueBlock()
    {
        return this;
    }

    @Override
    public Optional<ByteArrayBlock> getNulls()
    {
        return BlockUtil.getNulls(isNull, startOffset, positionCount);
    }
}
