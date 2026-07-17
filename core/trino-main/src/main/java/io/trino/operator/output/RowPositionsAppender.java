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
package io.trino.operator.output;

import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.type.RowType;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import jakarta.annotation.Nullable;

import java.util.Arrays;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.operator.output.PositionsAppenderUtil.calculateBlockResetSize;
import static io.trino.operator.output.PositionsAppenderUtil.calculateNewArraySize;
import static io.trino.spi.block.Bitmap.allocateWords;
import static io.trino.spi.block.Bitmap.clear;
import static io.trino.spi.block.Bitmap.clearBits;
import static io.trino.spi.block.Bitmap.copyBits;
import static io.trino.spi.block.Bitmap.hasSetBit;
import static io.trino.spi.block.Bitmap.hasUnsetBit;
import static io.trino.spi.block.Bitmap.set;
import static io.trino.spi.block.Bitmap.setBits;
import static io.trino.spi.block.Bitmap.wordsForBits;
import static io.trino.spi.block.RowBlock.fromNotNullSuppressedFieldBlocks;
import static java.util.Objects.requireNonNull;

public final class RowPositionsAppender
        implements PositionsAppender
{
    private static final int INSTANCE_SIZE = instanceSize(RowPositionsAppender.class);
    private final RowType type;
    private final UnnestingPositionsAppender[] fieldAppenders;
    private int initialEntryCount;
    private boolean initialized;

    private int positionCount;
    private boolean hasNullRow;
    private boolean hasNonNullRow;
    private int positionCapacity;
    @Nullable
    private long[] valueIsValid;
    private long retainedSizeInBytes = -1;
    private long sizeInBytes = -1;

    public static RowPositionsAppender createRowAppender(
            PositionsAppenderFactory positionsAppenderFactory,
            RowType type,
            int expectedPositions,
            long maxPageSizeInBytes)
    {
        UnnestingPositionsAppender[] fields = new UnnestingPositionsAppender[type.getFields().size()];
        for (int i = 0; i < fields.length; i++) {
            fields[i] = positionsAppenderFactory.create(type.getFields().get(i).getType(), expectedPositions, maxPageSizeInBytes);
        }
        return new RowPositionsAppender(type, fields, expectedPositions);
    }

    private RowPositionsAppender(RowType type, UnnestingPositionsAppender[] fieldAppenders, int expectedPositions)
    {
        this.type = type;
        this.fieldAppenders = requireNonNull(fieldAppenders, "fields is null");
        this.initialEntryCount = expectedPositions;
        resetSize();
    }

    @Override
    public void append(IntArrayList positions, ValueBlock block)
    {
        checkArgument(block instanceof RowBlock, "Block must be instance of %s", RowBlock.class);

        if (positions.isEmpty()) {
            return;
        }
        ensureCapacity(positions.size());
        RowBlock sourceRowBlock = (RowBlock) block;
        int startOffset = sourceRowBlock.getOffsetBase();

        Block[] rawFieldBlocks = sourceRowBlock.getRawFieldBlocks();

        if (startOffset == 0) {
            for (int i = 0; i < fieldAppenders.length; i++) {
                fieldAppenders[i].append(positions, rawFieldBlocks[i]);
            }
        }
        else {
            int[] adjustedPositions = new int[positions.size()];
            for (int i = 0; i < positions.size(); i++) {
                adjustedPositions[i] = startOffset + positions.getInt(i);
            }
            IntArrayList adjustedPositionsList = IntArrayList.wrap(adjustedPositions);

            for (int i = 0; i < fieldAppenders.length; i++) {
                fieldAppenders[i].append(adjustedPositionsList, rawFieldBlocks[i]);
            }
        }

        long[] rawValueIsValid = sourceRowBlock.getRawValueIsValid();
        int[] rawPositions = positions.elements();
        if (rawValueIsValid == null || !hasUnsetBit(rawValueIsValid, startOffset, rawPositions, 0, positions.size())) {
            if (valueIsValid != null) {
                setBits(valueIsValid, 0, positionCount, positions.size());
            }
            hasNonNullRow = true;
        }
        else {
            initializeValidityForFirstNull();
            copyBits(rawValueIsValid, startOffset, rawPositions, 0, valueIsValid, positionCount, positions.size());
            hasNullRow = true;
            hasNonNullRow |= hasSetBit(rawValueIsValid, startOffset, rawPositions, 0, positions.size());
        }

        positionCount += positions.size();
        resetSize();
    }

    @Override
    public void appendRange(ValueBlock block, int offset, int length)
    {
        checkArgument(block instanceof RowBlock, "Block must be instance of %s", RowBlock.class);
        if (length == 0) {
            return;
        }

        RowBlock sourceRowBlock = (RowBlock) block;
        ensureCapacity(length);

        Block[] rawFieldBlocks = sourceRowBlock.getRawFieldBlocks();
        int startOffset = sourceRowBlock.getOffsetBase();

        for (int i = 0; i < fieldAppenders.length; i++) {
            fieldAppenders[i].appendRange(rawFieldBlocks[i], startOffset + offset, length);
        }

        long[] rawValueIsValid = sourceRowBlock.getRawValueIsValid();
        if (rawValueIsValid == null || !hasUnsetBit(rawValueIsValid, startOffset + offset, length)) {
            if (valueIsValid != null) {
                setBits(valueIsValid, 0, positionCount, length);
            }
            hasNonNullRow = true;
        }
        else {
            initializeValidityForFirstNull();
            copyBits(rawValueIsValid, startOffset + offset, valueIsValid, positionCount, length);
            hasNullRow = true;
            hasNonNullRow |= hasSetBit(rawValueIsValid, startOffset + offset, length);
        }

        positionCount += length;
        resetSize();
    }

    @Override
    public void appendRle(ValueBlock value, int rlePositionCount)
    {
        checkArgument(value instanceof RowBlock, "Block must be instance of %s", RowBlock.class);

        ensureCapacity(rlePositionCount);
        RowBlock sourceRowBlock = (RowBlock) value;

        Block[] rawFieldBlocks = sourceRowBlock.getRawFieldBlocks();
        int startOffset = sourceRowBlock.getOffsetBase();

        for (int i = 0; i < fieldAppenders.length; i++) {
            fieldAppenders[i].appendRle(rawFieldBlocks[i].getSingleValueBlock(startOffset), rlePositionCount);
        }

        if (sourceRowBlock.isNull(0)) {
            // append rlePositionCount nulls
            initializeValidityForFirstNull();
            clearBits(valueIsValid, 0, positionCount, rlePositionCount);
            hasNullRow = true;
        }
        else {
            // append not null row value
            if (valueIsValid != null) {
                setBits(valueIsValid, 0, positionCount, rlePositionCount);
            }
            hasNonNullRow = true;
        }
        positionCount += rlePositionCount;
        resetSize();
    }

    @Override
    public void append(int position, ValueBlock value)
    {
        checkArgument(value instanceof RowBlock, "Block must be instance of %s", RowBlock.class);

        ensureCapacity(1);
        RowBlock sourceRowBlock = (RowBlock) value;

        Block[] rawFieldBlocks = sourceRowBlock.getRawFieldBlocks();
        int startOffset = sourceRowBlock.getOffsetBase();

        for (int i = 0; i < fieldAppenders.length; i++) {
            fieldAppenders[i].append(startOffset + position, rawFieldBlocks[i]);
        }

        if (sourceRowBlock.isNull(position)) {
            initializeValidityForFirstNull();
            clear(valueIsValid, 0, positionCount);
            hasNullRow = true;
        }
        else {
            // append not null row value
            if (valueIsValid != null) {
                set(valueIsValid, 0, positionCount);
            }
            hasNonNullRow = true;
        }
        positionCount++;
        resetSize();
    }

    @Override
    public Block build()
    {
        Block result;
        if (hasNonNullRow) {
            Block[] fieldBlocks = new Block[fieldAppenders.length];
            for (int i = 0; i < fieldAppenders.length; i++) {
                fieldBlocks[i] = fieldAppenders[i].build();
            }
            result = fromNotNullSuppressedFieldBlocks(positionCount, hasNullRow ? Optional.of(valueIsValid) : Optional.empty(), fieldBlocks);
        }
        else if (hasNullRow) {
            Block nullRowBlock = type.createNullBlock();
            result = RunLengthEncodedBlock.create(nullRowBlock, positionCount);
        }
        else {
            result = type.createBlockBuilder(null, 0).build();
        }

        reset();
        return result;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        if (retainedSizeInBytes != -1) {
            return retainedSizeInBytes;
        }

        long size = INSTANCE_SIZE + sizeOf(valueIsValid);
        for (UnnestingPositionsAppender field : fieldAppenders) {
            size += field.getRetainedSizeInBytes();
        }

        retainedSizeInBytes = size;
        return size;
    }

    @Override
    public long getSizeInBytes()
    {
        if (sizeInBytes != -1) {
            return sizeInBytes;
        }

        long size = (Integer.BYTES + Byte.BYTES) * (long) positionCount;
        for (UnnestingPositionsAppender field : fieldAppenders) {
            size += field.getSizeInBytes();
        }

        sizeInBytes = size;
        return sizeInBytes;
    }

    @Override
    public void reset()
    {
        for (UnnestingPositionsAppender field : fieldAppenders) {
            field.reset();
        }
        initialEntryCount = calculateBlockResetSize(positionCount);
        initialized = false;
        positionCapacity = 0;
        valueIsValid = null;
        positionCount = 0;
        hasNonNullRow = false;
        hasNullRow = false;
        resetSize();
    }

    private void ensureCapacity(int additionalCapacity)
    {
        if (positionCapacity <= positionCount + additionalCapacity) {
            int newSize;
            if (initialized) {
                newSize = calculateNewArraySize(positionCapacity);
            }
            else {
                newSize = initialEntryCount;
                initialized = true;
            }

            int newCapacity = Math.max(newSize, positionCount + additionalCapacity);
            positionCapacity = newCapacity;
            if (valueIsValid != null) {
                valueIsValid = Arrays.copyOf(valueIsValid, wordsForBits(newCapacity));
            }
            resetSize();
        }
    }

    private boolean initializeValidityForFirstNull()
    {
        if (valueIsValid != null) {
            return false;
        }
        valueIsValid = allocateWords(positionCapacity, false);
        setBits(valueIsValid, 0, 0, positionCount);
        resetSize();
        return true;
    }

    private void resetSize()
    {
        sizeInBytes = -1;
        retainedSizeInBytes = -1;
    }
}
