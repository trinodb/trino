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

import io.trino.spi.block.AbstractRowBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.RowType;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.operator.output.PositionsAppenderUtil.calculateBlockResetSize;
import static io.trino.operator.output.PositionsAppenderUtil.calculateNewArraySize;
import static io.trino.spi.block.RowBlock.fromFieldBlocks;
import static java.util.Objects.requireNonNull;

public class RowPositionsAppender
        implements PositionsAppender
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(RowPositionsAppender.class).instanceSize();
    private final PositionsAppender[] fieldAppenders;
    private int initialEntryCount;
    private boolean initialized;

    private int positionCount;
    private boolean hasNullRow;
    private boolean hasNonNullRow;
    private boolean[] rowIsNull = new boolean[0];
    private long retainedSizeInBytes;
    private long sizeInBytes;

    public static RowPositionsAppender createRowAppender(
            PositionsAppenderFactory positionsAppenderFactory,
            RowType type,
            int expectedPositions,
            long maxPageSizeInBytes)
    {
        PositionsAppender[] fields = new PositionsAppender[type.getFields().size()];
        for (int i = 0; i < fields.length; i++) {
            fields[i] = positionsAppenderFactory.create(type.getFields().get(i).getType(), expectedPositions, maxPageSizeInBytes);
        }
        return new RowPositionsAppender(fields, expectedPositions);
    }

    private RowPositionsAppender(PositionsAppender[] fieldAppenders, int expectedPositions)
    {
        this.fieldAppenders = requireNonNull(fieldAppenders, "fields is null");
        this.initialEntryCount = expectedPositions;
        updateRetainedSize();
    }

    @Override
    // TODO: Make PositionsAppender work performant with different block types (https://github.com/trinodb/trino/issues/13267)
    public void append(IntArrayList positions, Block block)
    {
        if (positions.isEmpty()) {
            return;
        }
        ensureCapacity(positions.size());
        AbstractRowBlock sourceRowBlock = (AbstractRowBlock) block;
        IntArrayList nonNullPositions;
        if (sourceRowBlock.mayHaveNull()) {
            nonNullPositions = processNullablePositions(positions, sourceRowBlock);
            hasNullRow |= nonNullPositions.size() < positions.size();
            hasNonNullRow |= nonNullPositions.size() > 0;
        }
        else {
            // the source Block does not have nulls
            nonNullPositions = processNonNullablePositions(positions, sourceRowBlock);
            hasNonNullRow = true;
        }

        List<Block> fieldBlocks = sourceRowBlock.getChildren();
        for (int i = 0; i < fieldAppenders.length; i++) {
            fieldAppenders[i].append(nonNullPositions, fieldBlocks.get(i));
        }

        positionCount += positions.size();
        updateSize();
    }

    @Override
    public void appendRle(RunLengthEncodedBlock rleBlock)
    {
        int rlePositionCount = rleBlock.getPositionCount();
        ensureCapacity(rlePositionCount);
        AbstractRowBlock sourceRowBlock = (AbstractRowBlock) rleBlock.getValue();
        if (sourceRowBlock.isNull(0)) {
            // append rlePositionCount nulls
            Arrays.fill(rowIsNull, positionCount, positionCount + rlePositionCount, true);
            hasNullRow = true;
        }
        else {
            // append not null row value
            List<Block> fieldBlocks = sourceRowBlock.getChildren();
            int fieldPosition = sourceRowBlock.getFieldBlockOffset(0);
            for (int i = 0; i < fieldAppenders.length; i++) {
                fieldAppenders[i].appendRle(new RunLengthEncodedBlock(fieldBlocks.get(i).getSingleValueBlock(fieldPosition), rlePositionCount));
            }
            hasNonNullRow = true;
        }
        positionCount += rlePositionCount;
        updateSize();
    }

    @Override
    public Block build()
    {
        Block[] fieldBlocks = new Block[fieldAppenders.length];
        for (int i = 0; i < fieldAppenders.length; i++) {
            fieldBlocks[i] = fieldAppenders[i].build();
        }
        Block result;
        if (hasNonNullRow) {
            result = fromFieldBlocks(positionCount, hasNullRow ? Optional.of(rowIsNull) : Optional.empty(), fieldBlocks);
        }
        else {
            Block nullRowBlock = fromFieldBlocks(1, Optional.of(new boolean[] {true}), fieldBlocks);
            result = new RunLengthEncodedBlock(nullRowBlock, positionCount);
        }

        reset();
        return result;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        long size = retainedSizeInBytes;
        for (PositionsAppender field : fieldAppenders) {
            size += field.getRetainedSizeInBytes();
        }
        return size;
    }

    @Override
    public long getSizeInBytes()
    {
        return sizeInBytes;
    }

    private void reset()
    {
        initialEntryCount = calculateBlockResetSize(positionCount);
        initialized = false;
        rowIsNull = new boolean[0];
        positionCount = 0;
        sizeInBytes = 0;
        hasNonNullRow = false;
        hasNullRow = false;
        updateRetainedSize();
    }

    private IntArrayList processNullablePositions(IntArrayList positions, AbstractRowBlock sourceRowBlock)
    {
        int[] nonNullPositions = new int[positions.size()];
        int nonNullPositionsCount = 0;

        for (int i = 0; i < positions.size(); i++) {
            int position = positions.getInt(i);
            boolean positionIsNull = sourceRowBlock.isNull(position);
            nonNullPositions[nonNullPositionsCount] = sourceRowBlock.getFieldBlockOffset(position);
            nonNullPositionsCount += positionIsNull ? 0 : 1;
            rowIsNull[positionCount + i] = positionIsNull;
        }

        return IntArrayList.wrap(nonNullPositions, nonNullPositionsCount);
    }

    private IntArrayList processNonNullablePositions(IntArrayList positions, AbstractRowBlock sourceRowBlock)
    {
        int[] nonNullPositions = new int[positions.size()];
        for (int i = 0; i < positions.size(); i++) {
            nonNullPositions[i] = sourceRowBlock.getFieldBlockOffset(positions.getInt(i));
        }
        return IntArrayList.wrap(nonNullPositions);
    }

    private void ensureCapacity(int additionalCapacity)
    {
        if (rowIsNull.length <= positionCount + additionalCapacity) {
            int newSize;
            if (initialized) {
                newSize = calculateNewArraySize(rowIsNull.length);
            }
            else {
                newSize = initialEntryCount;
                initialized = true;
            }

            int newCapacity = Math.max(newSize, positionCount + additionalCapacity);
            rowIsNull = Arrays.copyOf(rowIsNull, newCapacity);
            updateRetainedSize();
        }
    }

    private void updateSize()
    {
        long size = (Integer.BYTES + Byte.BYTES) * (long) positionCount;
        for (PositionsAppender field : fieldAppenders) {
            size += field.getSizeInBytes();
        }
        sizeInBytes = size;
    }

    private void updateRetainedSize()
    {
        retainedSizeInBytes = INSTANCE_SIZE + sizeOf(rowIsNull);
    }
}
