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
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.block.BlockUtil;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ShortArrayBlock;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Optional;

import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.block.BlockUtil.calculateBlockResetSize;
import static java.lang.Math.max;

public class ShortPositionsAppender
        implements BlockTypeAwarePositionsAppender
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ShortPositionsAppender.class).instanceSize();
    private static final Block NULL_VALUE_BLOCK = new ShortArrayBlock(1, Optional.of(new boolean[] {true}), new short[1]);

    @Nullable
    private final BlockBuilderStatus blockBuilderStatus;
    private boolean initialized;
    private final int initialEntryCount;

    private int positionCount;
    private boolean hasNullValue;
    private boolean hasNonNullValue;

    // it is assumed that these arrays are the same length
    private boolean[] valueIsNull = new boolean[0];
    private short[] values = new short[0];

    private long retainedSizeInBytes;

    public ShortPositionsAppender(@Nullable BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        this.blockBuilderStatus = blockBuilderStatus;
        this.initialEntryCount = max(expectedEntries, 1);

        updateDataSize();
    }

    @Override
    public void append(IntArrayList positions, Block block)
    {
        int[] positionArray = positions.elements();
        int newPositionCount = positions.size();
        ensureCapacity(positionCount + newPositionCount);

        if (block.mayHaveNull()) {
            for (int i = 0; i < newPositionCount; i++) {
                int position = positionArray[i];
                boolean isNull = block.isNull(position);
                int positionIndex = positionCount + i;
                if (isNull) {
                    valueIsNull[positionIndex] = true;
                    hasNullValue = true;
                }
                else {
                    values[positionIndex] = block.getShort(position, 0);
                    hasNonNullValue = true;
                }
            }
            this.positionCount += newPositionCount;
        }
        else {
            for (int i = 0; i < newPositionCount; i++) {
                int position = positionArray[i];
                values[positionCount + i] = block.getShort(position, 0);
            }
            positionCount += newPositionCount;
            this.hasNonNullValue = true;
        }

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(ShortArrayBlock.SIZE_IN_BYTES_PER_POSITION * newPositionCount);
        }
    }

    @Override
    public void appendDictionary(IntArrayList positions, DictionaryBlock block)
    {
        int[] positionArray = positions.elements();
        int newPositionCount = positions.size();
        ensureCapacity(positionCount + newPositionCount);
        if (block.mayHaveNull()) {
            for (int i = 0; i < newPositionCount; i++) {
                int position = positionArray[i];
                if (block.isNull(position)) {
                    valueIsNull[positionCount] = true;
                    hasNullValue = true;
                }
                else {
                    values[positionCount] = block.getShort(position, 0);
                    hasNonNullValue = true;
                }
                positionCount++;
            }
        }
        else {
            for (int i = 0; i < newPositionCount; i++) {
                int position = positionArray[i];
                values[positionCount] = block.getShort(position, 0);
                positionCount++;
            }
            hasNonNullValue = true;
        }

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(ShortArrayBlock.SIZE_IN_BYTES_PER_POSITION * newPositionCount);
        }
    }

    @Override
    public void appendRle(RunLengthEncodedBlock block)
    {
        int rlePositionCount = block.getPositionCount();
        int sourcePosition = 0;
        ensureCapacity(positionCount + rlePositionCount);
        if (block.isNull(sourcePosition)) {
            Arrays.fill(valueIsNull, positionCount, positionCount + rlePositionCount, true);
            hasNullValue = true;
        }
        else {
            short value = block.getShort(sourcePosition, 0);
            Arrays.fill(values, positionCount, positionCount + rlePositionCount, value);
            hasNonNullValue = true;
        }
        positionCount += rlePositionCount;

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(ShortArrayBlock.SIZE_IN_BYTES_PER_POSITION * rlePositionCount);
        }
    }

    @Override
    public void appendRow(Block source, int position)
    {
        if (source.isNull(position)) {
            appendNull();
        }
        else {
            writeShort(source.getShort(position, 0));
        }
    }

    private void appendNull()
    {
        ensureCapacity(positionCount + 1);

        valueIsNull[positionCount] = true;

        hasNullValue = true;
        positionCount++;
        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(ShortArrayBlock.SIZE_IN_BYTES_PER_POSITION);
        }
    }

    private void writeShort(short value)
    {
        ensureCapacity(positionCount + 1);

        values[positionCount] = value;

        hasNonNullValue = true;
        positionCount++;
        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(ShortArrayBlock.SIZE_IN_BYTES_PER_POSITION);
        }
    }

    @Override
    public Block build()
    {
        if (!hasNonNullValue) {
            return new RunLengthEncodedBlock(NULL_VALUE_BLOCK, positionCount);
        }
        return new ShortArrayBlock(positionCount, hasNullValue ? Optional.of(valueIsNull) : Optional.empty(), values);
    }

    @Override
    public BlockTypeAwarePositionsAppender newStateLike(@Nullable BlockBuilderStatus blockBuilderStatus)
    {
        return new ShortPositionsAppender(blockBuilderStatus, calculateBlockResetSize(positionCount));
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    private void ensureCapacity(int capacity)
    {
        if (values.length >= capacity) {
            return;
        }

        int newSize;
        if (initialized) {
            newSize = BlockUtil.calculateNewArraySize(values.length);
        }
        else {
            newSize = initialEntryCount;
            initialized = true;
        }
        newSize = Math.max(newSize, capacity);

        valueIsNull = Arrays.copyOf(valueIsNull, newSize);
        values = Arrays.copyOf(values, newSize);
        updateDataSize();
    }

    private void updateDataSize()
    {
        retainedSizeInBytes = INSTANCE_SIZE + sizeOf(valueIsNull) + sizeOf(values);
        if (blockBuilderStatus != null) {
            retainedSizeInBytes += BlockBuilderStatus.INSTANCE_SIZE;
        }
    }
}
