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
package io.trino.json;

import io.airlift.slice.DynamicSliceOutput;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ValueBlock;
import jakarta.annotation.Nullable;

import java.util.Arrays;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

/// Builds a [JsonBlock]. Values are appended as bytes into one buffer.
///
/// A value that already holds bytes — everything read from a connector, and any value that has
/// been encoded once — is copied straight in. Only a tree-form value, built by the path engine or
/// a constructor, is encoded here, and it is encoded exactly once.
public final class JsonBlockBuilder
        implements BlockBuilder
{
    private static final int INSTANCE_SIZE = instanceSize(JsonBlockBuilder.class);
    private static final int EXPECTED_BYTES_PER_ENTRY = 64;

    @Nullable
    private final BlockBuilderStatus blockBuilderStatus;
    private final int initialEntryCount;

    private final DynamicSliceOutput output;
    private int[] offsets = new int[1];
    private boolean[] valueIsNull = new boolean[0];
    private int positionCount;
    private boolean hasNullValue;
    private boolean hasNonNullValue;
    private boolean initialized;
    private long retainedSizeInBytes;

    public JsonBlockBuilder(@Nullable BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        this.blockBuilderStatus = blockBuilderStatus;
        this.initialEntryCount = max(expectedEntries, 1);
        this.output = new DynamicSliceOutput(max(expectedEntries, 1) * EXPECTED_BYTES_PER_ENTRY);
        updateRetainedSize();
    }

    /// Appends a value, writing its bytes into the buffer.
    public BlockBuilder appendJson(Json value)
    {
        requireNonNull(value, "value is null; use appendNull for null entries");
        ensureCapacity(positionCount + 1);
        writeValue(value);
        commitEntry(false);
        return this;
    }

    private void writeValue(Json value)
    {
        if (value instanceof EncodedJson encoded) {
            if (encoded.isRawText()) {
                // Raw text passes through untouched: storing it costs a copy, not a parse.
                output.writeBytes(encoded.rawText());
                return;
            }
            // Already bytes — copy them in, behind a version byte so the payload stands alone.
            output.appendByte(JsonItemEncoding.VERSION);
            output.writeBytes(encoded.backingSlice(), encoded.viewOffset(), encoded.viewEnd() - encoded.viewOffset());
            return;
        }
        // A tree: encode straight into the column's buffer, so the value is never materialized as
        // a slice of its own.
        JsonItems.encodeTreeInto(output, value);
    }

    @Override
    public void append(ValueBlock block, int position)
    {
        ensureCapacity(positionCount + 1);
        JsonBlock jsonBlock = (JsonBlock) block;
        if (jsonBlock.isNull(position)) {
            commitEntry(true);
            return;
        }
        output.writeBytes(jsonBlock.getRawSlice(position));
        commitEntry(false);
    }

    @Override
    public void appendRepeated(ValueBlock block, int position, int count)
    {
        for (int i = 0; i < count; i++) {
            append(block, position);
        }
    }

    @Override
    public void appendRange(ValueBlock block, int offset, int length)
    {
        for (int i = 0; i < length; i++) {
            append(block, offset + i);
        }
    }

    @Override
    public void appendPositions(ValueBlock block, int[] positions, int offset, int length)
    {
        for (int i = 0; i < length; i++) {
            append(block, positions[offset + i]);
        }
    }

    @Override
    public BlockBuilder appendNull()
    {
        ensureCapacity(positionCount + 1);
        commitEntry(true);
        return this;
    }

    private void commitEntry(boolean isNull)
    {
        valueIsNull[positionCount] = isNull;
        int entryEnd = output.size();
        if (isNull) {
            hasNullValue = true;
        }
        else {
            hasNonNullValue = true;
        }
        positionCount++;
        offsets[positionCount] = entryEnd;
        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(Integer.BYTES + Byte.BYTES + (offsets[positionCount] - offsets[positionCount - 1]));
        }
        updateRetainedSize();
    }

    @Override
    public void resetTo(int position)
    {
        if (position < 0 || position > positionCount) {
            throw new IllegalArgumentException("position out of bounds");
        }
        // Rewind the buffer to where the retained prefix ends.
        positionCount = position;
        output.reset(offsets[position]);

        hasNullValue = false;
        hasNonNullValue = false;
        for (int i = 0; i < positionCount; i++) {
            if (valueIsNull[i]) {
                hasNullValue = true;
            }
            else {
                hasNonNullValue = true;
            }
        }
    }

    @Override
    public Block build()
    {
        if (!hasNonNullValue) {
            return RunLengthEncodedBlock.create(new JsonBlock(0, 1, new boolean[] {true}, new int[] {0, 0}, EMPTY_SLICE), positionCount);
        }
        return buildValueBlock();
    }

    @Override
    public JsonBlock buildValueBlock()
    {
        return new JsonBlock(0, positionCount, hasNullValue ? valueIsNull : null, offsets, output.slice());
    }

    @Override
    public BlockBuilder newBlockBuilderLike(int expectedEntries, BlockBuilderStatus blockBuilderStatus)
    {
        return new JsonBlockBuilder(blockBuilderStatus, expectedEntries);
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getSizeInBytes()
    {
        return output.size() + ((long) positionCount * (Integer.BYTES + Byte.BYTES));
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    private void ensureCapacity(int capacity)
    {
        if (valueIsNull.length >= capacity) {
            return;
        }
        int newSize;
        if (initialized) {
            newSize = calculateNewArraySize(capacity);
        }
        else {
            newSize = initialEntryCount;
            initialized = true;
        }
        newSize = max(newSize, capacity);
        valueIsNull = Arrays.copyOf(valueIsNull, newSize);
        offsets = Arrays.copyOf(offsets, newSize + 1);
        updateRetainedSize();
    }

    private static int calculateNewArraySize(int currentSize)
    {
        return (int) max(currentSize * 2L, 1);
    }

    private void updateRetainedSize()
    {
        retainedSizeInBytes = INSTANCE_SIZE + output.getRetainedSize() + sizeOf(valueIsNull) + sizeOf(offsets);
    }
}
