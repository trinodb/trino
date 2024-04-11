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
package io.trino.operator.aggregation.arrayagg;

import com.google.common.base.Throwables;
import io.trino.operator.VariableWidthData;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.type.Type;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.slice.SizeOf.sizeOfObjectArray;
import static io.trino.operator.VariableWidthData.EMPTY_CHUNK;
import static io.trino.operator.VariableWidthData.POINTER_SIZE;
import static io.trino.operator.VariableWidthData.getChunkOffset;
import static java.lang.Math.toIntExact;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Objects.checkIndex;
import static java.util.Objects.requireNonNull;

public class FlatArrayBuilder
{
    private static final int INSTANCE_SIZE = instanceSize(FlatArrayBuilder.class);

    private static final int RECORDS_PER_GROUP_SHIFT = 10;
    private static final int RECORDS_PER_GROUP = 1 << RECORDS_PER_GROUP_SHIFT;
    private static final int RECORDS_PER_GROUP_MASK = RECORDS_PER_GROUP - 1;

    private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, LITTLE_ENDIAN);

    private final Type type;
    private final MethodHandle readFlat;
    private final MethodHandle writeFlat;
    private final boolean hasNextIndex;

    private final int recordNextIndexOffset;
    private final int recordNullOffset;
    private final int recordValueOffset;
    private final int recordSize;

    /**
     * The fixed chunk contains an array of records. The records are laid out as follows:
     * <ul>
     *     <li>12 byte optional pointer to variable width data (only present if the type is variable width)</li>
     *     <li>8 byte next index (only present if {@code hasNextIndex} is true)</li>
     *     <li>1 byte null flag for the element type</li>
     *     <li>N byte fixed size data for the element type</li>
     * </ul>
     * The pointer is placed first to simplify the offset calculations for variable with code.
     * This chunk contains {@code capacity + 1} records. The extra record is used for the swap operation.
     */
    private final List<byte[]> closedRecordGroups = new ArrayList<>();

    private byte[] openRecordGroup;

    private final VariableWidthData variableWidthData;

    private long capacity;
    private long size;

    public FlatArrayBuilder(
            Type type,
            MethodHandle readFlat,
            MethodHandle writeFlat,
            boolean hasNextIndex)
    {
        this.type = requireNonNull(type, "type is null");
        this.readFlat = requireNonNull(readFlat, "readFlat is null");
        this.writeFlat = requireNonNull(writeFlat, "writeFlat is null");
        this.hasNextIndex = hasNextIndex;

        boolean variableWidth = type.isFlatVariableWidth();
        variableWidthData = variableWidth ? new VariableWidthData() : null;
        if (hasNextIndex) {
            recordNextIndexOffset = (variableWidth ? POINTER_SIZE : 0);
            recordNullOffset = recordNextIndexOffset + Long.BYTES;
        }
        else {
            // use MIN_VALUE so that when it is added to the record offset we get a negative value, and thus an ArrayIndexOutOfBoundsException
            recordNextIndexOffset = Integer.MIN_VALUE;
            recordNullOffset = (variableWidth ? POINTER_SIZE : 0);
        }
        recordValueOffset = recordNullOffset + 1;

        recordSize = recordValueOffset + type.getFlatFixedSize();
    }

    private FlatArrayBuilder(FlatArrayBuilder state)
    {
        this.type = state.type;
        this.readFlat = state.readFlat;
        this.writeFlat = state.writeFlat;
        this.hasNextIndex = state.hasNextIndex;

        this.recordNextIndexOffset = state.recordNextIndexOffset;
        this.recordNullOffset = state.recordNullOffset;
        this.recordValueOffset = state.recordValueOffset;
        this.recordSize = state.recordSize;

        this.variableWidthData = state.variableWidthData;
        this.capacity = state.capacity;
        this.size = state.size;
        this.closedRecordGroups.addAll(state.closedRecordGroups);
        // the last open record group must be cloned because it is still being written to
        if (state.openRecordGroup != null) {
            this.openRecordGroup = state.openRecordGroup.clone();
        }
    }

    public long getEstimatedSize()
    {
        return INSTANCE_SIZE +
                sizeOfObjectArray(closedRecordGroups.size()) +
                ((long) closedRecordGroups.size() * RECORDS_PER_GROUP * recordSize) +
                (openRecordGroup == null ? 0 : sizeOf(openRecordGroup)) +
                (variableWidthData == null ? 0 : variableWidthData.getRetainedSizeBytes());
    }

    public Type type()
    {
        return type;
    }

    public long size()
    {
        return size;
    }

    public void setNextIndex(long tailIndex, long nextIndex)
    {
        checkArgument(hasNextIndex, "nextIndex is not supported");

        byte[] records = getRecords(tailIndex);
        int recordOffset = getRecordOffset(tailIndex);
        LONG_HANDLE.set(records, recordOffset + recordNextIndexOffset, nextIndex);
    }

    public void add(ValueBlock block, int position)
    {
        if (size == capacity) {
            growCapacity();
        }

        byte[] records = openRecordGroup;
        int recordOffset = getRecordOffset(size);
        size++;

        if (hasNextIndex) {
            LONG_HANDLE.set(records, recordOffset + recordNextIndexOffset, -1L);
        }

        if (block.isNull(position)) {
            records[recordOffset + recordNullOffset] = 1;
            return;
        }

        byte[] variableWidthChunk = EMPTY_CHUNK;
        int variableWidthChunkOffset = 0;
        if (variableWidthData != null) {
            int variableWidthLength = type.getFlatVariableWidthSize(block, position);
            variableWidthChunk = variableWidthData.allocate(records, recordOffset, variableWidthLength);
            variableWidthChunkOffset = getChunkOffset(records, recordOffset);
        }

        try {
            writeFlat.invokeExact(block, position, records, recordOffset + recordValueOffset, variableWidthChunk, variableWidthChunkOffset);
        }
        catch (Throwable throwable) {
            Throwables.throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
    }

    private void growCapacity()
    {
        if (openRecordGroup != null) {
            closedRecordGroups.add(openRecordGroup);
        }
        openRecordGroup = new byte[recordSize * RECORDS_PER_GROUP];
        capacity += RECORDS_PER_GROUP;
    }

    public void writeAll(BlockBuilder blockBuilder)
    {
        for (byte[] records : closedRecordGroups) {
            int recordOffset = 0;
            for (int recordIndex = 0; recordIndex < RECORDS_PER_GROUP; recordIndex++) {
                write(records, recordOffset, blockBuilder);
                recordOffset += recordSize;
            }
        }

        int recordsInOpenGroup = toIntExact(size - ((long) closedRecordGroups.size() * RECORDS_PER_GROUP));
        int recordOffset = 0;
        for (int recordIndex = 0; recordIndex < recordsInOpenGroup; recordIndex++) {
            write(openRecordGroup, recordOffset, blockBuilder);
            recordOffset += recordSize;
        }
    }

    public long write(long index, BlockBuilder blockBuilder)
    {
        checkIndex(index, size);

        byte[] records = getRecords(index);

        int recordOffset = getRecordOffset(index);
        write(records, recordOffset, blockBuilder);

        if (hasNextIndex) {
            return (long) LONG_HANDLE.get(records, recordOffset + recordNextIndexOffset);
        }
        return -1;
    }

    private void write(byte[] records, int recordOffset, BlockBuilder blockBuilder)
    {
        if (records[recordOffset + recordNullOffset] != 0) {
            blockBuilder.appendNull();
            return;
        }

        byte[] variableWidthChunk = EMPTY_CHUNK;
        if (variableWidthData != null) {
            variableWidthChunk = variableWidthData.getChunk(records, recordOffset);
        }

        try {
            readFlat.invokeExact(
                    records,
                    recordOffset + recordValueOffset,
                    variableWidthChunk,
                    blockBuilder);
        }
        catch (Throwable throwable) {
            Throwables.throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
    }

    public FlatArrayBuilder copy()
    {
        return new FlatArrayBuilder(this);
    }

    private byte[] getRecords(long index)
    {
        int recordGroupIndex = (int) (index >>> RECORDS_PER_GROUP_SHIFT);
        byte[] records;
        if (recordGroupIndex < closedRecordGroups.size()) {
            records = closedRecordGroups.get(recordGroupIndex);
        }
        else {
            checkState(recordGroupIndex == closedRecordGroups.size());
            records = openRecordGroup;
        }
        return records;
    }

    /**
     * Gets the offset of the record within a record group
     */
    private int getRecordOffset(long index)
    {
        return (((int) index) & RECORDS_PER_GROUP_MASK) * recordSize;
    }
}
