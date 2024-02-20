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
package io.trino.operator.aggregation.listagg;

import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.trino.operator.VariableWidthData;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.SqlRow;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.type.ArrayType;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.slice.SizeOf.sizeOfObjectArray;
import static io.trino.operator.VariableWidthData.POINTER_SIZE;
import static io.trino.operator.VariableWidthData.getChunkOffset;
import static io.trino.spi.StandardErrorCode.EXCEEDED_FUNCTION_MEMORY_LIMIT;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public abstract class AbstractListaggAggregationState
        implements ListaggAggregationState
{
    private static final int INSTANCE_SIZE = instanceSize(AbstractListaggAggregationState.class);

    private static final int MAX_OUTPUT_LENGTH = DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
    private static final int MAX_OVERFLOW_FILLER_LENGTH = 65_536;

    private static final int RECORDS_PER_GROUP_SHIFT = 10;
    protected static final int RECORDS_PER_GROUP = 1 << RECORDS_PER_GROUP_SHIFT;
    protected static final int RECORDS_PER_GROUP_MASK = RECORDS_PER_GROUP - 1;

    private boolean initialized;
    private Slice separator;
    private boolean overflowError;
    private Slice overflowFiller;
    private boolean showOverflowEntryCount;
    private int maxOutputLength = MAX_OUTPUT_LENGTH;

    protected final int recordSize;

    /**
     * The fixed chunk contains an array of records. The records are laid out as follows:
     * <ul>
     *     <li>12 byte pointer to variable width data
     *     <li>8 byte next index (only present if {@code hasNextIndex} is true)</li>
     * </ul>
     * The pointer is placed first to simplify the offset calculations for variable with code.
     * This chunk contains {@code capacity + 1} records. The extra record is used for the swap operation.
     */
    protected final List<byte[]> closedRecordGroups = new ArrayList<>();

    protected byte[] openRecordGroup;

    private final VariableWidthData variableWidthData;

    private long capacity;
    private long size;

    public AbstractListaggAggregationState(int extraRecordBytes)
    {
        variableWidthData = new VariableWidthData();
        recordSize = POINTER_SIZE + extraRecordBytes;
        openRecordGroup = new byte[recordSize * RECORDS_PER_GROUP];
        capacity = RECORDS_PER_GROUP;
    }

    public AbstractListaggAggregationState(AbstractListaggAggregationState state)
    {
        this.initialized = state.initialized;
        this.separator = state.separator;
        this.overflowError = state.overflowError;
        this.overflowFiller = state.overflowFiller;
        this.showOverflowEntryCount = state.showOverflowEntryCount;
        this.maxOutputLength = state.maxOutputLength;

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

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE +
                sizeOfObjectArray(closedRecordGroups.size()) +
                ((long) closedRecordGroups.size() * RECORDS_PER_GROUP * recordSize) +
                (openRecordGroup == null ? 0 : sizeOf(openRecordGroup)) +
                (variableWidthData == null ? 0 : variableWidthData.getRetainedSizeBytes());
    }

    protected final long size()
    {
        return size;
    }

    @Override
    public final void initialize(Slice separator, boolean overflowError, Slice overflowFiller, boolean showOverflowEntryCount)
    {
        if (initialized) {
            return;
        }
        requireNonNull(separator, "separator is null");
        requireNonNull(overflowFiller, "overflowFiller is null");
        if (overflowFiller.length() > MAX_OVERFLOW_FILLER_LENGTH) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("Overflow filler length %d exceeds maximum length %d", overflowFiller.length(), MAX_OVERFLOW_FILLER_LENGTH));
        }

        this.separator = separator;
        this.overflowError = overflowError;
        this.overflowFiller = overflowFiller;
        this.showOverflowEntryCount = showOverflowEntryCount;
        initialized = true;
    }

    @VisibleForTesting
    void setMaxOutputLength(int maxOutputLength)
    {
        this.maxOutputLength = maxOutputLength;
    }

    @Override
    public void add(ValueBlock block, int position)
    {
        checkArgument(!block.isNull(position), "element is null");

        if (size == capacity) {
            closedRecordGroups.add(openRecordGroup);
            openRecordGroup = new byte[recordSize * RECORDS_PER_GROUP];
            capacity += RECORDS_PER_GROUP;
        }

        byte[] records = openRecordGroup;
        int recordOffset = getRecordOffset(size);

        Slice slice = VARCHAR.getSlice(block, position);

        int variableWidthLength = slice.length();
        byte[] variableWidthChunk = variableWidthData.allocate(records, recordOffset, variableWidthLength);
        int variableWidthChunkOffset = getChunkOffset(records, recordOffset);

        slice.getBytes(0, variableWidthChunk, variableWidthChunkOffset, variableWidthLength);

        size++;
    }

    @Override
    public void serialize(RowBlockBuilder rowBlockBuilder)
    {
        if (size == 0) {
            rowBlockBuilder.appendNull();
            return;
        }
        rowBlockBuilder.buildEntry(fieldBuilders -> {
            VARCHAR.writeSlice(fieldBuilders.get(0), separator);
            BOOLEAN.writeBoolean(fieldBuilders.get(1), overflowError);
            VARCHAR.writeSlice(fieldBuilders.get(2), overflowFiller);
            BOOLEAN.writeBoolean(fieldBuilders.get(3), showOverflowEntryCount);

            ((ArrayBlockBuilder) fieldBuilders.get(4)).buildEntry(elementBuilder -> {
                VariableWidthBlockBuilder valueBuilder = (VariableWidthBlockBuilder) elementBuilder;
                for (byte[] records : closedRecordGroups) {
                    int recordOffset = 0;
                    for (int recordIndex = 0; recordIndex < RECORDS_PER_GROUP; recordIndex++) {
                        writeValue(records, recordOffset, valueBuilder);
                        recordOffset += recordSize;
                    }
                }
                int recordsInOpenGroup = ((int) size) & RECORDS_PER_GROUP_MASK;
                int recordOffset = 0;
                for (int recordIndex = 0; recordIndex < recordsInOpenGroup; recordIndex++) {
                    writeValue(openRecordGroup, recordOffset, valueBuilder);
                    recordOffset += recordSize;
                }
            });
        });
    }

    private void writeValue(byte[] records, int recordOffset, VariableWidthBlockBuilder elementBuilder)
    {
        byte[] variableWidthChunk = variableWidthData.getChunk(records, recordOffset);
        int valueOffset = getChunkOffset(records, recordOffset);
        int valueLength = VariableWidthData.getValueLength(records, recordOffset);

        elementBuilder.writeEntry(variableWidthChunk, valueOffset, valueLength);
    }

    @Override
    public void merge(ListaggAggregationState other)
    {
        SqlRow sqlRow = ((SingleListaggAggregationState) other).removeTempSerializedState();

        List<Block> fields = sqlRow.getRawFieldBlocks();
        int index = sqlRow.getRawIndex();
        Slice separator = VARCHAR.getSlice(fields.get(0), index);
        boolean overflowError = BOOLEAN.getBoolean(fields.get(1), index);
        Slice overflowFiller = VARCHAR.getSlice(fields.get(2), index);
        boolean showOverflowEntryCount = BOOLEAN.getBoolean(fields.get(3), index);
        initialize(separator, overflowError, overflowFiller, showOverflowEntryCount);

        Block array = new ArrayType(VARCHAR).getObject(fields.get(4), index);
        ValueBlock arrayValues = array.getUnderlyingValueBlock();
        for (int i = 0; i < array.getPositionCount(); i++) {
            add(arrayValues, arrayValues.getUnderlyingValuePosition(i));
        }
    }

    protected final boolean writeEntry(byte[] records, int recordOffset, SliceOutput out, int totalEntryCount, int emittedCount)
    {
        byte[] variableWidthChunk = variableWidthData.getChunk(records, recordOffset);
        int valueOffset = getChunkOffset(records, recordOffset);
        int valueLength = VariableWidthData.getValueLength(records, recordOffset);

        int spaceRequired = valueLength + (emittedCount > 0 ? separator.length() : 0);

        if (out.size() + spaceRequired > maxOutputLength) {
            writeOverflow(out, totalEntryCount, emittedCount);
            return false;
        }

        if (emittedCount > 0) {
            out.writeBytes(separator);
        }

        out.writeBytes(variableWidthChunk, valueOffset, valueLength);
        return true;
    }

    private void writeOverflow(SliceOutput out, int entryCount, int emittedCount)
    {
        if (overflowError) {
            throw new TrinoException(EXCEEDED_FUNCTION_MEMORY_LIMIT, format("Concatenated string has the length in bytes larger than the maximum output length %d", maxOutputLength));
        }

        if (emittedCount > 0) {
            out.writeBytes(separator);
        }
        out.writeBytes(overflowFiller);

        if (showOverflowEntryCount) {
            out.writeBytes(Slices.utf8Slice("("), 0, 1);
            Slice count = Slices.utf8Slice(Integer.toString(entryCount - emittedCount));
            out.writeBytes(count, 0, count.length());
            out.writeBytes(Slices.utf8Slice(")"), 0, 1);
        }
    }

    protected final byte[] getRecords(long index)
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

    protected final int getRecordOffset(long index)
    {
        return (((int) index) & RECORDS_PER_GROUP_MASK) * recordSize;
    }
}
