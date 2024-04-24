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

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceOutput;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.GroupedAccumulatorState;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.operator.VariableWidthData.POINTER_SIZE;
import static java.lang.Math.clamp;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

public class GroupListaggAggregationState
        extends AbstractListaggAggregationState
        implements GroupedAccumulatorState
{
    // See jdk.internal.util.ArraysSupport.SOFT_MAX_ARRAY_LENGTH for an explanation
    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, LITTLE_ENDIAN);

    private final int recordNextIndexOffset;
    private final DynamicSliceOutput out = new DynamicSliceOutput(0);

    private long[] groupHeadPositions = new long[0];
    private long[] groupTailPositions = new long[0];
    private int[] groupSize = new int[0];

    private int groupId = -1;

    public GroupListaggAggregationState()
    {
        super(Long.BYTES);
        recordNextIndexOffset = POINTER_SIZE;
    }

    private GroupListaggAggregationState(GroupListaggAggregationState state)
    {
        super(state);
        this.recordNextIndexOffset = state.recordNextIndexOffset;

        this.groupHeadPositions = Arrays.copyOf(state.groupHeadPositions, state.groupHeadPositions.length);
        this.groupTailPositions = Arrays.copyOf(state.groupTailPositions, state.groupTailPositions.length);
        this.groupSize = Arrays.copyOf(state.groupSize, state.groupSize.length);

        checkArgument(state.groupId == -1, "state.groupId is not -1");
        //noinspection DataFlowIssue
        this.groupId = -1;
    }

    @Override
    public long getEstimatedSize()
    {
        return super.getEstimatedSize() +
                sizeOf(groupHeadPositions) +
                sizeOf(groupTailPositions) +
                sizeOf(groupSize);
    }

    @Override
    public void setGroupId(int groupId)
    {
        this.groupId = groupId;
    }

    @Override
    public void ensureCapacity(int maxGroupId)
    {
        checkArgument(maxGroupId + 1 < MAX_ARRAY_SIZE, "Maximum array size exceeded");
        int requiredSize = maxGroupId + 1;
        if (requiredSize > groupHeadPositions.length) {
            int newSize = clamp(requiredSize * 2L, 1024, MAX_ARRAY_SIZE);
            int oldSize = groupHeadPositions.length;

            groupHeadPositions = Arrays.copyOf(groupHeadPositions, newSize);
            Arrays.fill(groupHeadPositions, oldSize, newSize, -1);

            groupTailPositions = Arrays.copyOf(groupTailPositions, newSize);
            Arrays.fill(groupTailPositions, oldSize, newSize, -1);

            groupSize = Arrays.copyOf(groupSize, newSize);
        }
    }

    @Override
    public void add(ValueBlock block, int position)
    {
        super.add(block, position);

        long index = size() - 1;
        byte[] records = openRecordGroup;
        int recordOffset = getRecordOffset(index);
        LONG_HANDLE.set(records, recordOffset + recordNextIndexOffset, -1L);

        if (groupTailPositions[groupId] == -1) {
            groupHeadPositions[groupId] = index;
        }
        else {
            long tailIndex = groupTailPositions[groupId];
            LONG_HANDLE.set(getRecords(tailIndex), getRecordOffset(tailIndex) + recordNextIndexOffset, index);
        }
        groupTailPositions[groupId] = index;

        groupSize[groupId]++;
    }

    @Override
    public void write(VariableWidthBlockBuilder blockBuilder)
    {
        if (groupSize[groupId] == 0) {
            blockBuilder.appendNull();
            return;
        }
        out.reset();
        write(out);
        blockBuilder.writeEntry(out.slice());
    }

    private void write(SliceOutput out)
    {
        long index = groupHeadPositions[groupId];
        int emittedCount = 0;
        int entryCount = groupSize[groupId];
        while (index != -1) {
            byte[] records = getRecords(index);
            int recordOffset = getRecordOffset(index);
            if (!writeEntry(records, recordOffset, out, entryCount, emittedCount)) {
                return;
            }
            index = (long) LONG_HANDLE.get(records, recordOffset + recordNextIndexOffset);
            emittedCount++;
        }
    }

    @Override
    public AccumulatorState copy()
    {
        return new GroupListaggAggregationState(this);
    }
}
