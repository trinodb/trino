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

import io.trino.operator.aggregation.state.AbstractGroupedAccumulatorState;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.type.Type;

import java.lang.invoke.MethodHandle;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.clamp;

public final class GroupArrayAggregationState
        extends AbstractGroupedAccumulatorState
        implements ArrayAggregationState
{
    private static final int INSTANCE_SIZE = instanceSize(GroupArrayAggregationState.class);

    // See jdk.internal.util.ArraysSupport.SOFT_MAX_ARRAY_LENGTH for an explanation
    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    private final FlatArrayBuilder arrayBuilder;
    private long[] groupHeadPositions = new long[0];
    private long[] groupTailPositions = new long[0];

    public GroupArrayAggregationState(Type type, MethodHandle readFlat, MethodHandle writeFlat)
    {
        arrayBuilder = new FlatArrayBuilder(type, readFlat, writeFlat, true);
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE +
                sizeOf(groupHeadPositions) +
                sizeOf(groupTailPositions) +
                arrayBuilder.getEstimatedSize();
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
        }
    }

    @Override
    public void add(ValueBlock block, int position)
    {
        int groupId = (int) getGroupId();
        long index = arrayBuilder.size();

        if (groupTailPositions[groupId] == -1) {
            groupHeadPositions[groupId] = index;
        }
        else {
            arrayBuilder.setNextIndex(groupTailPositions[groupId], index);
        }
        groupTailPositions[groupId] = index;
        arrayBuilder.add(block, position);
    }

    @Override
    public void writeAll(BlockBuilder blockBuilder)
    {
        long nextIndex = getGroupHeadPosition();
        checkArgument(nextIndex != -1, "Group is empty");
        while (nextIndex != -1) {
            nextIndex = arrayBuilder.write(nextIndex, blockBuilder);
        }
    }

    private long getGroupHeadPosition()
    {
        int groupId = (int) getGroupId();
        if (groupId >= groupHeadPositions.length) {
            return -1;
        }
        return groupHeadPositions[groupId];
    }

    @Override
    public boolean isEmpty()
    {
        return getGroupHeadPosition() == -1;
    }
}
