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
package io.trino.operator;

import io.trino.spi.Page;
import io.trino.spi.PageBuilder;

import static io.airlift.slice.SizeOf.instanceSize;

/**
 * GroupByHash that provides a round robin group ID assignment.
 */
public class CyclingGroupByHash
        implements GroupByHash
{
    private static final int INSTANCE_SIZE = instanceSize(CyclingGroupByHash.class);

    private final int totalGroupCount;
    private int maxGroupId;
    private int currentGroupId;

    public CyclingGroupByHash(int totalGroupCount)
    {
        this.totalGroupCount = totalGroupCount;
    }

    private CyclingGroupByHash(CyclingGroupByHash other)
    {
        this.totalGroupCount = other.totalGroupCount;
        this.maxGroupId = other.maxGroupId;
        this.currentGroupId = other.currentGroupId;
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE;
    }

    @Override
    public int getGroupCount()
    {
        return maxGroupId + 1;
    }

    @Override
    public void appendValuesTo(int groupId, PageBuilder pageBuilder)
    {
        throw new UnsupportedOperationException("Not yet supported");
    }

    @Override
    public Work<?> addPage(Page page)
    {
        throw new UnsupportedOperationException("Not yet supported");
    }

    @Override
    public Work<int[]> getGroupIds(Page page)
    {
        int[] groupIds = new int[page.getPositionCount()];
        for (int i = 0; i < page.getPositionCount(); i++) {
            groupIds[i] = currentGroupId;
            maxGroupId = Math.max(currentGroupId, maxGroupId);
            currentGroupId = (currentGroupId + 1) % totalGroupCount;
        }
        return new CompletedWork<>(groupIds);
    }

    @Override
    public long getRawHash(int groupId)
    {
        throw new UnsupportedOperationException("Not yet supported");
    }

    @Override
    public int getCapacity()
    {
        return totalGroupCount;
    }

    @Override
    public GroupByHash copy()
    {
        return new CyclingGroupByHash(this);
    }
}
