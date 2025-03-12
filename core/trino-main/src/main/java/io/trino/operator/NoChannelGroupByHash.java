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

public class NoChannelGroupByHash
        implements GroupByHash
{
    private static final int INSTANCE_SIZE = instanceSize(NoChannelGroupByHash.class);

    private int groupCount;

    public NoChannelGroupByHash() {}

    private NoChannelGroupByHash(NoChannelGroupByHash other)
    {
        this.groupCount = other.groupCount;
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE;
    }

    @Override
    public int getGroupCount()
    {
        return groupCount;
    }

    @Override
    public void appendValuesTo(int groupId, PageBuilder pageBuilder)
    {
        throw new UnsupportedOperationException("NoChannelGroupByHash does not support appendValuesTo");
    }

    @Override
    public Work<?> addPage(Page page)
    {
        updateGroupCount(page);
        // create a dump work whose result is never used.
        return new CompletedWork<>(0);
    }

    @Override
    public Work<int[]> getGroupIds(Page page)
    {
        updateGroupCount(page);
        return new CompletedWork<>(new int[page.getPositionCount()]);
    }

    @Override
    public long getRawHash(int groupId)
    {
        throw new UnsupportedOperationException("NoChannelGroupByHash does not support getHashCollisions");
    }

    @Override
    public int getCapacity()
    {
        return 2;
    }

    @Override
    public GroupByHash copy()
    {
        return new NoChannelGroupByHash(this);
    }

    private void updateGroupCount(Page page)
    {
        if (page.getPositionCount() > 0 && groupCount == 0) {
            groupCount = 1;
        }
    }
}
