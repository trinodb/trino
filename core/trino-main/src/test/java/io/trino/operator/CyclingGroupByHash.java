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

import com.google.common.collect.ImmutableList;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;

/**
 * GroupByHash that provides a round robin group ID assignment.
 */
public class CyclingGroupByHash
        implements GroupByHash
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(CyclingGroupByHash.class).instanceSize();

    private final int totalGroupCount;
    private int maxGroupId;
    private int currentGroupId;

    public CyclingGroupByHash(int totalGroupCount)
    {
        this.totalGroupCount = totalGroupCount;
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE;
    }

    @Override
    public long getHashCollisions()
    {
        return 0;
    }

    @Override
    public double getExpectedHashCollisions()
    {
        return 0;
    }

    @Override
    public List<Type> getTypes()
    {
        return ImmutableList.of();
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
    public Work<GroupByIdBlock> getGroupIds(Page page)
    {
        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, page.getChannelCount());
        for (int i = 0; i < page.getPositionCount(); i++) {
            BIGINT.writeLong(blockBuilder, currentGroupId);
            maxGroupId = Math.max(currentGroupId, maxGroupId);
            currentGroupId = (currentGroupId + 1) % totalGroupCount;
        }
        return new CompletedWork<>(new GroupByIdBlock(getGroupCount(), blockBuilder.build()));
    }

    @Override
    public boolean contains(int position, Page page, int[] hashChannels)
    {
        throw new UnsupportedOperationException("Not yet supported");
    }

    @Override
    public long getRawHash(int groupyId)
    {
        throw new UnsupportedOperationException("Not yet supported");
    }

    @Override
    public int getCapacity()
    {
        return totalGroupCount;
    }
}
