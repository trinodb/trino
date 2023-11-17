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
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCyclingGroupByHash
{
    @Test
    public void testSingleGroup()
    {
        CyclingGroupByHash groupByHash = new CyclingGroupByHash(1);
        Page page = createPage(1);
        int[] groupByIds = computeGroupByIdBlock(groupByHash, page);
        assertGrouping(groupByIds, 0);
        assertThat(groupByHash.getGroupCount()).isEqualTo(1);

        page = createPage(2);
        groupByIds = computeGroupByIdBlock(groupByHash, page);
        assertGrouping(groupByIds, 0, 0);
        assertThat(groupByHash.getGroupCount()).isEqualTo(1);
    }

    @Test
    public void testMultipleGroup()
    {
        CyclingGroupByHash groupByHash = new CyclingGroupByHash(2);
        Page page = createPage(3);
        int[] groupByIds = computeGroupByIdBlock(groupByHash, page);
        assertGrouping(groupByIds, 0, 1, 0);
        assertThat(groupByHash.getGroupCount()).isEqualTo(2);

        page = createPage(2);
        groupByIds = computeGroupByIdBlock(groupByHash, page);
        assertGrouping(groupByIds, 1, 0);
        assertThat(groupByHash.getGroupCount()).isEqualTo(2);
    }

    @Test
    public void testPartialGroup()
    {
        CyclingGroupByHash groupByHash = new CyclingGroupByHash(3);
        Page page = createPage(2);
        int[] groupByIds = computeGroupByIdBlock(groupByHash, page);
        assertGrouping(groupByIds, 0, 1);

        // Only 2 groups generated out of max 3
        assertThat(groupByHash.getGroupCount()).isEqualTo(2);
    }

    private static void assertGrouping(int[] groupIds, int... expectedGroupIds)
    {
        assertThat(groupIds).isEqualTo(expectedGroupIds);
    }

    private static int[] computeGroupByIdBlock(GroupByHash groupByHash, Page page)
    {
        Work<int[]> groupIds = groupByHash.getGroupIds(page);
        while (!groupIds.process()) {
            // Process until finished
        }
        return groupIds.getResult();
    }

    private static Page createPage(int positionCount)
    {
        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, positionCount);
        for (int i = 0; i < positionCount; i++) {
            BIGINT.writeLong(blockBuilder, i);
        }
        Block block = blockBuilder.build();
        return new Page(block);
    }
}
