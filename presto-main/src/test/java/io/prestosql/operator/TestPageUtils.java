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
package io.prestosql.operator;

import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.LazyBlock;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicLong;

import static io.prestosql.block.BlockAssertions.createIntsBlock;
import static io.prestosql.operator.PageUtils.recordMaterializedBytes;
import static org.testng.Assert.assertEquals;

public class TestPageUtils
{
    @Test
    public void testRecordMaterializedBytes()
    {
        Block first = createIntsBlock(1, 2, 3);
        LazyBlock second = lazyWrapper(first);
        LazyBlock third = lazyWrapper(first);
        Page page = new Page(3, first, second, third);

        second.getLoadedBlock();

        AtomicLong sizeInBytes = new AtomicLong();
        recordMaterializedBytes(page, sizeInBytes::getAndAdd);

        assertEquals(sizeInBytes.get(), first.getSizeInBytes() * 2);

        page.getBlock(2).getLoadedBlock();
        assertEquals(sizeInBytes.get(), first.getSizeInBytes() * 3);
    }

    private static LazyBlock lazyWrapper(Block block)
    {
        return new LazyBlock(block.getPositionCount(), block::getLoadedBlock);
    }
}
