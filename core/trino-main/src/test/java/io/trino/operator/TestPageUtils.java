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
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.LazyBlock;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static io.trino.block.BlockAssertions.createIntsBlock;
import static io.trino.operator.PageUtils.recordMaterializedBytes;
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

    @Test
    public void testNestedBlocks()
    {
        Block elements = lazyWrapper(createIntsBlock(1, 2, 3));
        Block arrayBlock = ArrayBlock.fromElementBlock(2, Optional.empty(), new int[] {0, 1, 3}, elements);
        long initialArraySize = arrayBlock.getSizeInBytes();
        Page page = new Page(2, arrayBlock);

        AtomicLong sizeInBytes = new AtomicLong();
        recordMaterializedBytes(page, sizeInBytes::getAndAdd);

        assertEquals(arrayBlock.getSizeInBytes(), initialArraySize);
        assertEquals(sizeInBytes.get(), arrayBlock.getSizeInBytes());

        // dictionary block caches size in bytes
        arrayBlock.getLoadedBlock();
        assertEquals(sizeInBytes.get(), arrayBlock.getSizeInBytes());
        assertEquals(sizeInBytes.get(), initialArraySize + elements.getSizeInBytes());
    }

    private static LazyBlock lazyWrapper(Block block)
    {
        return new LazyBlock(block.getPositionCount(), block::getLoadedBlock);
    }
}
