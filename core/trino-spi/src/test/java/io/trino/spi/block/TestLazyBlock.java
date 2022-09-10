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
package io.trino.spi.block;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestLazyBlock
{
    @Test
    public void testListener()
    {
        List<Block> notifications = new ArrayList<>();
        LazyBlock lazyBlock = new LazyBlock(1, () -> createSingleValueBlock(1));
        LazyBlock.listenForLoads(lazyBlock, notifications::add);

        Block loadedBlock = lazyBlock.getBlock();
        assertEquals(notifications, ImmutableList.of(loadedBlock));

        loadedBlock = lazyBlock.getBlock();
        assertEquals(notifications, ImmutableList.of(loadedBlock));
    }

    @Test
    public void testNestedListener()
    {
        List<Block> actualNotifications = new ArrayList<>();
        LazyBlock lazyBlock = new LazyBlock(1, TestLazyBlock::createInfiniteRecursiveRowBlock);
        LazyBlock.listenForLoads(lazyBlock, actualNotifications::add);

        List<Block> expectedNotifications = new ArrayList<>();
        assertNotificationsRecursive(5, lazyBlock, actualNotifications, expectedNotifications);
    }

    @Test
    public void testLoadedBlockNestedListener()
    {
        List<Block> actualNotifications = new ArrayList<>();
        LazyBlock lazyBlock = new LazyBlock(1, TestLazyBlock::createInfiniteRecursiveRowBlock);
        Block nestedRowBlock = lazyBlock.getBlock();
        LazyBlock.listenForLoads(lazyBlock, actualNotifications::add);
        Block loadedBlock = ((LazyBlock) nestedRowBlock.getChildren().get(0)).getBlock();
        assertEquals(actualNotifications, ImmutableList.of(loadedBlock));
    }

    @Test
    public void testNestedGetLoadedBlock()
    {
        List<Block> actualNotifications = new ArrayList<>();
        Block arrayBlock = new IntArrayBlock(1, Optional.empty(), new int[] {0});
        LazyBlock lazyArrayBlock = new LazyBlock(1, () -> arrayBlock);
        Block dictionaryBlock = DictionaryBlock.create(2, lazyArrayBlock, new int[] {0, 0});
        LazyBlock lazyBlock = new LazyBlock(2, () -> dictionaryBlock);
        LazyBlock.listenForLoads(lazyBlock, actualNotifications::add);

        Block loadedBlock = lazyBlock.getBlock();
        assertThat(loadedBlock).isInstanceOf(DictionaryBlock.class);
        assertThat(((DictionaryBlock) loadedBlock).getDictionary()).isInstanceOf(LazyBlock.class);
        assertEquals(actualNotifications, ImmutableList.of(loadedBlock));

        Block fullyLoadedBlock = lazyBlock.getLoadedBlock();
        assertThat(fullyLoadedBlock).isInstanceOf(DictionaryBlock.class);
        assertThat(((DictionaryBlock) fullyLoadedBlock).getDictionary()).isInstanceOf(IntArrayBlock.class);
        assertEquals(actualNotifications, ImmutableList.of(loadedBlock, arrayBlock));
        assertTrue(lazyBlock.isLoaded());
        assertTrue(dictionaryBlock.isLoaded());
    }

    private static void assertNotificationsRecursive(int depth, Block lazyBlock, List<Block> actualNotifications, List<Block> expectedNotifications)
    {
        assertFalse(lazyBlock.isLoaded());
        Block loadedBlock = ((LazyBlock) lazyBlock).getBlock();
        expectedNotifications.add(loadedBlock);
        assertEquals(actualNotifications, expectedNotifications);

        if (loadedBlock instanceof ArrayBlock) {
            long expectedSize = (Integer.BYTES + Byte.BYTES) * loadedBlock.getPositionCount();
            assertEquals(loadedBlock.getSizeInBytes(), expectedSize);

            Block elementsBlock = loadedBlock.getChildren().get(0);
            if (depth > 0) {
                assertNotificationsRecursive(depth - 1, elementsBlock, actualNotifications, expectedNotifications);
            }

            expectedSize += elementsBlock.getSizeInBytes();
            assertEquals(loadedBlock.getSizeInBytes(), expectedSize);
            return;
        }
        if (loadedBlock instanceof RowBlock) {
            long expectedSize = (Integer.BYTES + Byte.BYTES) * loadedBlock.getPositionCount();
            assertEquals(loadedBlock.getSizeInBytes(), expectedSize);

            for (Block fieldBlock : loadedBlock.getChildren()) {
                if (depth > 0) {
                    assertNotificationsRecursive(depth - 1, fieldBlock, actualNotifications, expectedNotifications);
                }

                long fieldBlockSize = fieldBlock.getSizeInBytes();
                expectedSize += fieldBlockSize;
                assertEquals(loadedBlock.getSizeInBytes(), expectedSize);
            }
            return;
        }
        throw new IllegalArgumentException("Unexpected loaded block type: " + loadedBlock.getClass().getSimpleName());
    }

    private static Block createSingleValueBlock(int value)
    {
        return new IntArrayBlock(1, Optional.empty(), new int[] {value});
    }

    private static Block createInfiniteRecursiveRowBlock()
    {
        return RowBlock.fromFieldBlocks(1, Optional.empty(), new Block[] {
                new LazyBlock(1, TestLazyBlock::createInfiniteRecursiveArrayBlock),
                new LazyBlock(1, TestLazyBlock::createInfiniteRecursiveArrayBlock),
                new LazyBlock(1, TestLazyBlock::createInfiniteRecursiveArrayBlock)
        });
    }

    private static Block createInfiniteRecursiveArrayBlock()
    {
        return ArrayBlock.fromElementBlock(
                1,
                Optional.empty(),
                new int[] {0, 1},
                new LazyBlock(1, TestLazyBlock::createInfiniteRecursiveRowBlock));
    }
}
