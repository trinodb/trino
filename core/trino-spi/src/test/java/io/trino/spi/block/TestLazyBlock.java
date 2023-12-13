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
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class TestLazyBlock
{
    @Test
    public void testListener()
    {
        List<Block> notifications = new ArrayList<>();
        LazyBlock lazyBlock = new LazyBlock(1, () -> createSingleValueBlock(1));
        LazyBlock.listenForLoads(lazyBlock, notifications::add);

        Block loadedBlock = lazyBlock.getBlock();
        assertThat(notifications).isEqualTo(ImmutableList.of(loadedBlock));

        loadedBlock = lazyBlock.getBlock();
        assertThat(notifications).isEqualTo(ImmutableList.of(loadedBlock));
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
        assertThat(actualNotifications).isEqualTo(ImmutableList.of(loadedBlock));
    }

    @Test
    public void testNestedGetLoadedBlock()
    {
        List<Block> actualNotifications = new ArrayList<>();
        Block arrayBlock = new IntArrayBlock(2, Optional.empty(), new int[] {0, 1});
        LazyBlock lazyArrayBlock = new LazyBlock(2, () -> arrayBlock);
        Block rowBlock = RowBlock.fromFieldBlocks(2, new Block[]{lazyArrayBlock});
        LazyBlock lazyBlock = new LazyBlock(2, () -> rowBlock);
        LazyBlock.listenForLoads(lazyBlock, actualNotifications::add);

        Block loadedBlock = lazyBlock.getBlock();
        assertThat(loadedBlock).isInstanceOf(RowBlock.class);
        assertThat(((RowBlock) loadedBlock).getFieldBlock(0)).isInstanceOf(LazyBlock.class);
        assertThat(actualNotifications).isEqualTo(ImmutableList.of(loadedBlock));

        Block fullyLoadedBlock = lazyBlock.getLoadedBlock();
        assertThat(fullyLoadedBlock).isInstanceOf(RowBlock.class);
        assertThat(((RowBlock) fullyLoadedBlock).getFieldBlock(0)).isInstanceOf(IntArrayBlock.class);
        assertThat(actualNotifications).isEqualTo(ImmutableList.of(loadedBlock, arrayBlock));
        assertThat(lazyBlock.isLoaded()).isTrue();
        assertThat(rowBlock.isLoaded()).isTrue();
    }

    private static void assertNotificationsRecursive(int depth, Block lazyBlock, List<Block> actualNotifications, List<Block> expectedNotifications)
    {
        assertThat(lazyBlock.isLoaded()).isFalse();
        Block loadedBlock = ((LazyBlock) lazyBlock).getBlock();
        expectedNotifications.add(loadedBlock);
        assertThat(actualNotifications).isEqualTo(expectedNotifications);

        if (loadedBlock instanceof ArrayBlock) {
            long expectedSize = (long) (Integer.BYTES + Byte.BYTES) * loadedBlock.getPositionCount();
            assertThat(loadedBlock.getSizeInBytes()).isEqualTo(expectedSize);

            Block elementsBlock = loadedBlock.getChildren().get(0);
            if (depth > 0) {
                assertNotificationsRecursive(depth - 1, elementsBlock, actualNotifications, expectedNotifications);
            }

            expectedSize += elementsBlock.getSizeInBytes();
            assertThat(loadedBlock.getSizeInBytes()).isEqualTo(expectedSize);
            return;
        }
        if (loadedBlock instanceof RowBlock) {
            long expectedSize = (long) Byte.BYTES * loadedBlock.getPositionCount();
            assertThat(loadedBlock.getSizeInBytes()).isEqualTo(expectedSize);

            for (Block fieldBlock : loadedBlock.getChildren()) {
                if (depth > 0) {
                    assertNotificationsRecursive(depth - 1, fieldBlock, actualNotifications, expectedNotifications);
                }

                long fieldBlockSize = fieldBlock.getSizeInBytes();
                expectedSize += fieldBlockSize;
                assertThat(loadedBlock.getSizeInBytes()).isEqualTo(expectedSize);
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
        return RowBlock.fromFieldBlocks(1, new Block[] {
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
