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
package io.trino.array;

import io.trino.spi.block.Block;
import io.trino.spi.block.IntArrayBlockBuilder;
import org.junit.jupiter.api.Test;

import static io.airlift.slice.SizeOf.instanceSize;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBlockBigArray
{
    @Test
    public void testRetainedSizeWithOverlappingBlocks()
    {
        int entries = 123;
        IntArrayBlockBuilder blockBuilder = new IntArrayBlockBuilder(null, entries);
        for (int i = 0; i < entries; i++) {
            blockBuilder.writeInt(i);
        }
        Block block = blockBuilder.build();

        // Verify we do not over count
        int arraySize = 456;
        int blocks = 7890;
        BlockBigArray blockBigArray = new BlockBigArray();
        blockBigArray.ensureCapacity(arraySize);
        for (int i = 0; i < blocks; i++) {
            blockBigArray.set(i % arraySize, block.getRegion(0, entries));
        }

        ReferenceCountMap referenceCountMap = new ReferenceCountMap();
        referenceCountMap.incrementAndGet(block);
        long expectedSize = instanceSize(BlockBigArray.class)
                + referenceCountMap.sizeOf()
                + (new ObjectBigArray<>()).sizeOf()
                + block.getRetainedSizeInBytes() + (arraySize - 1L) * instanceSize(block.getClass());
        assertThat(blockBigArray.sizeOf()).isEqualTo(expectedSize);
    }
}
