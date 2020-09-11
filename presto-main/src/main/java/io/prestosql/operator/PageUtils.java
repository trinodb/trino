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

import java.util.function.Consumer;
import java.util.function.LongConsumer;

import static io.prestosql.spi.block.LazyBlock.listenForLoads;
import static java.util.Objects.requireNonNull;

public final class PageUtils
{
    private PageUtils() {}

    public static void recordMaterializedBytes(Page page, LongConsumer sizeInBytesConsumer)
    {
        // account processed bytes from lazy blocks only when they are loaded
        long loadedBlocksSizeInBytes = 0;

        for (int i = 0; i < page.getChannelCount(); ++i) {
            Block block = page.getBlock(i);
            long initialSize = block.getSizeInBytes();
            loadedBlocksSizeInBytes += initialSize;
            listenForLoads(block, new BlockSizeListener(block, sizeInBytesConsumer, initialSize));
        }

        if (loadedBlocksSizeInBytes > 0) {
            sizeInBytesConsumer.accept(loadedBlocksSizeInBytes);
        }
    }

    private static class BlockSizeListener
            implements Consumer<Block>
    {
        private final Block topLevelBlock;
        private final LongConsumer sizeInBytesConsumer;
        private long lastSize;

        public BlockSizeListener(Block topLevelBlock, LongConsumer sizeInBytesConsumer, long initialSize)
        {
            this.topLevelBlock = requireNonNull(topLevelBlock, "topLevelBlock is null");
            this.sizeInBytesConsumer = requireNonNull(sizeInBytesConsumer, "sizeInBytesConsumer is null");
            this.lastSize = initialSize;
        }

        @Override
        public void accept(Block childBlock)
        {
            long newSize = topLevelBlock.getSizeInBytes();
            if (newSize == lastSize) {
                return;
            }
            sizeInBytesConsumer.accept(newSize - lastSize);
            lastSize = newSize;
        }
    }
}
