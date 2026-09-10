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
package io.trino.plugin.hive;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.function.ObjLongConsumer;

import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTransformConnectorPageSource
{
    @Test
    public void testSelectedPositionsRemapLoadedBlockWithoutRetainingCallerArray()
    {
        SourcePage inputPage = new SelectingSourcePage(new LongArrayBlock(5, Optional.empty(), new long[] {10, 11, 12, 13, 14}));
        ConnectorPageSource pageSource = TransformConnectorPageSource.builder()
                .transform(0, block -> block)
                .build(new SinglePageSource(inputPage));

        SourcePage page = pageSource.getNextSourcePage();
        assertThat(page.getBlock(0).getPositionCount()).isEqualTo(5);

        int[] positions = {99, 3, 1, 99};
        assertThat(page.trySelectPositions(positions, 1, 2)).isTrue();
        positions[1] = 0;
        positions[2] = 0;

        assertThat(page.getPositionCount()).isEqualTo(2);
        assertThat(BIGINT.getLong(page.getBlock(0), 0)).isEqualTo(13);
        assertThat(BIGINT.getLong(page.getBlock(0), 1)).isEqualTo(11);
    }

    private static final class SelectingSourcePage
            implements SourcePage
    {
        private Block block;

        private SelectingSourcePage(Block block)
        {
            this.block = block;
        }

        @Override
        public int getPositionCount()
        {
            return block.getPositionCount();
        }

        @Override
        public long getSizeInBytes()
        {
            return block.getSizeInBytes();
        }

        @Override
        public long getRetainedSizeInBytes()
        {
            return block.getRetainedSizeInBytes();
        }

        @Override
        public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer)
        {
            block.retainedBytesForEachPart(consumer);
        }

        @Override
        public int getChannelCount()
        {
            return 1;
        }

        @Override
        public Block getBlock(int channel)
        {
            return block;
        }

        @Override
        public Page getPage()
        {
            return new Page(block);
        }

        @Override
        public boolean trySelectPositions(int[] positions, int offset, int size)
        {
            block = block.copyPositions(positions, offset, size);
            return true;
        }

        @Override
        public void selectPositions(int[] positions, int offset, int size)
        {
            block = block.copyPositions(positions, offset, size);
        }
    }

    private static final class SinglePageSource
            implements ConnectorPageSource
    {
        private SourcePage page;

        private SinglePageSource(SourcePage page)
        {
            this.page = page;
        }

        @Override
        public long getCompletedBytes()
        {
            return 0;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public boolean isFinished()
        {
            return page == null;
        }

        @Override
        public SourcePage getNextSourcePage()
        {
            SourcePage result = page;
            page = null;
            return result;
        }

        @Override
        public void close() {}
    }
}
