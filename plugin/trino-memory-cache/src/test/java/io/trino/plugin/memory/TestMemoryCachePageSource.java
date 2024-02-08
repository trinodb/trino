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
package io.trino.plugin.memory;

import io.trino.plugin.memory.MemoryCacheManager.Channel;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.cache.CacheSplitId;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.plugin.memory.TestUtils.assertBlockEquals;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMemoryCachePageSource
{
    @Test
    public void testPageSource()
    {
        Channel firstChannel = createChannel(
                new IntArrayBlock(4, Optional.empty(), new int[] {0, 1, 2, 3}),
                new IntArrayBlock(2, Optional.empty(), new int[] {4, 5}));
        Channel secondChannel = createChannel(
                new LongArrayBlock(4, Optional.empty(), new long[] {10L, 11L, 12L, 13L}),
                new LongArrayBlock(2, Optional.empty(), new long[] {14L, 15L}));

        MemoryCachePageSource pageSource = new MemoryCachePageSource(new Channel[] {firstChannel, secondChannel});
        assertThat(pageSource.isFinished()).isFalse();
        assertThat(pageSource.getMemoryUsage()).isEqualTo(firstChannel.getBlocksRetainedSizeInBytes() + secondChannel.getBlocksRetainedSizeInBytes());
        assertThat(pageSource.getCompletedBytes()).isEqualTo(0L);

        Page page = pageSource.getNextPage();
        assertThat(page.getChannelCount()).isEqualTo(2);
        assertThat(page.getPositionCount()).isEqualTo(4);
        assertBlockEquals(page.getBlock(0), firstChannel.getBlocks()[0]);
        assertBlockEquals(page.getBlock(1), secondChannel.getBlocks()[0]);
        assertThat(pageSource.getCompletedBytes()).isEqualTo(56);
        assertThat(pageSource.isFinished()).isFalse();

        page = pageSource.getNextPage();
        assertThat(page.getChannelCount()).isEqualTo(2);
        assertThat(page.getPositionCount()).isEqualTo(2);
        assertBlockEquals(page.getBlock(0), firstChannel.getBlocks()[1]);
        assertBlockEquals(page.getBlock(1), secondChannel.getBlocks()[1]);
        assertThat(pageSource.getCompletedBytes()).isEqualTo(84);
        assertThat(pageSource.isFinished()).isTrue();
    }

    private static Channel createChannel(Block... blocks)
    {
        Channel channel = new Channel(new MemoryCacheManager.SplitKey(0, 0, new CacheSplitId("id")), 0);
        channel.setBlocks(blocks);
        channel.setLoaded();
        return channel;
    }
}
