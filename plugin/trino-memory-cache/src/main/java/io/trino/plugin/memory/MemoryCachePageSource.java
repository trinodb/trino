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
import io.trino.spi.connector.ConnectorPageSource;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class MemoryCachePageSource
        implements ConnectorPageSource
{
    private final long memoryUsageBytes;
    private final Block[][] channels;
    private final long positionCount;
    private int currentBlock;
    private long currentPosition;
    private long completedBytes;
    private boolean closed;

    public MemoryCachePageSource(Channel[] channels)
    {
        checkArgument(channels.length > 0);
        requireNonNull(channels);
        this.positionCount = channels[0].getPositionCount();
        this.channels = new Block[channels.length][];
        long memoryUsageBytes = 0L;
        long storeId = channels[0].getStoreId();
        for (int i = 0; i < channels.length; i++) {
            Channel channel = channels[i];
            memoryUsageBytes += channel.getBlocksRetainedSizeInBytes();
            checkArgument(positionCount == channel.getPositionCount(), "Position count (%s) doesn't match channel position count (%s)", positionCount, channel.getPositionCount());
            checkArgument(storeId == channel.getStoreId(), "Store ids don't match");
            this.channels[i] = channel.getBlocks();
        }
        this.memoryUsageBytes = memoryUsageBytes;
    }

    @Override
    public void close()
    {
        closed = true;
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return closed || currentPosition >= positionCount;
    }

    @Override
    public Page getNextPage()
    {
        if (isFinished()) {
            return null;
        }

        // extract current blocks
        Block[] blocks = new Block[channels.length];
        for (int channel = 0; channel < channels.length; channel++) {
            blocks[channel] = channels[channel][currentBlock];
            completedBytes += blocks[channel].getSizeInBytes();
        }

        // extract next page position count
        currentBlock++;
        currentPosition += blocks[0].getPositionCount();
        return new Page(blocks);
    }

    @Override
    public long getMemoryUsage()
    {
        return memoryUsageBytes;
    }
}
