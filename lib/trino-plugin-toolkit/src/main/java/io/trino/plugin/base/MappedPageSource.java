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
package io.trino.plugin.base;

import com.google.common.primitives.Ints;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;

import java.io.IOException;
import java.util.List;
import java.util.function.ObjLongConsumer;

import static java.util.Objects.requireNonNull;

public class MappedPageSource
        implements ConnectorPageSource
{
    private final ConnectorPageSource delegate;
    private final int[] delegateFieldIndex;

    public MappedPageSource(ConnectorPageSource delegate, List<Integer> delegateFieldIndex)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.delegateFieldIndex = Ints.toArray(requireNonNull(delegateFieldIndex, "delegateFieldIndex is null"));
    }

    @Override
    public long getCompletedBytes()
    {
        return delegate.getCompletedBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return delegate.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return delegate.isFinished();
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        SourcePage nextPage = delegate.getNextSourcePage();
        if (nextPage == null) {
            return null;
        }
        return new MappedSourcePage(nextPage, delegateFieldIndex);
    }

    @Override
    public long getMemoryUsage()
    {
        return delegate.getMemoryUsage();
    }

    @Override
    public void close()
            throws IOException
    {
        delegate.close();
    }

    private record MappedSourcePage(SourcePage sourcePage, int[] channels)
            implements SourcePage
    {
        private MappedSourcePage
        {
            requireNonNull(sourcePage, "sourcePage is null");
            requireNonNull(channels, "channels is null");
        }

        @Override
        public int getPositionCount()
        {
            return sourcePage.getPositionCount();
        }

        @Override
        public long getSizeInBytes()
        {
            return sourcePage.getSizeInBytes();
        }

        @Override
        public long getRetainedSizeInBytes()
        {
            return sourcePage.getRetainedSizeInBytes();
        }

        @Override
        public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer)
        {
            sourcePage.retainedBytesForEachPart(consumer);
        }

        @Override
        public int getChannelCount()
        {
            return channels.length;
        }

        @Override
        public Block getBlock(int channel)
        {
            return sourcePage.getBlock(channels[channel]);
        }

        @Override
        public Page getPage()
        {
            return sourcePage.getColumns(channels);
        }

        @Override
        public Page getColumns(int[] channels)
        {
            int[] newChannels = new int[channels.length];
            for (int i = 0; i < channels.length; i++) {
                newChannels[i] = this.channels[channels[i]];
            }
            return sourcePage.getColumns(newChannels);
        }

        @Override
        public void selectPositions(int[] positions, int offset, int size)
        {
            sourcePage.selectPositions(positions, offset, size);
        }
    }
}
