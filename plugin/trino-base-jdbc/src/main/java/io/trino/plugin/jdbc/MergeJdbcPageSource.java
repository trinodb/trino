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
package io.trino.plugin.jdbc;

import com.google.common.collect.ImmutableList;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.connector.ConnectorPageSource;

import java.io.IOException;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class MergeJdbcPageSource
        implements ConnectorPageSource
{
    private final ConnectorPageSource delegate;
    private final List<ColumnAdaptation> columnAdaptations;

    public MergeJdbcPageSource(ConnectorPageSource delegate, List<ColumnAdaptation> columnAdaptations)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.columnAdaptations = ImmutableList.copyOf(requireNonNull(columnAdaptations, "columnAdaptations is null"));
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
    public Page getNextPage()
    {
        Page page = delegate.getNextPage();
        if (page == null || columnAdaptations.isEmpty()) {
            return page;
        }

        return getColumnAdaptationsPage(page);
    }

    private Page getColumnAdaptationsPage(Page page)
    {
        Block[] blocks = new Block[columnAdaptations.size()];
        for (int i = 0; i < columnAdaptations.size(); i++) {
            blocks[i] = columnAdaptations.get(i).getBlock(page);
        }

        return new Page(page.getPositionCount(), blocks);
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

    public interface ColumnAdaptation
    {
        Block getBlock(Page sourcePage);
    }

    public static final class MergedRowAdaptation
            implements ColumnAdaptation
    {
        private final List<Integer> mergeRowIdSourceChannels;

        public MergedRowAdaptation(List<Integer> mergeRowIdSourceChannels)
        {
            this.mergeRowIdSourceChannels = mergeRowIdSourceChannels;
        }

        @Override
        public Block getBlock(Page page)
        {
            requireNonNull(page, "page is null");
            Block[] mergeRowIdBlocks = new Block[mergeRowIdSourceChannels.size()];
            for (int i = 0; i < mergeRowIdBlocks.length; i++) {
                mergeRowIdBlocks[i] = page.getBlock(mergeRowIdSourceChannels.get(i));
            }
            return RowBlock.fromFieldBlocks(page.getPositionCount(), mergeRowIdBlocks);
        }
    }

    public record SourceColumn(int sourceChannel)
            implements ColumnAdaptation
    {
        public SourceColumn
        {
            checkArgument(sourceChannel >= 0, "sourceChannel is negative");
        }

        @Override
        public Block getBlock(Page sourcePage)
        {
            return sourcePage.getBlock(sourceChannel);
        }
    }
}
