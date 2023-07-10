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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.metrics.Metrics;

import java.io.IOException;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class ConstantPopulatingPageSource
        implements ConnectorPageSource
{
    private final ConnectorPageSource delegate;
    private final Block[] constantColumns;
    private final int[] targetChannelToSourceChannel;

    private ConstantPopulatingPageSource(ConnectorPageSource delegate, Block[] constantColumns, int[] targetChannelToSourceChannel)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.constantColumns = requireNonNull(constantColumns, "constantColumns is null");
        this.targetChannelToSourceChannel = requireNonNull(targetChannelToSourceChannel, "targetChannelToSourceChannel is null");
    }

    @Override
    public long getCompletedBytes()
    {
        return delegate.getCompletedBytes();
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return delegate.getCompletedPositions();
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
        Page delegatePage = delegate.getNextPage();

        if (delegatePage == null) {
            return null;
        }

        int size = constantColumns.length;
        Block[] blocks = new Block[size];
        for (int targetChannel = 0; targetChannel < size; targetChannel++) {
            Block constantValue = constantColumns[targetChannel];
            if (constantValue != null) {
                blocks[targetChannel] = RunLengthEncodedBlock.create(constantValue, delegatePage.getPositionCount());
            }
            else {
                blocks[targetChannel] = delegatePage.getBlock(targetChannelToSourceChannel[targetChannel]);
            }
        }

        return new Page(delegatePage.getPositionCount(), blocks);
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

    @Override
    public CompletableFuture<?> isBlocked()
    {
        return delegate.isBlocked();
    }

    @Override
    public Metrics getMetrics()
    {
        return delegate.getMetrics();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private final ImmutableList.Builder<ColumnType> columns = ImmutableList.builder();

        private Builder()
        { }

        public Builder addConstantColumn(Block value)
        {
            columns.add(new ConstantColumn(value));
            return this;
        }

        public Builder addDelegateColumn(int sourceChannel)
        {
            columns.add(new DelegateColumn(sourceChannel));
            return this;
        }

        public ConnectorPageSource build(ConnectorPageSource delegate)
        {
            List<ColumnType> columns = this.columns.build();
            Block[] constantValues = new Block[columns.size()];
            int[] delegateIndexes = new int[columns.size()];

            // If no constant columns are added and the delegate columns are in order, nothing to do
            boolean isRequired = false;

            for (int columnChannel = 0; columnChannel < columns.size(); columnChannel++) {
                ColumnType column = columns.get(columnChannel);
                if (column instanceof ConstantColumn) {
                    constantValues[columnChannel] = ((ConstantColumn) column).getValue();
                    isRequired = true;
                }
                else if (column instanceof DelegateColumn) {
                    int delegateChannel = ((DelegateColumn) column).getSourceChannel();
                    delegateIndexes[columnChannel] = delegateChannel;
                    if (columnChannel != delegateChannel) {
                        isRequired = true;
                    }
                }
                else {
                    throw new IllegalStateException("Unknown ConstantPopulatingPageSource ColumnType " + column);
                }
            }

            if (!isRequired) {
                return delegate;
            }

            return new ConstantPopulatingPageSource(delegate, constantValues, delegateIndexes);
        }
    }

    public interface ColumnType {}

    private static class ConstantColumn
            implements ColumnType
    {
        private final Block value;

        private ConstantColumn(Block value)
        {
            this.value = requireNonNull(value, "value is null");
            checkArgument(value.getPositionCount() == 1, "ConstantColumn may only contain one value");
        }

        public Block getValue()
        {
            return value;
        }
    }

    private static class DelegateColumn
            implements ColumnType
    {
        private final int sourceChannel;

        private DelegateColumn(int sourceChannel)
        {
            this.sourceChannel = sourceChannel;
        }

        public int getSourceChannel()
        {
            return sourceChannel;
        }
    }
}
