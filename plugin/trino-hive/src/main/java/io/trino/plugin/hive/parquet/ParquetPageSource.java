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
package io.trino.plugin.hive.parquet;

import com.google.common.collect.ImmutableList;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.reader.ParquetReader;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_BAD_DATA;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ParquetPageSource
        implements ConnectorPageSource
{
    private final ParquetReader parquetReader;
    private final List<ColumnAdaptation> columnAdaptations;
    private final boolean isColumnAdaptationRequired;

    private boolean closed;
    private long completedPositions;

    private ParquetPageSource(
            ParquetReader parquetReader,
            List<ColumnAdaptation> columnAdaptations)
    {
        this.parquetReader = requireNonNull(parquetReader, "parquetReader is null");
        this.columnAdaptations = ImmutableList.copyOf(requireNonNull(columnAdaptations, "columnAdaptations is null"));
        this.isColumnAdaptationRequired = isColumnAdaptationRequired(columnAdaptations);
    }

    @Override
    public long getCompletedBytes()
    {
        return parquetReader.getDataSource().getReadBytes();
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return OptionalLong.of(completedPositions);
    }

    @Override
    public long getReadTimeNanos()
    {
        return parquetReader.getDataSource().getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return closed;
    }

    @Override
    public long getMemoryUsage()
    {
        return parquetReader.getMemoryContext().getBytes();
    }

    @Override
    public Page getNextPage()
    {
        Page page;
        try {
            page = getColumnAdaptationsPage(parquetReader.nextPage());
        }
        catch (IOException | RuntimeException e) {
            closeAllSuppress(e, this);
            throw handleException(parquetReader.getDataSource().getId(), e);
        }

        if (closed || page == null) {
            close();
            return null;
        }

        completedPositions += page.getPositionCount();
        return page;
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;

        try {
            parquetReader.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Metrics getMetrics()
    {
        return parquetReader.getMetrics();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private final ImmutableList.Builder<ColumnAdaptation> columns = ImmutableList.builder();

        private Builder() {}

        public Builder addConstantColumn(Block value)
        {
            columns.add(new ConstantColumn(value));
            return this;
        }

        public Builder addSourceColumn(int sourceChannel)
        {
            columns.add(new SourceColumn(sourceChannel));
            return this;
        }

        public Builder addNullColumn(Type type)
        {
            columns.add(new NullColumn(type));
            return this;
        }

        public Builder addRowIndexColumn()
        {
            columns.add(new RowIndexColumn());
            return this;
        }

        public ConnectorPageSource build(ParquetReader parquetReader)
        {
            return new ParquetPageSource(parquetReader, this.columns.build());
        }
    }

    private Page getColumnAdaptationsPage(Page page)
    {
        if (!isColumnAdaptationRequired) {
            return page;
        }
        if (page == null) {
            return null;
        }
        int batchSize = page.getPositionCount();
        Block[] blocks = new Block[columnAdaptations.size()];
        long startRowId = parquetReader.lastBatchStartRow();
        for (int columnChannel = 0; columnChannel < columnAdaptations.size(); columnChannel++) {
            blocks[columnChannel] = columnAdaptations.get(columnChannel).getBlock(page, startRowId);
        }
        return new Page(batchSize, blocks);
    }

    static TrinoException handleException(ParquetDataSourceId dataSourceId, Exception exception)
    {
        if (exception instanceof TrinoException) {
            return (TrinoException) exception;
        }
        if (exception instanceof ParquetCorruptionException) {
            return new TrinoException(HIVE_BAD_DATA, exception);
        }
        return new TrinoException(HIVE_CURSOR_ERROR, format("Failed to read Parquet file: %s", dataSourceId), exception);
    }

    private static boolean isColumnAdaptationRequired(List<ColumnAdaptation> columnAdaptations)
    {
        // If no synthetic columns are added and the source columns are in order, no adaptations are required
        for (int columnChannel = 0; columnChannel < columnAdaptations.size(); columnChannel++) {
            ColumnAdaptation column = columnAdaptations.get(columnChannel);
            if (column instanceof SourceColumn) {
                int delegateChannel = ((SourceColumn) column).getSourceChannel();
                if (columnChannel != delegateChannel) {
                    return true;
                }
            }
            else {
                return true;
            }
        }
        return false;
    }

    private interface ColumnAdaptation
    {
        Block getBlock(Page sourcePage, long startRowId);
    }

    private static class NullColumn
            implements ColumnAdaptation
    {
        private final Block nullBlock;

        private NullColumn(Type type)
        {
            this.nullBlock = type.createBlockBuilder(null, 1, 0)
                    .appendNull()
                    .build();
        }

        @Override
        public Block getBlock(Page sourcePage, long startRowId)
        {
            return RunLengthEncodedBlock.create(nullBlock, sourcePage.getPositionCount());
        }
    }

    private static class SourceColumn
            implements ColumnAdaptation
    {
        private final int sourceChannel;

        private SourceColumn(int sourceChannel)
        {
            checkArgument(sourceChannel >= 0, "sourceChannel is negative");
            this.sourceChannel = sourceChannel;
        }

        @Override
        public Block getBlock(Page sourcePage, long startRowId)
        {
            return sourcePage.getBlock(sourceChannel);
        }

        public int getSourceChannel()
        {
            return sourceChannel;
        }
    }

    private static class ConstantColumn
            implements ColumnAdaptation
    {
        private final Block singleValueBlock;

        private ConstantColumn(Block singleValueBlock)
        {
            checkArgument(singleValueBlock.getPositionCount() == 1, "ConstantColumnAdaptation singleValueBlock may only contain one position");
            this.singleValueBlock = singleValueBlock;
        }

        @Override
        public Block getBlock(Page sourcePage, long startRowId)
        {
            return RunLengthEncodedBlock.create(singleValueBlock, sourcePage.getPositionCount());
        }
    }

    private static class RowIndexColumn
            implements ColumnAdaptation
    {
        @Override
        public Block getBlock(Page sourcePage, long startRowId)
        {
            return createRowNumberBlock(startRowId, sourcePage.getPositionCount());
        }
    }

    private static Block createRowNumberBlock(long baseIndex, int size)
    {
        long[] rowIndices = new long[size];
        for (int position = 0; position < size; position++) {
            rowIndices[position] = baseIndex + position;
        }
        return new LongArrayBlock(size, Optional.empty(), rowIndices);
    }
}
