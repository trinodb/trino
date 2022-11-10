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
import io.trino.parquet.reader.ParquetReaderColumn;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.metrics.Metrics;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_BAD_DATA;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ParquetPageSource
        implements ConnectorPageSource
{
    private final ParquetReader parquetReader;
    private final List<ParquetReaderColumn> parquetReaderColumns;
    private final boolean areSyntheticColumnsPresent;

    private boolean closed;
    private long completedPositions;

    public ParquetPageSource(
            ParquetReader parquetReader,
            List<ParquetReaderColumn> parquetReaderColumns)
    {
        this.parquetReader = requireNonNull(parquetReader, "parquetReader is null");
        this.parquetReaderColumns = ImmutableList.copyOf(requireNonNull(parquetReaderColumns, "parquetReaderColumns is null"));
        this.areSyntheticColumnsPresent = parquetReaderColumns.stream()
                .anyMatch(column -> column.isRowIndexColumn() || column.field().isEmpty());
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

    private Page getColumnAdaptationsPage(Page page)
    {
        if (!areSyntheticColumnsPresent) {
            return page;
        }
        if (page == null) {
            return null;
        }
        int batchSize = page.getPositionCount();
        Block[] blocks = new Block[parquetReaderColumns.size()];
        int sourceColumn = 0;
        for (int columnIndex = 0; columnIndex < parquetReaderColumns.size(); columnIndex++) {
            ParquetReaderColumn column = parquetReaderColumns.get(columnIndex);
            if (column.isRowIndexColumn()) {
                blocks[columnIndex] = getRowIndexColumn(parquetReader.lastBatchStartRow(), batchSize);
            }
            else if (column.field().isEmpty()) {
                blocks[columnIndex] = RunLengthEncodedBlock.create(column.type(), null, batchSize);
            }
            else {
                blocks[columnIndex] = page.getBlock(sourceColumn);
                sourceColumn++;
            }
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

    private static Block getRowIndexColumn(long baseIndex, int size)
    {
        long[] rowIndices = new long[size];
        for (int position = 0; position < size; position++) {
            rowIndices[position] = baseIndex + position;
        }
        return new LongArrayBlock(size, Optional.empty(), rowIndices);
    }
}
