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
package io.prestosql.plugin.hive.orc;

import com.google.common.collect.ImmutableList;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.orc.OrcCorruptionException;
import io.prestosql.orc.OrcDataSource;
import io.prestosql.orc.OrcDataSourceId;
import io.prestosql.orc.OrcRecordReader;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.type.Type;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_BAD_DATA;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class OrcPageSource
        implements ConnectorPageSource
{
    private final OrcRecordReader recordReader;
    private final List<ColumnAdaptation> columnAdaptations;
    private final OrcDataSource orcDataSource;

    private boolean closed;

    private final AggregatedMemoryContext systemMemoryContext;

    private final FileFormatDataSourceStats stats;

    public OrcPageSource(
            OrcRecordReader recordReader,
            List<ColumnAdaptation> columnAdaptations,
            OrcDataSource orcDataSource,
            AggregatedMemoryContext systemMemoryContext,
            FileFormatDataSourceStats stats)
    {
        this.recordReader = requireNonNull(recordReader, "recordReader is null");
        this.columnAdaptations = ImmutableList.copyOf(requireNonNull(columnAdaptations, "columnAdaptations is null"));
        this.orcDataSource = requireNonNull(orcDataSource, "orcDataSource is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");
    }

    @Override
    public long getCompletedBytes()
    {
        return orcDataSource.getReadBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return orcDataSource.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return closed;
    }

    @Override
    public Page getNextPage()
    {
        Page page;
        try {
            page = recordReader.nextPage();
        }
        catch (IOException | RuntimeException e) {
            closeWithSuppression(e);
            throw handleException(orcDataSource.getId(), e);
        }

        if (page == null) {
            close();
            return null;
        }

        Block[] blocks = new Block[columnAdaptations.size()];
        for (int i = 0; i < columnAdaptations.size(); i++) {
            blocks[i] = columnAdaptations.get(i).block(page);
        }
        return new Page(page.getPositionCount(), blocks);
    }

    static PrestoException handleException(OrcDataSourceId dataSourceId, Exception exception)
    {
        if (exception instanceof PrestoException) {
            return (PrestoException) exception;
        }
        if (exception instanceof OrcCorruptionException) {
            return new PrestoException(HIVE_BAD_DATA, exception);
        }
        return new PrestoException(HIVE_CURSOR_ERROR, format("Failed to read ORC file: %s", dataSourceId), exception);
    }

    @Override
    public void close()
    {
        // some hive input formats are broken and bad things can happen if you close them multiple times
        if (closed) {
            return;
        }
        closed = true;

        try {
            stats.addMaxCombinedBytesPerRow(recordReader.getMaxCombinedBytesPerRow());
            recordReader.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("orcDataSource", orcDataSource.getId())
                .add("columns", columnAdaptations)
                .toString();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return systemMemoryContext.getBytes();
    }

    private void closeWithSuppression(Throwable throwable)
    {
        requireNonNull(throwable, "throwable is null");
        try {
            close();
        }
        catch (RuntimeException e) {
            // Self-suppression not permitted
            if (throwable != e) {
                throwable.addSuppressed(e);
            }
        }
    }

    public interface ColumnAdaptation
    {
        Block block(Page sourcePage);

        static ColumnAdaptation nullColumn(Type type)
        {
            return new NullColumn(type);
        }

        static ColumnAdaptation sourceColumn(int index)
        {
            return new SourceColumn(index);
        }
    }

    private static class NullColumn
            implements ColumnAdaptation
    {
        private final Type type;
        private final Block nullBlock;

        public NullColumn(Type type)
        {
            this.type = requireNonNull(type, "type is null");
            this.nullBlock = type.createBlockBuilder(null, 1, 0)
                    .appendNull()
                    .build();
        }

        @Override
        public Block block(Page sourcePage)
        {
            return new RunLengthEncodedBlock(nullBlock, sourcePage.getPositionCount());
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("type", type)
                    .toString();
        }
    }

    private static class SourceColumn
            implements ColumnAdaptation
    {
        private final int index;

        public SourceColumn(int index)
        {
            checkArgument(index >= 0, "index is negative");
            this.index = index;
        }

        @Override
        public Block block(Page sourcePage)
        {
            return sourcePage.getBlock(index);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("index", index)
                    .toString();
        }
    }
}
