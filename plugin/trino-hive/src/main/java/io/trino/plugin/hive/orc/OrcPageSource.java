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
package io.trino.plugin.hive.orc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.orc.OrcCorruptionException;
import io.trino.orc.OrcDataSource;
import io.trino.orc.OrcDataSourceId;
import io.trino.orc.OrcRecordReader;
import io.trino.orc.metadata.ColumnMetadata;
import io.trino.orc.metadata.CompressionKind;
import io.trino.orc.metadata.OrcType;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.orc.OrcDeletedRows.MaskDeletedRowsFunction;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.LazyBlock;
import io.trino.spi.block.LazyBlockLoader;
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

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_BAD_DATA;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static io.trino.plugin.hive.HivePageSource.BUCKET_CHANNEL;
import static io.trino.plugin.hive.HivePageSource.ORIGINAL_TRANSACTION_CHANNEL;
import static io.trino.plugin.hive.HivePageSource.ROW_ID_CHANNEL;
import static io.trino.plugin.hive.orc.OrcFileWriter.computeBucketValue;
import static io.trino.spi.block.RowBlock.fromFieldBlocks;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class OrcPageSource
        implements ConnectorPageSource
{
    private static final Block ORIGINAL_FILE_TRANSACTION_ID_BLOCK = nativeValueToBlock(BIGINT, 0L);
    public static final String ORC_CODEC_METRIC_PREFIX = "OrcReaderCompressionFormat_";

    private final OrcRecordReader recordReader;
    private final List<ColumnAdaptation> columnAdaptations;
    private final OrcDataSource orcDataSource;
    private final Optional<OrcDeletedRows> deletedRows;

    private boolean closed;

    private final AggregatedMemoryContext memoryContext;
    private final LocalMemoryContext localMemoryContext;

    private final FileFormatDataSourceStats stats;

    // Row ID relative to all the original files of the same bucket ID before this file in lexicographic order
    private final Optional<Long> originalFileRowId;
    private final CompressionKind compressionKind;

    private long completedPositions;

    private Optional<Page> outstandingPage = Optional.empty();

    public OrcPageSource(
            OrcRecordReader recordReader,
            List<ColumnAdaptation> columnAdaptations,
            OrcDataSource orcDataSource,
            Optional<OrcDeletedRows> deletedRows,
            Optional<Long> originalFileRowId,
            AggregatedMemoryContext memoryContext,
            FileFormatDataSourceStats stats,
            CompressionKind compressionKind)
    {
        this.recordReader = requireNonNull(recordReader, "recordReader is null");
        this.columnAdaptations = ImmutableList.copyOf(requireNonNull(columnAdaptations, "columnAdaptations is null"));
        this.orcDataSource = requireNonNull(orcDataSource, "orcDataSource is null");
        this.deletedRows = requireNonNull(deletedRows, "deletedRows is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
        this.localMemoryContext = memoryContext.newLocalMemoryContext(OrcPageSource.class.getSimpleName());
        this.originalFileRowId = requireNonNull(originalFileRowId, "originalFileRowId is null");
        this.compressionKind = requireNonNull(compressionKind, "compressionKind is null");
    }

    @Override
    public long getCompletedBytes()
    {
        return orcDataSource.getReadBytes();
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return OptionalLong.of(completedPositions);
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

    public ColumnMetadata<OrcType> getColumnTypes()
    {
        return recordReader.getColumnTypes();
    }

    @Override
    public Page getNextPage()
    {
        Page page;
        try {
            if (outstandingPage.isPresent()) {
                page = outstandingPage.get();
                outstandingPage = Optional.empty();
                // Mark no bytes consumed by outstandingPage.
                // We can reset it again below if deletedRows loading yields again.
                // In such case the brief period when it is set to 0 will not be observed externally as
                // page source memory usage is only read by engine after call to getNextPage completes.
                localMemoryContext.setBytes(0);
            }
            else {
                page = recordReader.nextPage();
            }
        }
        catch (IOException | RuntimeException e) {
            closeAllSuppress(e, this);
            throw handleException(orcDataSource.getId(), e);
        }

        if (page == null) {
            close();
            return null;
        }

        completedPositions += page.getPositionCount();

        OptionalLong startRowId = originalFileRowId
                .map(rowId -> OptionalLong.of(rowId + recordReader.getFilePosition()))
                .orElseGet(OptionalLong::empty);

        if (deletedRows.isPresent()) {
            boolean deletedRowsYielded = !deletedRows.get().loadOrYield();
            if (deletedRowsYielded) {
                outstandingPage = Optional.of(page);
                localMemoryContext.setBytes(page.getRetainedSizeInBytes());
                return null; // return control to engine so it can update memory usage for query
            }
        }

        MaskDeletedRowsFunction maskDeletedRowsFunction = deletedRows
                .map(deletedRows -> deletedRows.getMaskDeletedRowsFunction(page, startRowId))
                .orElseGet(() -> MaskDeletedRowsFunction.noMaskForPage(page));
        return getColumnAdaptationsPage(page, maskDeletedRowsFunction, recordReader.getFilePosition(), startRowId);
    }

    private Page getColumnAdaptationsPage(Page page, MaskDeletedRowsFunction maskDeletedRowsFunction, long filePosition, OptionalLong startRowId)
    {
        Block[] blocks = new Block[columnAdaptations.size()];
        for (int i = 0; i < columnAdaptations.size(); i++) {
            blocks[i] = columnAdaptations.get(i).block(page, maskDeletedRowsFunction, filePosition, startRowId);
        }
        return new Page(maskDeletedRowsFunction.getPositionCount(), blocks);
    }

    static TrinoException handleException(OrcDataSourceId dataSourceId, Exception exception)
    {
        if (exception instanceof TrinoException) {
            return (TrinoException) exception;
        }
        if (exception instanceof OrcCorruptionException) {
            return new TrinoException(HIVE_BAD_DATA, exception);
        }
        return new TrinoException(HIVE_CURSOR_ERROR, format("Failed to read ORC file: %s", dataSourceId), exception);
    }

    @Override
    public void close()
    {
        // some hive input formats are broken and bad things can happen if you close them multiple times
        if (closed) {
            return;
        }
        closed = true;

        Closer closer = Closer.create();

        closer.register(() -> {
            stats.addMaxCombinedBytesPerRow(recordReader.getMaxCombinedBytesPerRow());
            recordReader.close();
        });

        closer.register(() -> {
            if (deletedRows.isPresent()) {
                deletedRows.get().close();
            }
        });

        try {
            closer.close();
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
    public long getMemoryUsage()
    {
        return memoryContext.getBytes();
    }

    @Override
    public Metrics getMetrics()
    {
        return new Metrics(ImmutableMap.of(ORC_CODEC_METRIC_PREFIX + compressionKind.name(), new LongCount(recordReader.getTotalDataLength())));
    }

    public interface ColumnAdaptation
    {
        Block block(Page sourcePage, MaskDeletedRowsFunction maskDeletedRowsFunction, long filePosition, OptionalLong startRowId);

        static ColumnAdaptation nullColumn(Type type)
        {
            return new NullColumn(type);
        }

        static ColumnAdaptation sourceColumn(int index)
        {
            return new SourceColumn(index);
        }

        static ColumnAdaptation constantColumn(Block singleValueBlock)
        {
            return new ConstantAdaptation(singleValueBlock);
        }

        static ColumnAdaptation positionColumn()
        {
            return new PositionAdaptation();
        }

        static ColumnAdaptation mergedRowColumns()
        {
            return new MergedRowAdaptation();
        }

        static ColumnAdaptation mergedRowColumnsWithOriginalFiles(long startingRowId, int bucketId)
        {
            return new MergedRowAdaptationWithOriginalFiles(startingRowId, bucketId);
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
        public Block block(Page sourcePage, MaskDeletedRowsFunction maskDeletedRowsFunction, long filePosition, OptionalLong startRowId)
        {
            return RunLengthEncodedBlock.create(nullBlock, maskDeletedRowsFunction.getPositionCount());
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
        public Block block(Page sourcePage, MaskDeletedRowsFunction maskDeletedRowsFunction, long filePosition, OptionalLong startRowId)
        {
            Block block = sourcePage.getBlock(index);
            return new LazyBlock(maskDeletedRowsFunction.getPositionCount(), new MaskingBlockLoader(maskDeletedRowsFunction, block));
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("index", index)
                    .toString();
        }

        private static final class MaskingBlockLoader
                implements LazyBlockLoader
        {
            private MaskDeletedRowsFunction maskDeletedRowsFunction;
            private Block sourceBlock;

            public MaskingBlockLoader(MaskDeletedRowsFunction maskDeletedRowsFunction, Block sourceBlock)
            {
                this.maskDeletedRowsFunction = requireNonNull(maskDeletedRowsFunction, "maskDeletedRowsFunction is null");
                this.sourceBlock = requireNonNull(sourceBlock, "sourceBlock is null");
            }

            @Override
            public Block load()
            {
                checkState(maskDeletedRowsFunction != null, "Already loaded");

                Block resultBlock = maskDeletedRowsFunction.apply(sourceBlock.getLoadedBlock());

                maskDeletedRowsFunction = null;
                sourceBlock = null;

                return resultBlock;
            }
        }
    }

    /*
     * The rowId contains the ACID columns - - originalTransaction, rowId, bucket
     */
    private static final class MergedRowAdaptation
            implements ColumnAdaptation
    {
        @Override
        public Block block(Page page, MaskDeletedRowsFunction maskDeletedRowsFunction, long filePosition, OptionalLong startRowId)
        {
            requireNonNull(page, "page is null");
            return maskDeletedRowsFunction.apply(fromFieldBlocks(
                    page.getPositionCount(),
                    Optional.empty(),
                    new Block[] {
                            page.getBlock(ORIGINAL_TRANSACTION_CHANNEL),
                            page.getBlock(BUCKET_CHANNEL),
                            page.getBlock(ROW_ID_CHANNEL)
                    }));
        }
    }

    /**
     * The rowId contains the ACID columns - - originalTransaction, rowId, bucket,
     * derived from the original file.  The transactionId is always zero,
     * and the rowIds count up from the startingRowId.
     */
    private static final class MergedRowAdaptationWithOriginalFiles
            implements ColumnAdaptation
    {
        private final long startingRowId;
        private final Block bucketBlock;

        public MergedRowAdaptationWithOriginalFiles(long startingRowId, int bucketId)
        {
            this.startingRowId = startingRowId;
            this.bucketBlock = nativeValueToBlock(INTEGER, (long) computeBucketValue(bucketId, 0));
        }

        @Override
        public Block block(Page sourcePage, MaskDeletedRowsFunction maskDeletedRowsFunction, long filePosition, OptionalLong startRowId)
        {
            int positionCount = sourcePage.getPositionCount();
            return maskDeletedRowsFunction.apply(fromFieldBlocks(
                    positionCount,
                    Optional.empty(),
                    new Block[] {
                            RunLengthEncodedBlock.create(ORIGINAL_FILE_TRANSACTION_ID_BLOCK, positionCount),
                            RunLengthEncodedBlock.create(bucketBlock, positionCount),
                            createRowNumberBlock(startingRowId, filePosition, positionCount)
                    }));
        }
    }

    private static class ConstantAdaptation
            implements ColumnAdaptation
    {
        private final Block singleValueBlock;

        public ConstantAdaptation(Block singleValueBlock)
        {
            requireNonNull(singleValueBlock, "singleValueBlock is null");
            checkArgument(singleValueBlock.getPositionCount() == 1, "ConstantColumnAdaptation singleValueBlock may only contain one position");
            this.singleValueBlock = singleValueBlock;
        }

        @Override
        public Block block(Page sourcePage, MaskDeletedRowsFunction maskDeletedRowsFunction, long filePosition, OptionalLong startRowId)
        {
            return RunLengthEncodedBlock.create(singleValueBlock, sourcePage.getPositionCount());
        }
    }

    private static class PositionAdaptation
            implements ColumnAdaptation
    {
        @Override
        public Block block(Page sourcePage, MaskDeletedRowsFunction maskDeletedRowsFunction, long filePosition, OptionalLong startRowId)
        {
            checkArgument(startRowId.isEmpty(), "startRowId should not be specified when using PositionAdaptation");
            return createRowNumberBlock(0, filePosition, sourcePage.getPositionCount());
        }
    }

    private static Block createRowNumberBlock(long startingRowId, long filePosition, int positionCount)
    {
        long[] translatedRowIds = new long[positionCount];
        for (int index = 0; index < positionCount; index++) {
            translatedRowIds[index] = startingRowId + filePosition + index;
        }
        return new LongArrayBlock(positionCount, Optional.empty(), translatedRowIds);
    }
}
