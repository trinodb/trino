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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.plugin.hive.HiveWritableTableHandle.BucketInfo;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.util.HiveBucketing;
import io.trino.spi.Page;
import io.trino.spi.PageIndexer;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.IntArrayBlockBuilder;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_TOO_MANY_OPEN_PARTITIONS;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_WRITER_CLOSE_ERROR;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class HivePageSink
        implements ConnectorPageSink, ConnectorMergeSink
{
    private static final Logger LOG = Logger.get(HivePageSink.class);
    private static final int MAX_PAGE_POSITIONS = 4096;

    private final HiveWriterFactory writerFactory;

    private final boolean isTransactional;
    private final int[] dataColumnInputIndex; // ordinal of columns (not counting sample weight column)
    private final int[] partitionColumnsInputIndex; // ordinal of columns (not counting sample weight column)

    private final int[] bucketColumns;
    private final HiveBucketFunction bucketFunction;

    private final HiveWriterPagePartitioner pagePartitioner;

    private final int maxOpenWriters;
    private final ListeningExecutorService writeVerificationExecutor;

    private final JsonCodec<PartitionUpdate> partitionUpdateCodec;

    private final List<HiveWriter> writers = new ArrayList<>();

    private final long targetMaxFileSize;
    private final long idleWriterMinFileSize;
    private final List<Closeable> closedWriterRollbackActions = new ArrayList<>();
    private final List<Slice> partitionUpdates = new ArrayList<>();
    private final List<Callable<Object>> verificationTasks = new ArrayList<>();
    private final List<Boolean> activeWriters = new ArrayList<>();

    private final boolean isMergeSink;
    private long writtenBytes;
    private long memoryUsage;
    private long validationCpuNanos;
    private long currentOpenWriters;

    public HivePageSink(
            HiveWriterFactory writerFactory,
            List<HiveColumnHandle> inputColumns,
            AcidTransaction acidTransaction,
            Optional<BucketInfo> bucketInfo,
            PageIndexerFactory pageIndexerFactory,
            int maxOpenWriters,
            ListeningExecutorService writeVerificationExecutor,
            JsonCodec<PartitionUpdate> partitionUpdateCodec,
            ConnectorSession session)
    {
        this.writerFactory = requireNonNull(writerFactory, "writerFactory is null");

        requireNonNull(inputColumns, "inputColumns is null");

        requireNonNull(pageIndexerFactory, "pageIndexerFactory is null");

        this.isTransactional = acidTransaction.isTransactional();
        this.maxOpenWriters = maxOpenWriters;
        this.writeVerificationExecutor = requireNonNull(writeVerificationExecutor, "writeVerificationExecutor is null");
        this.partitionUpdateCodec = requireNonNull(partitionUpdateCodec, "partitionUpdateCodec is null");

        this.isMergeSink = acidTransaction.isMerge();
        requireNonNull(bucketInfo, "bucketInfo is null");
        this.pagePartitioner = new HiveWriterPagePartitioner(
                inputColumns,
                bucketInfo.isPresent(),
                pageIndexerFactory);

        // determine the input index of the partition columns and data columns
        // and determine the input index and type of bucketing columns
        ImmutableList.Builder<Integer> partitionColumns = ImmutableList.builder();
        ImmutableList.Builder<Integer> dataColumnsInputIndex = ImmutableList.builder();
        Object2IntMap<String> dataColumnNameToIdMap = new Object2IntOpenHashMap<>();
        Map<String, HiveType> dataColumnNameToTypeMap = new HashMap<>();
        for (int inputIndex = 0; inputIndex < inputColumns.size(); inputIndex++) {
            HiveColumnHandle column = inputColumns.get(inputIndex);
            if (column.isPartitionKey()) {
                partitionColumns.add(inputIndex);
            }
            else {
                dataColumnsInputIndex.add(inputIndex);
                dataColumnNameToIdMap.put(column.getName(), inputIndex);
                dataColumnNameToTypeMap.put(column.getName(), column.getHiveType());
            }
        }
        this.partitionColumnsInputIndex = Ints.toArray(partitionColumns.build());
        this.dataColumnInputIndex = Ints.toArray(dataColumnsInputIndex.build());

        if (bucketInfo.isPresent()) {
            HiveBucketing.BucketingVersion bucketingVersion = bucketInfo.get().bucketingVersion();
            int bucketCount = bucketInfo.get().bucketCount();
            bucketColumns = bucketInfo.get().bucketedBy().stream()
                    .mapToInt(dataColumnNameToIdMap::getInt)
                    .toArray();
            List<HiveType> bucketColumnTypes = bucketInfo.get().bucketedBy().stream()
                    .map(dataColumnNameToTypeMap::get)
                    .collect(toList());
            bucketFunction = new HiveBucketFunction(bucketingVersion, bucketCount, bucketColumnTypes);
        }
        else {
            bucketColumns = null;
            bucketFunction = null;
        }

        this.targetMaxFileSize = HiveSessionProperties.getTargetMaxFileSize(session).toBytes();
        this.idleWriterMinFileSize = HiveSessionProperties.getIdleWriterMinFileSize(session).toBytes();
    }

    @Override
    public long getCompletedBytes()
    {
        return writtenBytes;
    }

    @Override
    public long getMemoryUsage()
    {
        return memoryUsage;
    }

    @Override
    public long getValidationCpuNanos()
    {
        return validationCpuNanos;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return toCompletableFuture(isMergeSink ? doMergeSinkFinish() : doInsertSinkFinish());
    }

    private ListenableFuture<Collection<Slice>> doMergeSinkFinish()
    {
        ImmutableList.Builder<Slice> resultSlices = ImmutableList.builder();
        for (HiveWriter writer : writers) {
            if (writer == null) {
                continue;
            }
            writer.commit();
            MergeFileWriter mergeFileWriter = (MergeFileWriter) writer.getFileWriter();
            PartitionUpdateAndMergeResults results = mergeFileWriter.getPartitionUpdateAndMergeResults(writer.getPartitionUpdate());
            resultSlices.add(wrappedBuffer(PartitionUpdateAndMergeResults.CODEC.toJsonBytes(results)));
        }
        List<Slice> result = resultSlices.build();
        writtenBytes = writers.stream()
                .filter(Objects::nonNull)
                .mapToLong(HiveWriter::getWrittenBytes)
                .sum();
        return Futures.immediateFuture(result);
    }

    private ListenableFuture<Collection<Slice>> doInsertSinkFinish()
    {
        for (int writerIndex = 0; writerIndex < writers.size(); writerIndex++) {
            closeWriter(writerIndex);
        }
        writers.clear();

        List<Slice> result = ImmutableList.copyOf(partitionUpdates);

        if (verificationTasks.isEmpty()) {
            return Futures.immediateFuture(result);
        }

        try {
            List<ListenableFuture<?>> futures = writeVerificationExecutor.invokeAll(verificationTasks).stream()
                    .map(future -> (ListenableFuture<?>) future)
                    .collect(toList());
            return Futures.transform(Futures.allAsList(futures), input -> result, directExecutor());
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void abort()
    {
        List<Closeable> rollbackActions = Streams.concat(
                        writers.stream()
                                // writers can contain nulls if an exception is thrown when doAppend expands the writer list
                                .filter(Objects::nonNull)
                                .map(writer -> writer::rollback),
                        closedWriterRollbackActions.stream())
                .collect(toImmutableList());
        RuntimeException rollbackException = null;
        for (Closeable rollbackAction : rollbackActions) {
            try {
                rollbackAction.close();
            }
            catch (Throwable t) {
                if (rollbackException == null) {
                    rollbackException = new TrinoException(HIVE_WRITER_CLOSE_ERROR, "Error rolling back write to Hive");
                }
                rollbackException.addSuppressed(t);
            }
        }
        if (rollbackException != null) {
            throw rollbackException;
        }
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        int writeOffset = 0;
        while (writeOffset < page.getPositionCount()) {
            Page chunk = page.getRegion(writeOffset, min(page.getPositionCount() - writeOffset, MAX_PAGE_POSITIONS));
            writeOffset += chunk.getPositionCount();
            writePage(chunk);
        }
        return NOT_BLOCKED;
    }

    private void writePage(Page page)
    {
        int[] writerIndexes = getWriterIndexes(page);

        // position count for each writer
        int[] sizes = new int[writers.size()];
        for (int index : writerIndexes) {
            sizes[index]++;
        }

        // record which positions are used by which writer
        int[][] writerPositions = new int[writers.size()][];
        int[] counts = new int[writers.size()];

        for (int position = 0; position < page.getPositionCount(); position++) {
            int index = writerIndexes[position];

            int count = counts[index];
            if (count == 0) {
                writerPositions[index] = new int[sizes[index]];
            }
            writerPositions[index][count] = position;
            counts[index] = count + 1;
        }

        // invoke the writers
        Page dataPage = getDataPage(page);
        for (int index = 0; index < writerPositions.length; index++) {
            int[] positions = writerPositions[index];
            if (positions == null) {
                continue;
            }

            // If write is partitioned across multiple writers, filter page using dictionary blocks
            Page pageForWriter = dataPage;
            if (positions.length != dataPage.getPositionCount()) {
                verify(positions.length == counts[index]);
                pageForWriter = pageForWriter.getPositions(positions, 0, positions.length);
            }

            HiveWriter writer = writers.get(index);
            verify(writer != null, "Expected writer at index %s", index);

            long currentWritten = writer.getWrittenBytes();
            long currentMemory = writer.getMemoryUsage();

            writer.append(pageForWriter);

            writtenBytes += (writer.getWrittenBytes() - currentWritten);
            memoryUsage += (writer.getMemoryUsage() - currentMemory);
            // Mark this writer as active (i.e. not idle)
            activeWriters.set(index, true);
        }
    }

    private void closeWriter(int writerIndex)
    {
        HiveWriter writer = writers.get(writerIndex);
        if (writer == null) {
            return;
        }

        long currentWritten = writer.getWrittenBytes();
        long currentMemory = writer.getMemoryUsage();

        closedWriterRollbackActions.add(writer.commit());

        writtenBytes += (writer.getWrittenBytes() - currentWritten);
        memoryUsage -= currentMemory;
        validationCpuNanos += writer.getValidationCpuNanos();

        writers.set(writerIndex, null);
        currentOpenWriters--;

        PartitionUpdate partitionUpdate = writer.getPartitionUpdate();
        partitionUpdates.add(wrappedBuffer(partitionUpdateCodec.toJsonBytes(partitionUpdate)));
    }

    @Override
    public void closeIdleWriters()
    {
        // For transactional tables we don't want to split output files because there is an explicit or implicit bucketing
        // and file names have no random component (e.g. bucket_00000)
        if (bucketFunction != null || isTransactional) {
            return;
        }

        for (int writerIndex = 0; writerIndex < writers.size(); writerIndex++) {
            HiveWriter writer = writers.get(writerIndex);
            if (activeWriters.get(writerIndex) || writer == null || writer.getWrittenBytes() <= idleWriterMinFileSize) {
                activeWriters.set(writerIndex, false);
                continue;
            }
            LOG.debug("Closing writer %s with %s bytes written", writerIndex, writer.getWrittenBytes());
            closeWriter(writerIndex);
        }
    }

    private int[] getWriterIndexes(Page page)
    {
        Page partitionColumns = extractColumns(page, partitionColumnsInputIndex);
        Block bucketBlock = buildBucketBlock(page);
        int[] writerIndexes = pagePartitioner.partitionPage(partitionColumns, bucketBlock);

        // expand writers list to new size
        while (writers.size() <= pagePartitioner.getMaxIndex()) {
            writers.add(null);
            activeWriters.add(false);
        }

        // create missing writers
        for (int position = 0; position < page.getPositionCount(); position++) {
            int writerIndex = writerIndexes[position];
            HiveWriter writer = writers.get(writerIndex);
            if (writer != null) {
                // if current file not too big continue with the current writer
                // for transactional tables we don't want to split output files because there is an explicit or implicit bucketing
                // and file names have no random component (e.g. bucket_00000)
                if (bucketFunction != null || isTransactional || writer.getWrittenBytes() <= targetMaxFileSize) {
                    continue;
                }
                // close current writer
                closeWriter(writerIndex);
            }

            OptionalInt bucketNumber = OptionalInt.empty();
            if (bucketBlock != null) {
                bucketNumber = OptionalInt.of(INTEGER.getInt(bucketBlock, position));
            }

            writer = writerFactory.createWriter(partitionColumns, position, bucketNumber);

            writers.set(writerIndex, writer);
            currentOpenWriters++;
            memoryUsage += writer.getMemoryUsage();
        }
        verify(writers.size() == pagePartitioner.getMaxIndex() + 1);

        if (currentOpenWriters > maxOpenWriters) {
            throw new TrinoException(HIVE_TOO_MANY_OPEN_PARTITIONS, format("Exceeded limit of %s open writers for partitions/buckets", maxOpenWriters));
        }

        return writerIndexes;
    }

    private Page getDataPage(Page page)
    {
        if (isMergeSink) {
            return page;
        }
        Block[] blocks = new Block[dataColumnInputIndex.length];
        for (int i = 0; i < dataColumnInputIndex.length; i++) {
            int dataColumn = dataColumnInputIndex[i];
            blocks[i] = page.getBlock(dataColumn);
        }
        return new Page(page.getPositionCount(), blocks);
    }

    private Block buildBucketBlock(Page page)
    {
        if (bucketFunction == null) {
            return null;
        }

        IntArrayBlockBuilder bucketColumnBuilder = new IntArrayBlockBuilder(null, page.getPositionCount());
        Page bucketColumnsPage = extractColumns(page, bucketColumns);
        for (int position = 0; position < page.getPositionCount(); position++) {
            int bucket = bucketFunction.getBucket(bucketColumnsPage, position);
            INTEGER.writeInt(bucketColumnBuilder, bucket);
        }
        return bucketColumnBuilder.build();
    }

    private static Page extractColumns(Page page, int[] columns)
    {
        Block[] blocks = new Block[columns.length];
        for (int i = 0; i < columns.length; i++) {
            int dataColumn = columns[i];
            blocks[i] = page.getBlock(dataColumn);
        }
        return new Page(page.getPositionCount(), blocks);
    }

    @Override
    public void storeMergedRows(Page page)
    {
        checkArgument(isMergeSink, "isMergeSink is false");
        appendPage(page);
    }

    private static class HiveWriterPagePartitioner
    {
        private final PageIndexer pageIndexer;

        public HiveWriterPagePartitioner(
                List<HiveColumnHandle> inputColumns,
                boolean bucketed,
                PageIndexerFactory pageIndexerFactory)
        {
            requireNonNull(inputColumns, "inputColumns is null");
            requireNonNull(pageIndexerFactory, "pageIndexerFactory is null");

            List<Type> partitionColumnTypes = inputColumns.stream()
                    .filter(HiveColumnHandle::isPartitionKey)
                    .map(HiveColumnHandle::getType)
                    .collect(toList());

            if (bucketed) {
                partitionColumnTypes.add(INTEGER);
            }

            this.pageIndexer = pageIndexerFactory.createPageIndexer(partitionColumnTypes);
        }

        public int[] partitionPage(Page partitionColumns, Block bucketBlock)
        {
            if (bucketBlock != null) {
                Block[] blocks = new Block[partitionColumns.getChannelCount() + 1];
                for (int i = 0; i < partitionColumns.getChannelCount(); i++) {
                    blocks[i] = partitionColumns.getBlock(i);
                }
                blocks[blocks.length - 1] = bucketBlock;
                partitionColumns = new Page(partitionColumns.getPositionCount(), blocks);
            }
            return pageIndexer.indexPage(partitionColumns);
        }

        public int getMaxIndex()
        {
            return pageIndexer.getMaxIndex();
        }
    }
}
