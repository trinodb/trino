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
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.concurrent.MoreFutures;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.plugin.hive.util.HiveBucketing.BucketingVersion;
import io.trino.spi.Page;
import io.trino.spi.PageIndexer;
import io.trino.spi.PageIndexerFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.IntArrayBlockBuilder;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

import static com.google.common.base.Verify.verify;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_TOO_MANY_OPEN_PARTITIONS;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_WRITER_CLOSE_ERROR;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class HivePageSink
        implements ConnectorPageSink
{
    private static final Logger log = Logger.get(HivePageSink.class);

    private static final int MAX_PAGE_POSITIONS = 4096;

    private final HiveWriterFactory writerFactory;

    private final boolean isTransactional;
    private final int[] dataColumnInputIndex; // ordinal of columns (not counting sample weight column)
    private final int[] partitionColumnsInputIndex; // ordinal of columns (not counting sample weight column)

    private final int[] bucketColumns;
    private final HiveBucketFunction bucketFunction;

    private final HiveWriterPagePartitioner pagePartitioner;
    private final HdfsEnvironment hdfsEnvironment;

    private final int maxOpenWriters;
    private final ListeningExecutorService writeVerificationExecutor;

    private final JsonCodec<PartitionUpdate> partitionUpdateCodec;

    private final List<HiveWriter> writers = new ArrayList<>();

    private final ConnectorSession session;

    private final OptionalLong targetMaxFileSize;
    private final List<HiveWriter> closedWriters = new ArrayList<>();
    private final List<Slice> partitionUpdates = new ArrayList<>();
    private final List<Callable<Object>> verificationTasks = new ArrayList<>();

    private long writtenBytes;
    private long memoryUsage;
    private long validationCpuNanos;

    public HivePageSink(
            HiveWriterFactory writerFactory,
            List<HiveColumnHandle> inputColumns,
            boolean isTransactional,
            Optional<HiveBucketProperty> bucketProperty,
            PageIndexerFactory pageIndexerFactory,
            HdfsEnvironment hdfsEnvironment,
            int maxOpenWriters,
            ListeningExecutorService writeVerificationExecutor,
            JsonCodec<PartitionUpdate> partitionUpdateCodec,
            ConnectorSession session)
    {
        this.writerFactory = requireNonNull(writerFactory, "writerFactory is null");

        requireNonNull(inputColumns, "inputColumns is null");

        requireNonNull(pageIndexerFactory, "pageIndexerFactory is null");

        this.isTransactional = isTransactional;
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.maxOpenWriters = maxOpenWriters;
        this.writeVerificationExecutor = requireNonNull(writeVerificationExecutor, "writeVerificationExecutor is null");
        this.partitionUpdateCodec = requireNonNull(partitionUpdateCodec, "partitionUpdateCodec is null");

        requireNonNull(bucketProperty, "bucketProperty is null");
        this.pagePartitioner = new HiveWriterPagePartitioner(
                inputColumns,
                bucketProperty.isPresent(),
                pageIndexerFactory);

        // determine the input index of the partition columns and data columns
        // and determine the input index and type of bucketing columns
        ImmutableList.Builder<Integer> partitionColumns = ImmutableList.builder();
        ImmutableList.Builder<Integer> dataColumnsInputIndex = ImmutableList.builder();
        Object2IntMap<String> dataColumnNameToIdMap = new Object2IntOpenHashMap<>();
        Map<String, HiveType> dataColumnNameToTypeMap = new HashMap<>();
        // sample weight column is passed separately, so index must be calculated without this column
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

        if (bucketProperty.isPresent()) {
            BucketingVersion bucketingVersion = bucketProperty.get().getBucketingVersion();
            int bucketCount = bucketProperty.get().getBucketCount();
            bucketColumns = bucketProperty.get().getBucketedBy().stream()
                    .mapToInt(dataColumnNameToIdMap::get)
                    .toArray();
            List<HiveType> bucketColumnTypes = bucketProperty.get().getBucketedBy().stream()
                    .map(dataColumnNameToTypeMap::get)
                    .collect(toList());
            bucketFunction = new HiveBucketFunction(bucketingVersion, bucketCount, bucketColumnTypes);
        }
        else {
            bucketColumns = null;
            bucketFunction = null;
        }

        this.session = requireNonNull(session, "session is null");
        this.targetMaxFileSize = Optional.ofNullable(HiveSessionProperties.getTargetMaxFileSize(session)).stream().mapToLong(DataSize::toBytes).findAny();
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
        // Must be wrapped in doAs entirely
        // Implicit FileSystem initializations are possible in HiveRecordWriter#commit -> RecordWriter#close
        ListenableFuture<Collection<Slice>> result = hdfsEnvironment.doAs(session.getIdentity(), this::doFinish);
        return MoreFutures.toCompletableFuture(result);
    }

    private ListenableFuture<Collection<Slice>> doFinish()
    {
        for (HiveWriter writer : writers) {
            closeWriter(writer);
        }
        List<Slice> result = ImmutableList.copyOf(partitionUpdates);

        writtenBytes = closedWriters.stream()
                .mapToLong(HiveWriter::getWrittenBytes)
                .sum();
        validationCpuNanos = closedWriters.stream()
                .mapToLong(HiveWriter::getValidationCpuNanos)
                .sum();

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
        // Must be wrapped in doAs entirely
        // Implicit FileSystem initializations are possible in HiveRecordWriter#rollback -> RecordWriter#close
        hdfsEnvironment.doAs(session.getIdentity(), this::doAbort);
    }

    private void doAbort()
    {
        Optional<Exception> rollbackException = Optional.empty();
        for (HiveWriter writer : Iterables.concat(writers, closedWriters)) {
            // writers can contain nulls if an exception is thrown when doAppend expends the writer list
            if (writer != null) {
                try {
                    writer.rollback();
                }
                catch (Exception e) {
                    log.warn("exception '%s' while rollback on %s", e, writer);
                    rollbackException = Optional.of(e);
                }
            }
        }
        if (rollbackException.isPresent()) {
            throw new TrinoException(HIVE_WRITER_CLOSE_ERROR, "Error rolling back write to Hive", rollbackException.get());
        }
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        if (page.getPositionCount() > 0) {
            // Must be wrapped in doAs entirely
            // Implicit FileSystem initializations are possible in HiveRecordWriter#addRow or #createWriter
            hdfsEnvironment.doAs(session.getIdentity(), () -> doAppend(page));
        }

        return NOT_BLOCKED;
    }

    private void doAppend(Page page)
    {
        while (page.getPositionCount() > MAX_PAGE_POSITIONS) {
            Page chunk = page.getRegion(0, MAX_PAGE_POSITIONS);
            page = page.getRegion(MAX_PAGE_POSITIONS, page.getPositionCount() - MAX_PAGE_POSITIONS);
            writePage(chunk);
        }

        writePage(page);
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

            long currentWritten = writer.getWrittenBytes();
            long currentMemory = writer.getMemoryUsage();

            writer.append(pageForWriter);

            writtenBytes += (writer.getWrittenBytes() - currentWritten);
            memoryUsage += (writer.getMemoryUsage() - currentMemory);
        }
    }

    private void closeWriter(HiveWriter writer)
    {
        long currentWritten = writer.getWrittenBytes();
        long currentMemory = writer.getMemoryUsage();
        writer.commit();
        writtenBytes += (writer.getWrittenBytes() - currentWritten);
        memoryUsage += (writer.getMemoryUsage() - currentMemory);

        closedWriters.add(writer);

        PartitionUpdate partitionUpdate = writer.getPartitionUpdate();
        partitionUpdates.add(wrappedBuffer(partitionUpdateCodec.toJsonBytes(partitionUpdate)));
        writer.getVerificationTask()
                .map(Executors::callable)
                .ifPresent(verificationTasks::add);
    }

    private int[] getWriterIndexes(Page page)
    {
        Page partitionColumns = extractColumns(page, partitionColumnsInputIndex);
        Block bucketBlock = buildBucketBlock(page);
        int[] writerIndexes = pagePartitioner.partitionPage(partitionColumns, bucketBlock);
        if (pagePartitioner.getMaxIndex() >= maxOpenWriters) {
            throw new TrinoException(HIVE_TOO_MANY_OPEN_PARTITIONS, format("Exceeded limit of %s open writers for partitions/buckets", maxOpenWriters));
        }

        // expand writers list to new size
        while (writers.size() <= pagePartitioner.getMaxIndex()) {
            writers.add(null);
        }

        // create missing writers
        for (int position = 0; position < page.getPositionCount(); position++) {
            int writerIndex = writerIndexes[position];
            HiveWriter writer = writers.get(writerIndex);
            if (writer != null) {
                // if current file not too big continue with the current writer
                // for transactional tables we don't want to split output files because there is an explicit or implicit bucketing
                // and file names have no random component (e.g. bucket_00000)
                if (bucketFunction != null || isTransactional || writer.getWrittenBytes() <= targetMaxFileSize.orElse(Long.MAX_VALUE)) {
                    continue;
                }
                // close current writer
                closeWriter(writer);
            }

            OptionalInt bucketNumber = OptionalInt.empty();
            if (bucketBlock != null) {
                bucketNumber = OptionalInt.of(bucketBlock.getInt(position, 0));
            }
            writer = writerFactory.createWriter(partitionColumns, position, bucketNumber);
            writers.set(writerIndex, writer);
        }
        verify(writers.size() == pagePartitioner.getMaxIndex() + 1);
        verify(!writers.contains(null));

        return writerIndexes;
    }

    private Page getDataPage(Page page)
    {
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
            bucketColumnBuilder.writeInt(bucket);
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
