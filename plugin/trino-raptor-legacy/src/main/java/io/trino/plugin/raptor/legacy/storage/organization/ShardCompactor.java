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
package io.trino.plugin.raptor.legacy.storage.organization;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.stats.CounterStat;
import io.airlift.stats.DistributionStat;
import io.trino.orc.OrcReaderOptions;
import io.trino.plugin.raptor.legacy.metadata.ColumnInfo;
import io.trino.plugin.raptor.legacy.metadata.ShardInfo;
import io.trino.plugin.raptor.legacy.storage.StorageManager;
import io.trino.plugin.raptor.legacy.storage.StorageManagerConfig;
import io.trino.plugin.raptor.legacy.storage.StoragePageSink;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.OptionalInt;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public final class ShardCompactor
{
    private final StorageManager storageManager;

    private final CounterStat inputShards = new CounterStat();
    private final CounterStat outputShards = new CounterStat();
    private final DistributionStat inputShardsPerCompaction = new DistributionStat();
    private final DistributionStat outputShardsPerCompaction = new DistributionStat();
    private final DistributionStat compactionLatencyMillis = new DistributionStat();
    private final DistributionStat sortedCompactionLatencyMillis = new DistributionStat();
    private final OrcReaderOptions orcReaderOptions;
    private final TypeOperators typeOperators;

    @Inject
    public ShardCompactor(StorageManager storageManager, StorageManagerConfig config, TypeManager typeManager)
    {
        this(storageManager,
                config.toOrcReaderOptions(),
                typeManager.getTypeOperators());
    }

    public ShardCompactor(StorageManager storageManager, OrcReaderOptions orcReaderOptions, TypeOperators typeOperators)
    {
        this.storageManager = requireNonNull(storageManager, "storageManager is null");
        this.orcReaderOptions = requireNonNull(orcReaderOptions, "orcReaderOptions is null");
        this.typeOperators = requireNonNull(typeOperators, "typeOperators is null");
    }

    public List<ShardInfo> compact(long transactionId, OptionalInt bucketNumber, Set<UUID> uuids, List<ColumnInfo> columns)
            throws IOException
    {
        long start = System.nanoTime();
        List<Long> columnIds = columns.stream().map(ColumnInfo::getColumnId).collect(toList());
        List<Type> columnTypes = columns.stream().map(ColumnInfo::getType).collect(toList());

        StoragePageSink storagePageSink = storageManager.createStoragePageSink(transactionId, bucketNumber, columnIds, columnTypes, false);

        List<ShardInfo> shardInfos;
        try {
            shardInfos = compact(storagePageSink, bucketNumber, uuids, columnIds, columnTypes);
        }
        catch (IOException | RuntimeException e) {
            storagePageSink.rollback();
            throw e;
        }

        updateStats(uuids.size(), shardInfos.size(), nanosSince(start).toMillis());
        return shardInfos;
    }

    private List<ShardInfo> compact(StoragePageSink storagePageSink, OptionalInt bucketNumber, Set<UUID> uuids, List<Long> columnIds, List<Type> columnTypes)
            throws IOException
    {
        for (UUID uuid : uuids) {
            try (ConnectorPageSource pageSource = storageManager.getPageSource(uuid, bucketNumber, columnIds, columnTypes, TupleDomain.all(), orcReaderOptions)) {
                while (!pageSource.isFinished()) {
                    Page page = pageSource.getNextPage();
                    if (isNullOrEmptyPage(page)) {
                        continue;
                    }
                    storagePageSink.appendPage(page);
                    if (storagePageSink.isFull()) {
                        storagePageSink.flush();
                    }
                }
            }
        }
        return getFutureValue(storagePageSink.commit());
    }

    public List<ShardInfo> compactSorted(long transactionId, OptionalInt bucketNumber, Set<UUID> uuids, List<ColumnInfo> columns, List<Long> sortColumnIds, List<SortOrder> sortOrders)
            throws IOException
    {
        checkArgument(sortColumnIds.size() == sortOrders.size(), "sortColumnIds and sortOrders must be of the same size");

        long start = System.nanoTime();

        List<Long> columnIds = columns.stream().map(ColumnInfo::getColumnId).collect(toList());
        List<Type> columnTypes = columns.stream().map(ColumnInfo::getType).collect(toList());

        checkArgument(ImmutableSet.copyOf(columnIds).containsAll(sortColumnIds), "sortColumnIds must be a subset of columnIds");

        List<Integer> sortIndexes = sortColumnIds.stream()
                .map(columnIds::indexOf)
                .collect(toList());

        Queue<SortedRowSource> rowSources = new PriorityQueue<>();
        StoragePageSink outputPageSink = storageManager.createStoragePageSink(transactionId, bucketNumber, columnIds, columnTypes, false);
        PageBuilder pageBuilder = new PageBuilder(columnTypes);
        try {
            for (UUID uuid : uuids) {
                ConnectorPageSource pageSource = storageManager.getPageSource(uuid, bucketNumber, columnIds, columnTypes, TupleDomain.all(), orcReaderOptions);
                SortedRowSource rowSource = new SortedRowSource(pageSource, columnTypes, sortIndexes, sortOrders, typeOperators);
                rowSources.add(rowSource);
            }
            while (!rowSources.isEmpty()) {
                SortedRowSource rowSource = rowSources.poll();
                if (!rowSource.hasNext()) {
                    // rowSource is empty, close it
                    rowSource.close();
                    continue;
                }

                rowSource.next().appendTo(pageBuilder);

                if (pageBuilder.isFull()) {
                    outputPageSink.appendPage(pageBuilder.build());
                    pageBuilder.reset();
                }

                if (outputPageSink.isFull()) {
                    outputPageSink.flush();
                }

                rowSources.add(rowSource);
            }

            if (!pageBuilder.isEmpty()) {
                outputPageSink.appendPage(pageBuilder.build());
            }

            outputPageSink.flush();
            List<ShardInfo> shardInfos = getFutureValue(outputPageSink.commit());

            updateStats(uuids.size(), shardInfos.size(), nanosSince(start).toMillis());

            return shardInfos;
        }
        catch (IOException | RuntimeException e) {
            outputPageSink.rollback();
            throw e;
        }
        finally {
            rowSources.forEach(SortedRowSource::closeQuietly);
        }
    }

    private static class SortedRowSource
            implements Iterator<Row>, Comparable<SortedRowSource>, Closeable
    {
        private final ConnectorPageSource pageSource;
        private final List<Integer> sortIndexes;
        private final List<MethodHandle> orderingOperators;

        private Page currentPage;
        private int currentPosition;

        public SortedRowSource(ConnectorPageSource pageSource, List<Type> columnTypes, List<Integer> sortIndexes, List<SortOrder> sortOrders, TypeOperators typeOperators)
        {
            this.pageSource = requireNonNull(pageSource, "pageSource is null");
            this.sortIndexes = ImmutableList.copyOf(requireNonNull(sortIndexes, "sortIndexes is null"));
            requireNonNull(columnTypes, "columnTypes is null");
            requireNonNull(sortOrders, "sortOrders is null");

            ImmutableList.Builder<MethodHandle> orderingOperators = ImmutableList.builder();
            for (int index = 0; index < sortIndexes.size(); index++) {
                Type type = columnTypes.get(sortIndexes.get(index));
                SortOrder sortOrder = sortOrders.get(index);
                orderingOperators.add(typeOperators.getOrderingOperator(type, sortOrder, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION)));
            }
            this.orderingOperators = orderingOperators.build();

            currentPage = pageSource.getNextPage();
            currentPosition = 0;
        }

        @Override
        public boolean hasNext()
        {
            if (hasMorePositions(currentPage, currentPosition)) {
                return true;
            }

            Page page = getNextPage(pageSource);
            if (isNullOrEmptyPage(page)) {
                return false;
            }
            currentPage = page.getLoadedPage();
            currentPosition = 0;
            return true;
        }

        private static Page getNextPage(ConnectorPageSource pageSource)
        {
            Page page = null;
            while (isNullOrEmptyPage(page) && !pageSource.isFinished()) {
                page = pageSource.getNextPage();
                if (page != null) {
                    page = page.getLoadedPage();
                }
            }
            return page;
        }

        @Override
        public Row next()
        {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            Row row = new Row(currentPage, currentPosition);
            currentPosition++;
            return row;
        }

        @Override
        public int compareTo(SortedRowSource other)
        {
            if (!hasNext()) {
                return 1;
            }

            if (!other.hasNext()) {
                return -1;
            }

            try {
                for (int i = 0; i < sortIndexes.size(); i++) {
                    int channel = sortIndexes.get(i);

                    Block leftBlock = currentPage.getBlock(channel);
                    int leftBlockPosition = currentPosition;

                    Block rightBlock = other.currentPage.getBlock(channel);
                    int rightBlockPosition = other.currentPosition;

                    MethodHandle comparator = orderingOperators.get(i);
                    int compare = (int) comparator.invokeExact(leftBlock, leftBlockPosition, rightBlock, rightBlockPosition);
                    if (compare != 0) {
                        return compare;
                    }
                }
                return 0;
            }
            catch (Throwable throwable) {
                throwIfUnchecked(throwable);
                throw new TrinoException(GENERIC_INTERNAL_ERROR, throwable);
            }
        }

        private static boolean hasMorePositions(Page currentPage, int currentPosition)
        {
            return currentPage != null && currentPosition < currentPage.getPositionCount();
        }

        void closeQuietly()
        {
            try {
                close();
            }
            catch (IOException ignored) {
            }
        }

        @Override
        public void close()
                throws IOException
        {
            pageSource.close();
        }
    }

    private static class Row
    {
        private final Page page;
        private final int position;

        public Row(Page page, int position)
        {
            this.page = requireNonNull(page, "page is null");
            this.position = position;
        }

        public void appendTo(PageBuilder pageBuilder)
        {
            pageBuilder.declarePosition();
            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                Block block = page.getBlock(channel);
                BlockBuilder output = pageBuilder.getBlockBuilder(channel);
                pageBuilder.getType(channel).appendTo(block, position, output);
            }
        }
    }

    private static boolean isNullOrEmptyPage(Page nextPage)
    {
        return nextPage == null || nextPage.getPositionCount() == 0;
    }

    private void updateStats(int inputShardsCount, int outputShardsCount, long latency)
    {
        inputShards.update(inputShardsCount);
        outputShards.update(outputShardsCount);

        inputShardsPerCompaction.add(inputShardsCount);
        outputShardsPerCompaction.add(outputShardsCount);

        compactionLatencyMillis.add(latency);
    }

    @Managed
    @Nested
    public CounterStat getInputShards()
    {
        return inputShards;
    }

    @Managed
    @Nested
    public CounterStat getOutputShards()
    {
        return outputShards;
    }

    @Managed
    @Nested
    public DistributionStat getInputShardsPerCompaction()
    {
        return inputShardsPerCompaction;
    }

    @Managed
    @Nested
    public DistributionStat getOutputShardsPerCompaction()
    {
        return outputShardsPerCompaction;
    }

    @Managed
    @Nested
    public DistributionStat getCompactionLatencyMillis()
    {
        return compactionLatencyMillis;
    }

    @Managed
    @Nested
    public DistributionStat getSortedCompactionLatencyMillis()
    {
        return sortedCompactionLatencyMillis;
    }
}
