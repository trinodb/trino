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
package io.trino.plugin.iceberg.delete;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.errorprone.annotations.ThreadSafe;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.spi.BlocksHash;
import io.trino.spi.BlocksHashFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_CANNOT_OPEN_SPLIT;
import static java.util.Objects.requireNonNull;

public final class EqualityDeleteFilter
{
    private final List<Integer> deleteFieldIds;
    private final EqualityDeleteIndex index;

    private EqualityDeleteFilter(List<Integer> deleteFieldIds, EqualityDeleteIndex index)
    {
        this.deleteFieldIds = requireNonNull(deleteFieldIds, "deleteFieldIds is null");
        this.index = requireNonNull(index, "index is null");
    }

    public PageFilter createPageFilter(List<IcebergColumnHandle> columns, long splitDataSequenceNumber)
    {
        // Key by the leaf field ID: a nested key field is read through its own dereference column
        Map<Integer, Integer> dataChannelsByFieldId = new HashMap<>();
        for (int channel = 0; channel < columns.size(); channel++) {
            IcebergColumnHandle column = columns.get(channel);
            if (column.isMetadataColumn()) {
                continue;
            }
            dataChannelsByFieldId.putIfAbsent(column.getId(), channel);
        }
        // map from delete schema channel to data page channel
        int[] channels = new int[deleteFieldIds.size()];
        for (int deleteChannel = 0; deleteChannel < deleteFieldIds.size(); deleteChannel++) {
            int fieldId = deleteFieldIds.get(deleteChannel);
            Integer channel = dataChannelsByFieldId.get(fieldId);
            if (channel == null) {
                throw new TrinoException(ICEBERG_CANNOT_OPEN_SPLIT, "columns list doesn't contain equality delete field ID %s".formatted(fieldId));
            }
            channels[deleteChannel] = channel;
        }
        return new EqualityDeletePageFilter(channels, index, splitDataSequenceNumber);
    }

    private static final class EqualityDeletePageFilter
            implements PageFilter
    {
        private final int[] channels;
        private final EqualityDeleteIndex index;
        private final long splitDataSequenceNumber;

        EqualityDeletePageFilter(int[] channels, EqualityDeleteIndex index, long splitDataSequenceNumber)
        {
            this.channels = channels;
            this.index = index;
            this.splitDataSequenceNumber = splitDataSequenceNumber;
        }

        @Override
        public Positions filterPositions(SourcePage page, Positions positions)
        {
            Block[] blocks = new Block[channels.length];
            for (int i = 0; i < channels.length; i++) {
                blocks[i] = page.getBlock(channels[i]);
            }
            return index.filterPage(blocks, positions, splitDataSequenceNumber);
        }
    }

    public static EqualityDeleteFilterBuilder builder(List<IcebergColumnHandle> deleteColumns, BlocksHashFactory blocksHashFactory)
    {
        return new FlatHashEqualityDeleteFilterBuilder(deleteColumns, blocksHashFactory);
    }

    /**
     * Keys stored in a non-thread safe FlatHash and reads/writes guarded by lock
     */
    @ThreadSafe
    static final class EqualityDeleteIndex
    {
        private static final int INSTANCE_SIZE = instanceSize(EqualityDeleteIndex.class);

        private final ReadWriteLock lock = new ReentrantReadWriteLock();
        private final BlocksHash blocksHash;
        private final LongArrayList sequenceNumbers;
        private volatile long estimatedMemoryUsage;

        EqualityDeleteIndex(BlocksHash blocksHash, LongArrayList sequenceNumbers)
        {
            this.blocksHash = requireNonNull(blocksHash, "blocksHash is null");
            this.sequenceNumbers = requireNonNull(sequenceNumbers, "sequenceNumbers is null");
        }

        void insertPage(Block[] blocks, int positionCount, long deleteSequenceNumber)
        {
            lock.writeLock().lock();
            try {
                for (int position = 0; position < positionCount; position++) {
                    int groupId = blocksHash.putIfAbsent(blocks, position);
                    if (groupId == sequenceNumbers.size()) {
                        sequenceNumbers.add(deleteSequenceNumber);
                    }
                    else if (sequenceNumbers.getLong(groupId) < deleteSequenceNumber) {
                        sequenceNumbers.set(groupId, deleteSequenceNumber);
                    }
                }
                estimatedMemoryUsage = INSTANCE_SIZE + blocksHash.getEstimatedSize() + sizeOf(sequenceNumbers.elements());
            }
            finally {
                lock.writeLock().unlock();
            }
        }

        Positions filterPage(Block[] blocks, Positions positions, long splitDataSequenceNumber)
        {
            lock.readLock().lock();
            try {
                return positions.filter(position -> !isDeleted(blocks, position, splitDataSequenceNumber));
            }
            finally {
                lock.readLock().unlock();
            }
        }

        long getEstimatedSizeInBytes()
        {
            return estimatedMemoryUsage;
        }

        private boolean isDeleted(Block[] blocks, int position, long sequenceNumber)
        {
            int groupId = blocksHash.getIfPresent(blocks, position);
            return groupId >= 0 && sequenceNumbers.getLong(groupId) > sequenceNumber;
        }
    }

    @ThreadSafe
    private static final class FlatHashEqualityDeleteFilterBuilder
            implements EqualityDeleteFilterBuilder
    {
        private static final int INSTANCE_SIZE = instanceSize(FlatHashEqualityDeleteFilterBuilder.class);
        private static final int EXPECTED_SIZE = 1024;
        private static final boolean CACHE_HASH_VALUES = true;

        private final List<IcebergColumnHandle> deleteColumns;
        private final EqualityDeleteIndex index;
        private final Map<String, ListenableFutureTask<?>> loadingFiles = new ConcurrentHashMap<>();

        private FlatHashEqualityDeleteFilterBuilder(List<IcebergColumnHandle> deleteColumns, BlocksHashFactory blocksHashFactory)
        {
            this.deleteColumns = ImmutableList.copyOf(requireNonNull(deleteColumns, "deleteColumns is null"));
            List<Type> columnTypes = this.deleteColumns.stream()
                    .map(IcebergColumnHandle::getType)
                    .collect(toImmutableList());
            BlocksHash blocksHash = requireNonNull(blocksHashFactory, "blocksHashFactory is null").create(columnTypes, CACHE_HASH_VALUES, EXPECTED_SIZE);
            this.index = new EqualityDeleteIndex(blocksHash, new LongArrayList(EXPECTED_SIZE));
        }

        @Override
        public ListenableFuture<?> readEqualityDeletes(DeleteFile deleteFile, DeletePageSourceProvider deletePageSourceProvider)
        {
            // ensure only one thread loads the file
            ListenableFutureTask<?> futureTask = loadingFiles.computeIfAbsent(
                    deleteFile.path(),
                    _ -> ListenableFutureTask.create(() -> readEqualityDeletesInternal(deleteFile, deletePageSourceProvider), null));
            futureTask.run();
            return Futures.nonCancellationPropagating(futureTask);
        }

        private void readEqualityDeletesInternal(DeleteFile deleteFile, DeletePageSourceProvider deletePageSourceProvider)
        {
            long deleteSequenceNumber = deleteFile.dataSequenceNumber();
            try (ConnectorPageSource pageSource = deletePageSourceProvider.openDeletes(deleteFile, deleteColumns, TupleDomain.all())) {
                while (!pageSource.isFinished()) {
                    SourcePage page = pageSource.getNextSourcePage();
                    if (page == null) {
                        continue;
                    }

                    Block[] blocks = new Block[deleteColumns.size()];
                    for (int i = 0; i < blocks.length; i++) {
                        blocks[i] = page.getBlock(i);
                    }

                    index.insertPage(blocks, page.getPositionCount(), deleteSequenceNumber);
                }
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public EqualityDeleteFilter build()
        {
            return new EqualityDeleteFilter(deleteColumns.stream().map(IcebergColumnHandle::getId).collect(toImmutableList()), index);
        }

        @Override
        public long getEstimatedSizeInBytes()
        {
            return INSTANCE_SIZE + index.getEstimatedSizeInBytes();
        }
    }
}
