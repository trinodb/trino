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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.errorprone.annotations.ThreadSafe;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.StructLikeWrapper;
import org.apache.iceberg.util.StructProjection;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.INTEGER_INSTANCE_SIZE;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOfObjectArray;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.iceberg.IcebergMetadataColumn.isMetadataColumnId;
import static io.trino.plugin.iceberg.IcebergUtil.structTypeFromHandles;
import static java.util.Objects.requireNonNull;

public final class EqualityDeleteFilter
        implements DeleteFilter
{
    private final Schema deleteSchema;
    private final Map<StructLikeWrapper, DataSequenceNumber> deletedRows;

    private EqualityDeleteFilter(Schema deleteSchema, Map<StructLikeWrapper, DataSequenceNumber> deletedRows)
    {
        this.deleteSchema = requireNonNull(deleteSchema, "deleteSchema is null");
        this.deletedRows = requireNonNull(deletedRows, "deletedRows is null");
    }

    @Override
    public RowPredicate createPredicate(List<IcebergColumnHandle> columns, long splitDataSequenceNumber)
    {
        StructType fileStructType = structTypeFromHandles(columns.stream()
                .filter(column -> !isMetadataColumnId(column.getId())) // equality deletes don't apply to metadata columns
                .collect(toImmutableList()));
        StructType deleteStructType = deleteSchema.asStruct();
        if (deleteSchema.columns().stream().anyMatch(column -> fileStructType.field(column.fieldId()) == null)) {
            throw new TrinoException(ICEBERG_CANNOT_OPEN_SPLIT, "columns list doesn't contain all equality delete columns");
        }

        StructLikeWrapper structLikeWrapper = StructLikeWrapper.forType(deleteStructType);
        StructProjection projection = StructProjection.create(fileStructType, deleteStructType);
        Type[] types = columns.stream()
                .map(IcebergColumnHandle::getType)
                .toArray(Type[]::new);

        return (page, position) -> {
            StructProjection row = projection.wrap(new LazyTrinoRow(types, page, position));
            DataSequenceNumber maxDeleteVersion = deletedRows.get(structLikeWrapper.set(row));
            // clear reference to avoid memory leak
            structLikeWrapper.set(null);
            return maxDeleteVersion == null || maxDeleteVersion.dataSequenceNumber() <= splitDataSequenceNumber;
        };
    }

    public static EqualityDeleteFilterBuilder builder(Schema deleteSchema)
    {
        return new EqualityDeleteFilterBuilder(deleteSchema);
    }

    @ThreadSafe
    public static class EqualityDeleteFilterBuilder
    {
        private static final int SIMPLE_ENTRY_INSTANCE_SIZE = instanceSize(AbstractMap.SimpleEntry.class);
        private static final int STRUCT_LIKE_WRAPPER_INSTANCE_SIZE = instanceSize(StructLikeWrapper.class);
        private static final int TRINO_ROW_INSTANCE_SIZE = instanceSize(TrinoRow.class);
        private static final int DATA_SEQUENCE_SIZE = instanceSize(DataSequenceNumber.class);
        private static final int MAXIMUM_HASH_TABLE_CAPACITY = 1 << 30;

        private final Schema deleteSchema;
        private final Map<StructLikeWrapper, DataSequenceNumber> deletedRows;
        private final Map<String, ListenableFutureTask<?>> loadingFiles = new ConcurrentHashMap<>();
        private final LongAdder estimatedSizeInBytes = new LongAdder();

        private EqualityDeleteFilterBuilder(Schema deleteSchema)
        {
            this.deleteSchema = requireNonNull(deleteSchema, "deleteSchema is null");
            this.deletedRows = new ConcurrentHashMap<>();
        }

        public ListenableFuture<?> readEqualityDeletes(DeleteFile deleteFile, List<IcebergColumnHandle> deleteColumns, DeletePageSourceProvider deletePageSourceProvider)
        {
            verify(deleteColumns.size() == deleteSchema.columns().size(), "delete columns size doesn't match delete schema size");

            // ensure only one thread loads the file
            ListenableFutureTask<?> futureTask = loadingFiles.computeIfAbsent(
                    deleteFile.path(),
                    key -> ListenableFutureTask.create(() -> readEqualityDeletesInternal(deleteFile, deleteColumns, deletePageSourceProvider), null));
            futureTask.run();
            return Futures.nonCancellationPropagating(futureTask);
        }

        private void readEqualityDeletesInternal(DeleteFile deleteFile, List<IcebergColumnHandle> deleteColumns, DeletePageSourceProvider deletePageSourceProvider)
        {
            DataSequenceNumber sequenceNumber = new DataSequenceNumber(deleteFile.dataSequenceNumber());
            try (ConnectorPageSource pageSource = deletePageSourceProvider.openDeletes(deleteFile, deleteColumns, TupleDomain.all())) {
                Type[] types = deleteColumns.stream()
                        .map(IcebergColumnHandle::getType)
                        .toArray(Type[]::new);

                StructLikeWrapper wrapper = StructLikeWrapper.forType(deleteSchema.asStruct());
                long sizeInBytes = 0;
                long positionsCount = 0;
                AtomicInteger addedRowsCount = new AtomicInteger();
                while (!pageSource.isFinished()) {
                    SourcePage page = pageSource.getNextSourcePage();
                    if (page == null) {
                        continue;
                    }

                    for (int position = 0; position < page.getPositionCount(); position++) {
                        TrinoRow row = new TrinoRow(types, page, position);
                        deletedRows.compute(wrapper.copyFor(row), (_, existing) -> {
                            if (existing == null) {
                                addedRowsCount.incrementAndGet();
                                return sequenceNumber;
                            }

                            if (sequenceNumber.dataSequenceNumber() > existing.dataSequenceNumber()) {
                                return sequenceNumber;
                            }
                            return existing;
                        });
                    }
                    sizeInBytes += page.getSizeInBytes();
                    positionsCount += page.getPositionCount();
                }

                if (positionsCount > 0 && addedRowsCount.get() > 0) {
                    long avgDataSizePerKey = sizeInBytes / positionsCount;
                    long avgSizePerKey = STRUCT_LIKE_WRAPPER_INSTANCE_SIZE
                            + INTEGER_INSTANCE_SIZE // org.apache.iceberg.util.StructLikeWrapper.hashCode
                            + TRINO_ROW_INSTANCE_SIZE
                            + sizeOfObjectArray(types.length) // io.trino.plugin.iceberg.delete.TrinoRow.values
                            + avgDataSizePerKey;
                    estimatedSizeInBytes.add(estimatedSizeOfAddedRows(addedRowsCount.get(), avgSizePerKey));
                }
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        /**
         * Builds the EqualityDeleteFilter.
         * After building the EqualityDeleteFilter, additional rows can be added to this builder, and the filter can be rebuilt.
         */
        public EqualityDeleteFilter build()
        {
            return new EqualityDeleteFilter(deleteSchema, deletedRows);
        }

        public long getEstimatedSizeInBytes()
        {
            return estimatedSizeInBytes.longValue() + sizeOfObjectArray(tableSizeFor(deletedRows.size()));
        }

        private static long estimatedSizeOfAddedRows(int addedRowsCount, long keySize)
        {
            return addedRowsCount * (SIMPLE_ENTRY_INSTANCE_SIZE + keySize + DATA_SEQUENCE_SIZE);
        }

        /**
         * Returns a power of two table size for the given current hashmap size
         */
        private static int tableSizeFor(int size)
        {
            long tableSize = (long) (1.0 + size / 0.75f);
            if (tableSize >= MAXIMUM_HASH_TABLE_CAPACITY) {
                return MAXIMUM_HASH_TABLE_CAPACITY;
            }
            int n = -1 >>> Integer.numberOfLeadingZeros((int) (tableSize - 1));
            return (n < 0) ? 1 : (n >= MAXIMUM_HASH_TABLE_CAPACITY) ? MAXIMUM_HASH_TABLE_CAPACITY : n + 1;
        }
    }

    private record DataSequenceNumber(long dataSequenceNumber) {}
}
