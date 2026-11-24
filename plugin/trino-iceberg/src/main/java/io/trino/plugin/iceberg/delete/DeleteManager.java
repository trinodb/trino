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

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.spi.BlocksHashFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.Schema;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.stream.IntStream;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.addExceptionCallback;
import static io.airlift.concurrent.MoreFutures.getDone;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_BAD_DATA;
import static io.trino.plugin.iceberg.IcebergUtil.getProjectedColumns;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class DeleteManager
{
    private final TypeManager typeManager;
    private final BlocksHashFactory blocksHashFactory;
    private final Runnable memoryUsageReporter;
    private final Executor executor;
    private final Map<List<Integer>, EqualityDeleteFilterBuilder> equalityDeleteFiltersBySchema = new ConcurrentHashMap<>();

    public DeleteManager(TypeManager typeManager, BlocksHashFactory blocksHashFactory, Runnable memoryUsageReporter, Executor executor)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.blocksHashFactory = requireNonNull(blocksHashFactory, "blocksHashFactory is null");
        this.memoryUsageReporter = requireNonNull(memoryUsageReporter, "memoryUsageReporter is null");
        this.executor = requireNonNull(executor, "executor is null");
    }

    /**
     * Starts loading the delete files that apply to the split and returns a future of the page filter that
     * removes the deleted rows. All failures are reported through the future instead of throwing inline.
     * Position deletes use {@link DeleteLoadContext#positionDeletePageSourceProvider}; deletion vectors use {@link DeleteLoadContext#deletionVectorReader}.
     * Equality delete reads use {@link DeleteLoadContext#equalityDeletePageSourceProvider} and are deduped and shared across splits.
     */
    public ListenableFuture<Optional<PageFilter>> createDeletePageFilter(
            String dataFilePath,
            OptionalLong equalityDeleteSequenceNumber,
            List<DeleteFile> deleteFiles,
            List<IcebergColumnHandle> readColumns,
            Schema tableSchema,
            OptionalLong startRowPosition,
            OptionalLong endRowPosition,
            DeleteLoadContext context)
    {
        ListenableFuture<Optional<PageFilter>> pageFilter = Futures.submitAsync(
                () -> startLoad(
                        dataFilePath,
                        equalityDeleteSequenceNumber,
                        deleteFiles,
                        readColumns,
                        tableSchema,
                        startRowPosition,
                        endRowPosition,
                        context),
                directExecutor());

        return Futures.catching(pageFilter, Exception.class, e -> {
            throwIfInstanceOf(e, TrinoException.class);
            throw new TrinoException(ICEBERG_BAD_DATA, "Failed to load delete files for " + dataFilePath, e);
        }, directExecutor());
    }

    private ListenableFuture<Optional<PageFilter>> startLoad(
            String dataFilePath,
            OptionalLong equalityDeleteSequenceNumber,
            List<DeleteFile> deleteFiles,
            List<IcebergColumnHandle> readColumns,
            Schema tableSchema,
            OptionalLong startRowPosition,
            OptionalLong endRowPosition,
            DeleteLoadContext context)
    {
        if (deleteFiles.isEmpty()) {
            return immediateFuture(Optional.empty());
        }

        Optional<DeleteFile> deletionVectorFile = Optional.empty();
        List<DeleteFile> positionDeleteFiles = new ArrayList<>();
        List<DeleteFile> equalityDeleteFiles = new ArrayList<>();
        for (DeleteFile deleteFile : deleteFiles) {
            switch (deleteFile.content()) {
                case POSITION_DELETES -> {
                    if (deleteFile.isDeletionVector()) {
                        if (deletionVectorFile.isPresent()) {
                            throw new TrinoException(ICEBERG_BAD_DATA, "Multiple deletion vector files found for data file: " + dataFilePath);
                        }
                        deletionVectorFile = Optional.of(deleteFile);
                    }
                    else {
                        positionDeleteFiles.add(deleteFile);
                    }
                }
                case EQUALITY_DELETES -> equalityDeleteFiles.add(deleteFile);
                case DATA, DATA_MANIFEST, DELETE_MANIFEST -> throw new VerifyException("DATA is not delete file type");
            }
        }

        // fast fail before reading any delete files
        if (!equalityDeleteFiles.isEmpty() && equalityDeleteSequenceNumber.isEmpty()) {
            throw new TrinoException(ICEBERG_BAD_DATA, "Cannot apply equality deletes: Iceberg manifest is missing dataSequenceNumber for " + dataFilePath);
        }
        // each file can have a different set of columns for the equality delete, so we need to create a new builder for each set of columns;
        // the key fields are projected themselves, so a key nested in a row column is read through a dereference
        Map<List<Integer>, EqualityDeleteFilterBuilder> equalityDeleteBuilders = equalityDeleteFiles.stream()
                .map(DeleteFile::equalityFieldIds)
                .distinct()
                .collect(toImmutableMap(identity(), fieldIds -> {
                    verify(!fieldIds.isEmpty(), "equality field IDs are missing");
                    return equalityDeleteFiltersBySchema.computeIfAbsent(
                            fieldIds,
                            _ -> EqualityDeleteFilter.builder(getProjectedColumns(tableSchema, typeManager, ImmutableSet.copyOf(fieldIds)), blocksHashFactory, executor));
                }));

        ListenableFuture<Optional<DeletionVector>> deletionVectorFuture = readDeletionVector(deletionVectorFile, context.deletionVectorReader(), context.decodeExecutor());
        // by spec "Readers can safely ignore position delete files if there is a DV for a data file"
        List<DeleteFile> positionDeleteFilesToRead = deletionVectorFile.isPresent() ? ImmutableList.of() : positionDeleteFiles;
        ListenableFuture<Optional<DeletionVector>> positionDeleteFuture = PositionDeleteReader.readPositionDeletes(
                dataFilePath,
                positionDeleteFilesToRead,
                startRowPosition,
                endRowPosition,
                context.positionDeletePageSourceProvider(),
                typeManager,
                executor,
                context.decodeExecutor(),
                context.memoryContext());
        ListenableFuture<List<EqualityDeleteFilter>> equalityDeleteFuture = createEqualityDeleteFilters(equalityDeleteFiles, equalityDeleteBuilders, context.equalityDeletePageSourceProvider(), context.decodeExecutor());

        ListenableFuture<Optional<PageFilter>> result = Futures.whenAllSucceed(deletionVectorFuture, positionDeleteFuture, equalityDeleteFuture)
                .call(
                        () -> createPageFilter(
                                readColumns,
                                getDone(deletionVectorFuture),
                                getDone(positionDeleteFuture),
                                getDone(equalityDeleteFuture),
                                equalityDeleteSequenceNumber,
                                context.memoryContext()),
                        // an equality delete load can complete on the thread of another split, so build the filter on this split's thread
                        context.decodeExecutor());
        addExceptionCallback(result, () -> {
            deletionVectorFuture.cancel(true);
            positionDeleteFuture.cancel(true);
            equalityDeleteFuture.cancel(true);
        });
        return result;
    }

    private ListenableFuture<Optional<DeletionVector>> readDeletionVector(Optional<DeleteFile> deletionVectorFile, DeletionVectorReader deletionVectorReader, Executor decodeExecutor)
    {
        if (deletionVectorFile.isEmpty()) {
            return immediateFuture(Optional.empty());
        }
        DeleteFile deleteFile = deletionVectorFile.get();
        DeletionVector.Builder deletionVector = DeletionVector.builder();
        ListenableFuture<?> load = new ReferenceCountedLoader<SerializedDeletionVector>().load(
                deleteFile.path(),
                executor,
                () -> deletionVectorReader.read(deleteFile),
                decodeExecutor,
                serialized -> deletionVector.deserialize(serialized.content()));
        return Futures.transform(load, _ -> deletionVector.build(), directExecutor());
    }

    private static Optional<PageFilter> createPageFilter(
            List<IcebergColumnHandle> readColumns,
            Optional<DeletionVector> deletionVector,
            Optional<DeletionVector> positionDeleteVector,
            List<EqualityDeleteFilter> equalityDeleteFilters,
            OptionalLong equalityDeleteSequenceNumber,
            MemoryContext memoryContext)
    {
        // a DV supersedes position delete files, so at most one of the two is present
        Optional<DeletionVector> deletedPositions = deletionVector.or(() -> positionDeleteVector);
        // the vector is retained by the page filter until the page source memory context is closed
        deletedPositions.ifPresent(vector -> memoryContext.setBytes(vector.retainedSizeInBytes()));

        Optional<PageFilter> positionDeletes = deletedPositions
                .map(vector -> {
                    int filePositionChannel = IntStream.range(0, readColumns.size())
                            .filter(i -> readColumns.get(i).isRowPositionColumn())
                            .boxed()
                            .collect(onlyElement());
                    return PageFilter.of((page, position) -> {
                        long filePosition = BIGINT.getLong(page.getBlock(filePositionChannel), position);
                        return !vector.isRowDeleted(filePosition);
                    });
                });

        ImmutableList.Builder<PageFilter> deleteFiltersBuilder = ImmutableList.builder();
        positionDeletes.ifPresent(deleteFiltersBuilder::add);
        if (!equalityDeleteFilters.isEmpty()) {
            long splitDataSequenceNumber = equalityDeleteSequenceNumber.orElseThrow();
            equalityDeleteFilters.stream()
                    .map(filter -> filter.createPageFilter(readColumns, splitDataSequenceNumber))
                    .forEach(deleteFiltersBuilder::add);
        }

        return PageFilter.allOf(deleteFiltersBuilder.build());
    }

    public long getEstimatedSizeInBytes()
    {
        return equalityDeleteFiltersBySchema.values().stream()
                .mapToLong(EqualityDeleteFilterBuilder::getEstimatedSizeInBytes)
                .sum();
    }

    public record SerializedDeletionVector(Slice content, Closeable memoryReservation)
            implements Closeable
    {
        public SerializedDeletionVector
        {
            requireNonNull(content, "content is null");
            requireNonNull(memoryReservation, "memoryReservation is null");
        }

        @Override
        public void close()
                throws IOException
        {
            memoryReservation.close();
        }
    }

    public record DeleteLoadContext(
            DeletionVectorReader deletionVectorReader,
            DeletePageSourceProvider positionDeletePageSourceProvider,
            DeletePageSourceProvider equalityDeletePageSourceProvider,
            Executor decodeExecutor,
            MemoryContext memoryContext)
    {
        public DeleteLoadContext
        {
            requireNonNull(deletionVectorReader, "deletionVectorReader is null");
            requireNonNull(positionDeletePageSourceProvider, "positionDeletePageSourceProvider is null");
            requireNonNull(equalityDeletePageSourceProvider, "equalityDeletePageSourceProvider is null");
            requireNonNull(decodeExecutor, "decodeExecutor is null");
            requireNonNull(memoryContext, "memoryContext is null");
        }
    }

    public interface DeletionVectorReader
    {
        /**
         * Reads the serialized deletion vector. Does IO only, so it runs on the delete loading pool.
         */
        SerializedDeletionVector read(DeleteFile deleteFile);
    }

    private ListenableFuture<List<EqualityDeleteFilter>> createEqualityDeleteFilters(
            List<DeleteFile> equalityDeleteFiles,
            Map<List<Integer>, EqualityDeleteFilterBuilder> equalityDeleteBuilders,
            DeletePageSourceProvider equalityDeletePageSourceProvider,
            Executor decodeExecutor)
    {
        if (equalityDeleteFiles.isEmpty()) {
            return immediateFuture(ImmutableList.of());
        }

        // The equality delete files can be loaded in parallel. There may be multiple splits requesting the same files.
        // Each file is loaded once and the other callers share that load.
        ListenableFuture<Void> loads = BoundedLoader.loadAll(
                equalityDeleteFiles,
                file -> Futures.transform(
                        equalityDeleteBuilders.get(file.equalityFieldIds()).readEqualityDeletes(file, equalityDeletePageSourceProvider, decodeExecutor),
                        _ -> {
                            memoryUsageReporter.run();
                            return null;
                        },
                        directExecutor()));
        return Futures.transform(
                loads,
                _ -> equalityDeleteBuilders.values().stream()
                        .map(EqualityDeleteFilterBuilder::build)
                        .collect(toImmutableList()),
                directExecutor());
    }
}
