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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.plugin.iceberg.delete.EqualityDeleteFilter.EqualityDeleteFilterBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.Schema;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_BAD_DATA;
import static io.trino.plugin.iceberg.IcebergUtil.getColumnHandle;
import static io.trino.plugin.iceberg.IcebergUtil.schemaFromHandles;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Future.State.SUCCESS;

public class DeleteManager
{
    private final TypeManager typeManager;
    private final Map<List<Integer>, EqualityDeleteFilterBuilder> equalityDeleteFiltersBySchema = new ConcurrentHashMap<>();

    public DeleteManager(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    public Optional<RowPredicate> getDeletePredicate(
            String dataFilePath,
            long dataSequenceNumber,
            List<DeleteFile> deleteFiles,
            List<IcebergColumnHandle> readColumns,
            Schema tableSchema,
            OptionalLong startRowPosition,
            OptionalLong endRowPosition,
            DeletionVectorReader deletionVectorReader,
            DeletePageSourceProvider deletePageSourceProvider)
    {
        if (deleteFiles.isEmpty()) {
            return Optional.empty();
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
                case DATA -> throw new VerifyException("DATA is not delete file type");
            }
        }

        // by spec "Readers can safely ignore position delete files if there is a DV for a data file"
        Optional<DeletionVector> deletionVector = deletionVectorFile
                .map(deletionVectorReader::read)
                .or(() -> PositionDeleteReader.readPositionDeletes(
                        dataFilePath,
                        positionDeleteFiles,
                        startRowPosition,
                        endRowPosition,
                        deletePageSourceProvider,
                        typeManager));

        Optional<RowPredicate> positionDeletes = deletionVector
                .map(vector -> {
                    int filePositionChannel = IntStream.range(0, readColumns.size())
                            .filter(i -> readColumns.get(i).isRowPositionColumn())
                            .boxed()
                            .collect(onlyElement());
                    return (page, position) -> {
                        long filePosition = BIGINT.getLong(page.getBlock(filePositionChannel), position);
                        return !vector.isRowDeleted(filePosition);
                    };
                });

        Optional<RowPredicate> equalityDeletes = createEqualityDeleteFilter(equalityDeleteFiles, tableSchema, deletePageSourceProvider).stream()
                .map(filter -> filter.createPredicate(readColumns, dataSequenceNumber))
                .reduce(RowPredicate::and);

        if (positionDeletes.isPresent() && equalityDeletes.isPresent()) {
            return Optional.of(positionDeletes.get().and(equalityDeletes.get()));
        }
        return positionDeletes.or(() -> equalityDeletes);
    }

    public long getEstimatedSizeInBytes()
    {
        return equalityDeleteFiltersBySchema.values().stream()
                .mapToLong(EqualityDeleteFilterBuilder::getEstimatedSizeInBytes)
                .sum();
    }

    public interface DeletionVectorReader
    {
        DeletionVector read(DeleteFile deleteFile);
    }

    private List<EqualityDeleteFilter> createEqualityDeleteFilter(List<DeleteFile> equalityDeleteFiles, Schema schema, DeletePageSourceProvider deletePageSourceProvider)
    {
        if (equalityDeleteFiles.isEmpty()) {
            return List.of();
        }

        // The equality delete files can be loaded in parallel. There may be multiple split threads attempting to load the
        // same files. The current thread will only load a file if it is not already being loaded by another thread.
        List<ListenableFuture<?>> pendingLoads = new ArrayList<>();
        Set<EqualityDeleteFilterBuilder> deleteFilters = new HashSet<>();
        for (DeleteFile deleteFile : equalityDeleteFiles) {
            List<Integer> fieldIds = deleteFile.equalityFieldIds();
            verify(!fieldIds.isEmpty(), "equality field IDs are missing");
            List<IcebergColumnHandle> deleteColumns = fieldIds.stream()
                    .map(id -> getColumnHandle(schema.findField(id), typeManager))
                    .collect(toImmutableList());

            // each file can have a different set of columns for the equality delete, so we need to create a new builder for each set of columns
            EqualityDeleteFilterBuilder builder = equalityDeleteFiltersBySchema.computeIfAbsent(fieldIds, _ -> EqualityDeleteFilter.builder(schemaFromHandles(deleteColumns)));
            deleteFilters.add(builder);

            ListenableFuture<?> loadFuture = builder.readEqualityDeletes(deleteFile, deleteColumns, deletePageSourceProvider);
            if (loadFuture.state() != SUCCESS) {
                pendingLoads.add(loadFuture);
            }
        }

        // Wait loads happening in other threads
        try {
            Futures.allAsList(pendingLoads).get();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        catch (ExecutionException e) {
            // Since execution can happen on another thread, it is not safe to unwrap the exception
            throw new TrinoException(ICEBERG_BAD_DATA, "Failed to load equality deletes", e);
        }

        return deleteFilters.stream()
                .map(EqualityDeleteFilterBuilder::build)
                .toList();
    }
}
