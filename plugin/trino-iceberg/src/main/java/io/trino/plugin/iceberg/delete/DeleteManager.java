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
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.plugin.iceberg.IcebergPageSourceProvider.ReaderPageSourceWithRowPositions;
import io.trino.plugin.iceberg.delete.EqualityDeleteFilter.EqualityDeleteFilterBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Conversions;
import org.roaringbitmap.longlong.LongBitmapDataProvider;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_BAD_DATA;
import static io.trino.plugin.iceberg.IcebergUtil.getColumnHandle;
import static io.trino.plugin.iceberg.IcebergUtil.schemaFromHandles;
import static io.trino.plugin.iceberg.delete.PositionDeleteFilter.readPositionDeletes;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Future.State.SUCCESS;
import static org.apache.iceberg.MetadataColumns.DELETE_FILE_PATH;
import static org.apache.iceberg.MetadataColumns.DELETE_FILE_POS;

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
            ReaderPageSourceWithRowPositions readerPageSourceWithRowPositions,
            DeletePageSourceProvider deletePageSourceProvider)
    {
        if (deleteFiles.isEmpty()) {
            return Optional.empty();
        }

        List<DeleteFile> positionDeleteFiles = new ArrayList<>();
        List<DeleteFile> equalityDeleteFiles = new ArrayList<>();
        for (DeleteFile deleteFile : deleteFiles) {
            switch (deleteFile.content()) {
                case POSITION_DELETES -> positionDeleteFiles.add(deleteFile);
                case EQUALITY_DELETES -> equalityDeleteFiles.add(deleteFile);
                case DATA -> throw new VerifyException("DATA is not delete file type");
            }
        }

        Optional<RowPredicate> positionDeletes = createPositionDeleteFilter(dataFilePath, positionDeleteFiles, readerPageSourceWithRowPositions, deletePageSourceProvider)
                .map(filter -> filter.createPredicate(readColumns, dataSequenceNumber));
        Optional<RowPredicate> equalityDeletes = createEqualityDeleteFilter(equalityDeleteFiles, tableSchema, deletePageSourceProvider).stream()
                .map(filter -> filter.createPredicate(readColumns, dataSequenceNumber))
                .reduce(RowPredicate::and);

        if (positionDeletes.isEmpty()) {
            return equalityDeletes;
        }
        return equalityDeletes
                .map(rowPredicate -> positionDeletes.get().and(rowPredicate))
                .or(() -> positionDeletes);
    }

    public interface DeletePageSourceProvider
    {
        ConnectorPageSource openDeletes(
                DeleteFile delete,
                List<IcebergColumnHandle> deleteColumns,
                TupleDomain<IcebergColumnHandle> tupleDomain);
    }

    private Optional<DeleteFilter> createPositionDeleteFilter(
            String dataFilePath,
            List<DeleteFile> positionDeleteFiles,
            ReaderPageSourceWithRowPositions readerPageSourceWithRowPositions,
            DeletePageSourceProvider deletePageSourceProvider)
    {
        if (positionDeleteFiles.isEmpty()) {
            return Optional.empty();
        }

        Slice targetPath = utf8Slice(dataFilePath);

        Optional<Long> startRowPosition = readerPageSourceWithRowPositions.getStartRowPosition();
        Optional<Long> endRowPosition = readerPageSourceWithRowPositions.getEndRowPosition();
        verify(startRowPosition.isPresent() == endRowPosition.isPresent(), "startRowPosition and endRowPosition must be specified together");
        IcebergColumnHandle deleteFilePath = getColumnHandle(DELETE_FILE_PATH, typeManager);
        IcebergColumnHandle deleteFilePos = getColumnHandle(DELETE_FILE_POS, typeManager);
        List<IcebergColumnHandle> deleteColumns = ImmutableList.of(deleteFilePath, deleteFilePos);
        TupleDomain<IcebergColumnHandle> deleteDomain = TupleDomain.fromFixedValues(ImmutableMap.of(deleteFilePath, NullableValue.of(VARCHAR, targetPath)));
        if (startRowPosition.isPresent()) {
            Range positionRange = Range.range(deleteFilePos.getType(), startRowPosition.get(), true, endRowPosition.get(), true);
            TupleDomain<IcebergColumnHandle> positionDomain = TupleDomain.withColumnDomains(ImmutableMap.of(deleteFilePos, Domain.create(ValueSet.ofRanges(positionRange), false)));
            deleteDomain = deleteDomain.intersect(positionDomain);
        }

        LongBitmapDataProvider deletedRows = new Roaring64Bitmap();
        for (DeleteFile deleteFile : positionDeleteFiles) {
            if (shouldLoadPositionDeleteFile(deleteFile, startRowPosition, endRowPosition)) {
                try (ConnectorPageSource pageSource = deletePageSourceProvider.openDeletes(deleteFile, deleteColumns, deleteDomain)) {
                    readPositionDeletes(pageSource, targetPath, deletedRows);
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }

        if (deletedRows.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new PositionDeleteFilter(deletedRows));
    }

    private static boolean shouldLoadPositionDeleteFile(DeleteFile deleteFile, Optional<Long> startRowPosition, Optional<Long> endRowPosition)
    {
        if (startRowPosition.isEmpty()) {
            return true;
        }

        byte[] lowerBoundBytes = deleteFile.getLowerBounds().get(DELETE_FILE_POS.fieldId());
        Optional<Long> positionLowerBound = Optional.ofNullable(lowerBoundBytes)
                .map(bytes -> Conversions.fromByteBuffer(DELETE_FILE_POS.type(), ByteBuffer.wrap(bytes)));

        byte[] upperBoundBytes = deleteFile.getUpperBounds().get(DELETE_FILE_POS.fieldId());
        Optional<Long> positionUpperBound = Optional.ofNullable(upperBoundBytes)
                .map(bytes -> Conversions.fromByteBuffer(DELETE_FILE_POS.type(), ByteBuffer.wrap(bytes)));

        return (positionLowerBound.isEmpty() || positionLowerBound.get() <= endRowPosition.orElseThrow()) &&
                (positionUpperBound.isEmpty() || positionUpperBound.get() >= startRowPosition.get());
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
