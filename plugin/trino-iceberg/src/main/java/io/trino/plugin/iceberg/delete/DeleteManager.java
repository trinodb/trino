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
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.TypeManager;
import jakarta.validation.constraints.NotNull;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Conversions;
import org.roaringbitmap.longlong.LongBitmapDataProvider;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.iceberg.IcebergUtil.getColumnHandle;
import static io.trino.plugin.iceberg.IcebergUtil.schemaFromHandles;
import static io.trino.plugin.iceberg.delete.PositionDeleteFilter.readPositionDeletes;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.apache.iceberg.FileContent.EQUALITY_DELETES;
import static org.apache.iceberg.FileContent.POSITION_DELETES;
import static org.apache.iceberg.MetadataColumns.DELETE_FILE_PATH;
import static org.apache.iceberg.MetadataColumns.DELETE_FILE_POS;

public class DeleteManager
{
    private final ConcurrentHashMap<Set<Integer>, EqualityDeleteFilter> equalityDeleteFiltersBySchema = new ConcurrentHashMap<>();

    /**
     * @return an optional {@link RowPredicate} that indicates if a row is deleted.
     *         The resulting predicate is not thread-safe, but it is safe to create a predicate per thread by calling this method on each thread.
     */
    public Optional<RowPredicate> getDeletePredicate(Schema tableSchema,
            String path,
            long dataSequenceNumber,
            List<DeleteFile> deletes,
            Optional<Long> startRowPosition,
            Optional<Long> endRowPosition,
            List<IcebergColumnHandle> readColumns,
            TypeManager typeManager,
            DeletePageSourceProvider deletePageSourceProvider)
    {
        if (deletes.isEmpty()) {
            return Optional.empty();
        }
        List<DeleteFilter> deleteFilters = readDeletes(
                tableSchema,
                path,
                deletes,
                startRowPosition,
                endRowPosition,
                typeManager,
                deletePageSourceProvider);

        return deleteFilters.stream()
                .map(filter -> filter.createPredicate(readColumns, dataSequenceNumber))
                .reduce(EqualityDeleteFilter::combinePredicates);
    }

    public interface DeletePageSourceProvider
    {
        ConnectorPageSource openDeletes(DeleteFile delete,
                List<IcebergColumnHandle> deleteColumns,
                TupleDomain<IcebergColumnHandle> tupleDomain);
    }

    private List<DeleteFilter> readDeletes(
            Schema tableSchema,
            String dataFilePath,
            List<DeleteFile> deleteFiles,
            Optional<Long> startRowPosition,
            Optional<Long> endRowPosition,
            TypeManager typeManager,
            DeletePageSourceProvider deletePageSourceProvider)
    {
        verify(startRowPosition.isPresent() == endRowPosition.isPresent(), "startRowPosition and endRowPosition must be specified together");

        Slice targetPath = utf8Slice(dataFilePath);
        List<DeleteFilter> filters = new ArrayList<>();
        LongBitmapDataProvider deletedRows = new Roaring64Bitmap();

        IcebergColumnHandle deleteFilePath = getColumnHandle(DELETE_FILE_PATH, typeManager);
        IcebergColumnHandle deleteFilePos = getColumnHandle(DELETE_FILE_POS, typeManager);
        List<IcebergColumnHandle> deleteColumns = ImmutableList.of(deleteFilePath, deleteFilePos);
        TupleDomain<IcebergColumnHandle> deleteDomain = TupleDomain.fromFixedValues(ImmutableMap.of(deleteFilePath, NullableValue.of(VARCHAR, targetPath)));
        if (startRowPosition.isPresent()) {
            Range positionRange = Range.range(deleteFilePos.getType(), startRowPosition.get(), true, endRowPosition.get(), true);
            TupleDomain<IcebergColumnHandle> positionDomain = TupleDomain.withColumnDomains(ImmutableMap.of(deleteFilePos, Domain.create(ValueSet.ofRanges(positionRange), false)));
            deleteDomain = deleteDomain.intersect(positionDomain);
        }

        ArrayList<DeleteFile> filesToReprocess = new ArrayList<>();

        for (DeleteFile delete : deleteFiles) {
            if (delete.content() == POSITION_DELETES) {
                loadPositionDeleteFile(startRowPosition, endRowPosition, deletePageSourceProvider, targetPath, deletedRows, deleteColumns, deleteDomain, delete);
            }
            else if (delete.content() == EQUALITY_DELETES) {
                loadEqualityDeleteFile(delete, tableSchema, typeManager, deletePageSourceProvider, filesToReprocess, false);
            }
            else {
                throw new VerifyException("Unknown delete content: " + delete.content());
            }
        }

        for (DeleteFile delete : filesToReprocess) {
            loadEqualityDeleteFile(delete, tableSchema, typeManager, deletePageSourceProvider, null, true);
        }

        if (!deletedRows.isEmpty()) {
            filters.add(new PositionDeleteFilter(deletedRows));
        }
        equalityDeleteFiltersBySchema.forEach((k, f) -> filters.add(f));

        return filters;
    }

    private static void loadPositionDeleteFile(Optional<Long> startRowPosition,
            Optional<Long> endRowPosition,
            DeletePageSourceProvider deletePageSourceProvider,
            Slice targetPath,
            LongBitmapDataProvider deletedRows,
            List<IcebergColumnHandle> deleteColumns,
            TupleDomain<IcebergColumnHandle> deleteDomain,
            DeleteFile delete)
    {
        if (startRowPosition.isPresent()) {
            byte[] lowerBoundBytes = delete.getLowerBounds().get(DELETE_FILE_POS.fieldId());
            Optional<Long> positionLowerBound = Optional.ofNullable(lowerBoundBytes)
                    .map(bytes -> Conversions.fromByteBuffer(DELETE_FILE_POS.type(), ByteBuffer.wrap(bytes)));

            byte[] upperBoundBytes = delete.getUpperBounds().get(DELETE_FILE_POS.fieldId());
            Optional<Long> positionUpperBound = Optional.ofNullable(upperBoundBytes)
                    .map(bytes -> Conversions.fromByteBuffer(DELETE_FILE_POS.type(), ByteBuffer.wrap(bytes)));

            if ((positionLowerBound.isPresent() && positionLowerBound.get() > endRowPosition.get()) ||
                    (positionUpperBound.isPresent() && positionUpperBound.get() < startRowPosition.get())) {
                return;
            }
        }

        try (ConnectorPageSource pageSource =
                deletePageSourceProvider.openDeletes(delete, deleteColumns, deleteDomain)) {
            readPositionDeletes(pageSource, targetPath, deletedRows);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void loadEqualityDeleteFile(
            DeleteFile delete,
            Schema tableSchema,
            TypeManager typeManager,
            DeletePageSourceProvider deletePageSourceProvider,
            ArrayList<DeleteFile> filesToReprocess,
            boolean forceWait)
    {
        List<Integer> fieldIds = delete.equalityFieldIds();
        verify(!fieldIds.isEmpty(), "equality field IDs are missing");
        List<IcebergColumnHandle> columns = fieldIds.stream()
                .map(id -> getColumnHandle(tableSchema.findField(id), typeManager))
                .collect(toImmutableList());
        var deleteSchema = schemaFromHandles(columns);
        EqualityDeleteFilter equalityDeleteFilter = getFilterByDeleteSchema(deleteSchema, delete.equalityFieldIds());
        coordinateDeleteFileLoading(delete, deletePageSourceProvider, filesToReprocess, forceWait, equalityDeleteFilter, columns);
    }

    /**
     * Loads the delete file while taking into account other splits that may be loading it in parallel.
     * A {@link io.trino.plugin.iceberg.delete.EqualityDeleteFilter.FileToken} represents a file being loaded.
     * It may exist in multiple sequence numbers so if:
     * - We're already loading the file in a newer sequence number, don't load the file here
     * - We already loaded the file in an older sequence number, reload the file with the new sequence number
     */
    private static void coordinateDeleteFileLoading(DeleteFile delete,
            DeletePageSourceProvider deletePageSourceProvider,
            ArrayList<DeleteFile> filesToReprocess,
            boolean forceWait,
            EqualityDeleteFilter equalityDeleteFilter,
            List<IcebergColumnHandle> columns)
    {
        long deleteFileSequenceNumber = delete.getDataSequenceNumber();
        EqualityDeleteFilter.FileToken fileToken = equalityDeleteFilter.getLoadFileToken(delete.path(), deleteFileSequenceNumber);
        boolean shouldLoadNow = fileToken.acquireLoading();
        if (!shouldLoadNow && !forceWait) {
            // Files that are currently being loaded by another split, skip them and load another file, return to these files after loading other files to ensure they were loaded successfully
            if (fileToken.shouldLoad(deleteFileSequenceNumber)) {
                filesToReprocess.add(delete);
            }
        }
        else {
            synchronized (fileToken) {
                if (fileToken.shouldLoad(deleteFileSequenceNumber)) {
                    fileToken.setDataVersion(deleteFileSequenceNumber);
                    try (ConnectorPageSource pageSource = deletePageSourceProvider.openDeletes(delete, columns, TupleDomain.all())) {
                        equalityDeleteFilter.loadEqualityDeletes(pageSource, columns, deleteFileSequenceNumber);
                        fileToken.setLoaded();
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                    finally {
                        fileToken.releaseLoading();
                    }
                }
            }
        }
    }

    @NotNull
    private EqualityDeleteFilter getFilterByDeleteSchema(Schema deleteSchema, List<Integer> equalityDeleteFieldIds)
    {
        return equalityDeleteFiltersBySchema.computeIfAbsent(ImmutableSet.copyOf(equalityDeleteFieldIds),
                (s) -> new EqualityDeleteFilter(deleteSchema));
    }
}
