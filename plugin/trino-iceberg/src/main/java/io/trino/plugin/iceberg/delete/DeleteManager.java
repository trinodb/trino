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
import io.trino.plugin.iceberg.IcebergPageSourceProvider.ReaderPageSourceWithRowPositions;
import io.trino.plugin.iceberg.delete.EqualityDeleteFilter.EqualityDeleteFilterBuilder;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.iceberg.IcebergUtil.getColumnHandle;
import static io.trino.plugin.iceberg.IcebergUtil.schemaFromHandles;
import static io.trino.plugin.iceberg.delete.PositionDeleteFilter.readPositionDeletes;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.MetadataColumns.DELETE_FILE_PATH;
import static org.apache.iceberg.MetadataColumns.DELETE_FILE_POS;

public class DeleteManager
{
    private final TypeManager typeManager;

    public DeleteManager(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    public Optional<RowPredicate> getDeletePredicate(
            Schema tableSchema,
            List<DeleteFile> deletes,
            String path,
            List<IcebergColumnHandle> readColumns,
            ReaderPageSourceWithRowPositions readerPageSourceWithRowPositions,
            DeletePageSourceProvider deletePageSourceProvider)
    {
        List<DeleteFilter> deleteFilters = readDeletes(
                tableSchema,
                path,
                deletes,
                readerPageSourceWithRowPositions.getStartRowPosition(),
                readerPageSourceWithRowPositions.getEndRowPosition(),
                deletePageSourceProvider);
        return deleteFilters.stream()
                .map(filter -> filter.createPredicate(readColumns))
                .reduce(RowPredicate::and);
    }

    public interface DeletePageSourceProvider
    {
        ConnectorPageSource openDeletes(
                DeleteFile delete,
                List<IcebergColumnHandle> deleteColumns,
                TupleDomain<IcebergColumnHandle> tupleDomain);
    }

    private List<DeleteFilter> readDeletes(
            Schema schema,
            String dataFilePath,
            List<DeleteFile> deleteFiles,
            Optional<Long> startRowPosition,
            Optional<Long> endRowPosition,
            DeletePageSourceProvider deletePageSourceProvider)
    {
        verify(startRowPosition.isPresent() == endRowPosition.isPresent(), "startRowPosition and endRowPosition must be specified together");

        Slice targetPath = utf8Slice(dataFilePath);
        List<DeleteFilter> filters = new ArrayList<>();
        LongBitmapDataProvider deletedRows = new Roaring64Bitmap();
        Map<Set<Integer>, EqualityDeleteFilterBuilder> deletesSetByFieldIds = new HashMap<>();

        IcebergColumnHandle deleteFilePath = getColumnHandle(DELETE_FILE_PATH, typeManager);
        IcebergColumnHandle deleteFilePos = getColumnHandle(DELETE_FILE_POS, typeManager);
        List<IcebergColumnHandle> deleteColumns = ImmutableList.of(deleteFilePath, deleteFilePos);
        TupleDomain<IcebergColumnHandle> deleteDomain = TupleDomain.fromFixedValues(ImmutableMap.of(deleteFilePath, NullableValue.of(VARCHAR, targetPath)));
        if (startRowPosition.isPresent()) {
            Range positionRange = Range.range(deleteFilePos.getType(), startRowPosition.get(), true, endRowPosition.get(), true);
            TupleDomain<IcebergColumnHandle> positionDomain = TupleDomain.withColumnDomains(ImmutableMap.of(deleteFilePos, Domain.create(ValueSet.ofRanges(positionRange), false)));
            deleteDomain = deleteDomain.intersect(positionDomain);
        }

        for (DeleteFile delete : deleteFiles) {
            switch (delete.content()) {
                case POSITION_DELETES -> {
                    if (startRowPosition.isPresent()) {
                        byte[] lowerBoundBytes = delete.getLowerBounds().get(DELETE_FILE_POS.fieldId());
                        Optional<Long> positionLowerBound = Optional.ofNullable(lowerBoundBytes)
                                .map(bytes -> Conversions.fromByteBuffer(DELETE_FILE_POS.type(), ByteBuffer.wrap(bytes)));

                        byte[] upperBoundBytes = delete.getUpperBounds().get(DELETE_FILE_POS.fieldId());
                        Optional<Long> positionUpperBound = Optional.ofNullable(upperBoundBytes)
                                .map(bytes -> Conversions.fromByteBuffer(DELETE_FILE_POS.type(), ByteBuffer.wrap(bytes)));

                        if ((positionLowerBound.isPresent() && positionLowerBound.get() > endRowPosition.get()) ||
                                (positionUpperBound.isPresent() && positionUpperBound.get() < startRowPosition.get())) {
                            continue;
                        }
                    }

                    try (ConnectorPageSource pageSource = deletePageSourceProvider.openDeletes(delete, deleteColumns, deleteDomain)) {
                        readPositionDeletes(pageSource, targetPath, deletedRows);
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
                case EQUALITY_DELETES -> {
                    Set<Integer> fieldIds = ImmutableSet.copyOf(delete.equalityFieldIds());
                    verify(!fieldIds.isEmpty(), "equality field IDs are missing");
                    List<IcebergColumnHandle> columns = fieldIds.stream()
                            .map(id -> getColumnHandle(schema.findField(id), typeManager))
                            .collect(toImmutableList());

                    EqualityDeleteFilterBuilder builder = deletesSetByFieldIds.computeIfAbsent(fieldIds, _ -> EqualityDeleteFilter.builder(schemaFromHandles(columns)));
                    try (ConnectorPageSource pageSource = deletePageSourceProvider.openDeletes(delete, columns, TupleDomain.all())) {
                        builder.readEqualityDeletes(pageSource, columns);
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
                case DATA -> throw new VerifyException("DATA is not delete file type");
            }
        }

        if (!deletedRows.isEmpty()) {
            filters.add(new PositionDeleteFilter(deletedRows));
        }

        for (EqualityDeleteFilterBuilder builder : deletesSetByFieldIds.values()) {
            filters.add(builder.build());
        }

        return filters;
    }
}
