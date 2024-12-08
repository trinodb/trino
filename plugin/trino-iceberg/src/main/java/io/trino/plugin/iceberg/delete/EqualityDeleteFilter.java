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

import com.amazonaws.annotation.ThreadSafe;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.plugin.iceberg.delete.DeleteManager.DeletePageSourceProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import org.apache.iceberg.Schema;
import org.apache.iceberg.util.StructLikeWrapper;
import org.apache.iceberg.util.StructProjection;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.iceberg.IcebergUtil.schemaFromHandles;
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
        Schema fileSchema = schemaFromHandles(columns);
        if (deleteSchema.columns().stream().anyMatch(column -> fileSchema.findField(column.fieldId()) == null)) {
            throw new TrinoException(ICEBERG_CANNOT_OPEN_SPLIT, "columns list doesn't contain all equality delete columns");
        }

        StructLikeWrapper structLikeWrapper = StructLikeWrapper.forType(deleteSchema.asStruct());
        StructProjection projection = StructProjection.create(fileSchema, deleteSchema);
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
        private final Schema deleteSchema;
        private final Map<StructLikeWrapper, DataSequenceNumber> deletedRows;
        private final Map<String, ListenableFutureTask<?>> loadingFiles = new ConcurrentHashMap<>();

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
                while (!pageSource.isFinished()) {
                    SourcePage page = pageSource.getNextSourcePage();
                    if (page == null) {
                        continue;
                    }

                    for (int position = 0; position < page.getPositionCount(); position++) {
                        TrinoRow row = new TrinoRow(types, page, position);
                        deletedRows.merge(wrapper.copyFor(row), sequenceNumber, (existing, newValue) -> {
                            if (existing.dataSequenceNumber() > newValue.dataSequenceNumber()) {
                                return existing;
                            }
                            return newValue;
                        });
                    }
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
    }

    private record DataSequenceNumber(long dataSequenceNumber) {}
}
