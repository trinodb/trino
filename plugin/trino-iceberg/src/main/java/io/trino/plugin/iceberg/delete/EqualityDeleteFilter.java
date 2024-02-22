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

import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.type.Type;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeMap;
import org.apache.iceberg.util.StructProjection;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.iceberg.IcebergUtil.schemaFromHandles;
import static java.util.Objects.requireNonNull;

public final class EqualityDeleteFilter
        implements DeleteFilter
{
    /**
     * Box class for data sequence number to avoid auto-boxing when adding values to the delete map
     */
    private static class DataSequenceNumber
    {
        private long dataSequenceNumber;

        public DataSequenceNumber(long dataSequenceNumber)
        {
            this.dataSequenceNumber = dataSequenceNumber;
        }
    }

    public static final class FileToken
    {
        private long dataVersion;
        private boolean isLoaded;
        private boolean isLoading;
        private final Object lockToken;

        private FileToken(long dataVersion)
        {
            this.dataVersion = dataVersion;
            this.isLoaded = false;
            this.isLoading = false;
            this.lockToken = new Object();
        }

        public boolean shouldLoad(long deleteDataSequenceNumber)
        {
            synchronized (lockToken) {
                return dataVersion < deleteDataSequenceNumber || (!isLoaded && dataVersion == deleteDataSequenceNumber);
            }
        }

        public boolean acquireLoading()
        {
            synchronized (lockToken) {
                if (isLoading) {
                    return false;
                }
                else {
                    isLoading = true;
                    return true;
                }
            }
        }

        public void setDataVersion(long newDataVersion)
        {
            synchronized (lockToken) {
                this.dataVersion = newDataVersion;
            }
        }

        public void setLoaded()
        {
            synchronized (lockToken) {
                this.isLoaded = true;
            }
        }

        public void releaseLoading()
        {
            synchronized (lockToken) {
                isLoading = false;
            }
        }
    }

    private final Schema deleteSchema;
    private final StructLikeMap<DataSequenceNumber> deleteMap;
    private final ReentrantReadWriteLock readWriteLock;
    private final HashMap<String, FileToken> loadedFiles;

    public EqualityDeleteFilter(Schema deleteSchema)
    {
        this.deleteSchema = requireNonNull(deleteSchema, "deleteSchema is null");
        this.deleteMap = StructLikeMap.create(deleteSchema.asStruct());
        this.loadedFiles = new HashMap<>();
        this.readWriteLock = new ReentrantReadWriteLock();
    }

    public FileToken getLoadFileToken(String fileName, long deleteFileSequenceNumber)
    {
        synchronized (loadedFiles) {
            return loadedFiles.computeIfAbsent(fileName, k -> new FileToken(deleteFileSequenceNumber));
        }
    }

    @Override
    public RowPredicate createPredicate(List<IcebergColumnHandle> columns, long splitDataSequenceNumber)
    {
        Type[] types = columns.stream()
                .map(IcebergColumnHandle::getType)
                .toArray(Type[]::new);

        Schema fileSchema = schemaFromHandles(columns);
        boolean hasRequiredColumns = columns.size() >= fileSchema.columns().size();
        for (Types.NestedField column : deleteSchema.columns()) {
            hasRequiredColumns = hasRequiredColumns && fileSchema.findField(column.fieldId()) != null;
        }
        if (!hasRequiredColumns) {
            // If we don't have all the required columns this delete filter can't be applied.
            // The iceberg page source provider is responsible for making sure that we have all the required columns when a delete filter should be applied
            throw new TrinoException(ICEBERG_CANNOT_OPEN_SPLIT, "columns list doesn't contain all equality delete columns");
        }
        else {
            StructProjection projection = StructProjection.create(fileSchema, deleteSchema);

            RowPredicate predicate = (page, position) -> {
                StructProjection wrapped = projection.wrap(new LazyTrinoRow(types, page, position));
                DataSequenceNumber maxDeleteVersion = deleteMap.get(wrapped);
                return maxDeleteVersion == null || maxDeleteVersion.dataSequenceNumber <= splitDataSequenceNumber;
            };
            return new LockingRowPredicate(predicate, readWriteLock);
        }
    }

    public void loadEqualityDeletes(ConnectorPageSource pageSource,
            List<IcebergColumnHandle> columns,
            long deleteFileSequenceNumber)
    {
        Type[] types = columns.stream()
                .map(IcebergColumnHandle::getType)
                .toArray(Type[]::new);

        DataSequenceNumber boxedDataSequenceNumber = new DataSequenceNumber(deleteFileSequenceNumber);
        while (!pageSource.isFinished()) {
            Page page = pageSource.getNextPage();
            if (page == null) {
                continue;
            }

            readWriteLock.writeLock().lock();
            try {
                for (int position = 0; position < page.getPositionCount(); position++) {
                    TrinoRow key = new TrinoRow(types, page, position);
                    DataSequenceNumber existingSequenceNumber = deleteMap.put(key, boxedDataSequenceNumber);
                    if (existingSequenceNumber != null && existingSequenceNumber.dataSequenceNumber > deleteFileSequenceNumber) {
                        deleteMap.put(key, existingSequenceNumber);
                    }
                }
            }
            finally {
                readWriteLock.writeLock().unlock();
            }
        }
    }

    static RowPredicate combinePredicates(RowPredicate first, RowPredicate second)
    {
        if (first instanceof LockingRowPredicate) {
            return combineLockingPredicate((LockingRowPredicate) first, second);
        }
        else if (second instanceof LockingRowPredicate) {
            return combineLockingPredicate((LockingRowPredicate) second, first);
        }
        return (page, position) -> first.test(page, position) && second.test(page, position);
    }

    private static RowPredicate combineLockingPredicate(LockingRowPredicate first, RowPredicate second)
    {
        return new LockingRowPredicate(combinePredicates(first.underlying, second), first.readWriteLock);
    }

    private static class LockingRowPredicate
            implements RowPredicate
    {
        private final RowPredicate underlying;
        private final ReentrantReadWriteLock readWriteLock;

        private LockingRowPredicate(RowPredicate underlying,
                ReentrantReadWriteLock readWriteLock)
        {
            this.underlying = underlying;
            this.readWriteLock = readWriteLock;
        }

        @Override
        public boolean test(Page page, int position)
        {
            return underlying.test(page, position);
        }

        @Override
        public Page filterPage(Page page)
        {
            readWriteLock.readLock().lock();
            try {
                int positionCount = page.getPositionCount();
                int[] retained = new int[positionCount];
                int retainedCount = 0;
                for (int position = 0; position < positionCount; position++) {
                    if (test(page, position)) {
                        retained[retainedCount] = position;
                        retainedCount++;
                    }
                }
                if (retainedCount == positionCount) {
                    return page;
                }
                return page.getPositions(retained, 0, retainedCount);
            }
            finally {
                readWriteLock.readLock().unlock();
            }
        }
    }
}
