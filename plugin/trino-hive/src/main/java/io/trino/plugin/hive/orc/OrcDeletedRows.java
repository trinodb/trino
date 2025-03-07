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
package io.trino.plugin.hive.orc;

import com.google.common.collect.ImmutableSet;
import io.trino.annotation.NotThreadSafe;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.orc.OrcCorruptionException;
import io.trino.plugin.hive.AcidInfo;
import io.trino.plugin.hive.util.AcidBucketCodec;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.security.ConnectorIdentity;
import jakarta.annotation.Nullable;

import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.ObjLongConsumer;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOfObjectArray;
import static io.trino.plugin.hive.BackgroundHiveSplitLoader.hasAttemptId;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_BAD_DATA;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static io.trino.plugin.hive.util.AcidTables.bucketFileName;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.util.Objects.requireNonNull;

@NotThreadSafe
public class OrcDeletedRows
{
    private static final int ORIGINAL_TRANSACTION_INDEX = 0;
    private static final int BUCKET_ID_INDEX = 1;
    private static final int ROW_ID_INDEX = 2;

    private static final long DELETED_ROWS_MEMORY_INCREASE_YIELD_THREHOLD = 32 * 1204 * 1024;

    private final String sourceFileName;
    private final OrcDeleteDeltaPageSourceFactory pageSourceFactory;
    private final TrinoFileSystem fileSystem;
    private final AcidInfo acidInfo;
    private final OptionalInt bucketNumber;
    private final LocalMemoryContext memoryUsage;

    private State state = State.NOT_LOADED;
    @Nullable
    private Loader loader;
    @Nullable
    private Set<RowId> deletedRows;

    private enum State {
        NOT_LOADED,
        LOADING,
        LOADED,
        CLOSED
    }

    public OrcDeletedRows(
            String sourceFileName,
            OrcDeleteDeltaPageSourceFactory pageSourceFactory,
            ConnectorIdentity identity,
            TrinoFileSystemFactory fileSystemFactory,
            AcidInfo acidInfo,
            OptionalInt bucketNumber,
            AggregatedMemoryContext memoryContext)
    {
        this.sourceFileName = requireNonNull(sourceFileName, "sourceFileName is null");
        this.pageSourceFactory = requireNonNull(pageSourceFactory, "pageSourceFactory is null");
        this.fileSystem = requireNonNull(fileSystemFactory, "fileSystemFactory is null").create(identity);
        this.acidInfo = requireNonNull(acidInfo, "acidInfo is null");
        this.bucketNumber = requireNonNull(bucketNumber, "bucketNumber is null");
        this.memoryUsage = memoryContext.newLocalMemoryContext(OrcDeletedRows.class.getSimpleName());
    }

    public SourcePage maskPage(SourcePage sourcePage, OptionalLong startRowId)
    {
        return new OrcAcidMaskedSourcePage(sourcePage, startRowId);
    }

    @NotThreadSafe
    private class OrcAcidMaskedSourcePage
            implements SourcePage
    {
        private final OptionalLong startRowId;
        private final SourcePage sourcePage;
        private boolean deleteMaskApplied;

        public OrcAcidMaskedSourcePage(SourcePage sourcePage, OptionalLong startRowId)
        {
            this.sourcePage = requireNonNull(sourcePage, "sourcePage is null");
            this.startRowId = requireNonNull(startRowId, "startRowId is null");
        }

        @Override
        public int getPositionCount()
        {
            applyDeleteMaskIfNecessary();
            return sourcePage.getPositionCount();
        }

        @Override
        public long getSizeInBytes()
        {
            if (!deleteMaskApplied) {
                return 0;
            }
            return sourcePage.getSizeInBytes();
        }

        @Override
        public long getRetainedSizeInBytes()
        {
            return sourcePage.getRetainedSizeInBytes();
        }

        @Override
        public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer)
        {
            sourcePage.retainedBytesForEachPart(consumer);
        }

        @Override
        public int getChannelCount()
        {
            return sourcePage.getChannelCount();
        }

        @Override
        public Block getBlock(int channel)
        {
            applyDeleteMaskIfNecessary();
            return sourcePage.getBlock(channel);
        }

        @Override
        public Page getPage()
        {
            applyDeleteMaskIfNecessary();
            return sourcePage.getPage();
        }

        @Override
        public void selectPositions(int[] positions, int offset, int size)
        {
            applyDeleteMaskIfNecessary();
            sourcePage.selectPositions(positions, offset, size);
        }

        private void applyDeleteMaskIfNecessary()
        {
            if (deleteMaskApplied) {
                return;
            }
            applyDeleteMask();
        }

        private void applyDeleteMask()
        {
            verify(!deleteMaskApplied, "mask already applied");
            Set<RowId> deletedRows = getDeletedRows();
            if (deletedRows.isEmpty()) {
                deleteMaskApplied = true;
                return;
            }

            int[] validPositions = new int[sourcePage.getPositionCount()];
            int validPositionsIndex = 0;
            for (int position = 0; position < sourcePage.getPositionCount(); position++) {
                RowId rowId = getRowId(position);
                if (!deletedRows.contains(rowId)) {
                    validPositions[validPositionsIndex] = position;
                    validPositionsIndex++;
                }
            }
            sourcePage.selectPositions(validPositions, 0, validPositionsIndex);
            deleteMaskApplied = true;
        }

        private RowId getRowId(int position)
        {
            verify(sourcePage != null, "sourcePage is null");
            long originalTransaction;
            long row;
            int bucket;
            int statementId;
            if (startRowId.isPresent()) {
                // original transaction ID is always 0 for original file row delete delta.
                originalTransaction = 0;
                // For original files set the bucket number to original bucket number if table was bucketed or to 0 if it was not.
                // Set statement Id to 0.
                // Verified manually that this is consistent with Hive 3.1 behavior.
                bucket = bucketNumber.orElse(0);
                statementId = 0;
                // In case of original files, calculate row ID is start row ID of the page + current position in the page
                row = startRowId.getAsLong() + position;
            }
            else {
                originalTransaction = BIGINT.getLong(sourcePage.getBlock(ORIGINAL_TRANSACTION_INDEX), position);
                int encodedBucketValue = INTEGER.getInt(sourcePage.getBlock(BUCKET_ID_INDEX), position);
                AcidBucketCodec bucketCodec = AcidBucketCodec.forBucket(encodedBucketValue);
                bucket = bucketCodec.decodeWriterId(encodedBucketValue);
                statementId = bucketCodec.decodeStatementId(encodedBucketValue);
                row = BIGINT.getLong(sourcePage.getBlock(ROW_ID_INDEX), position);
            }
            return new RowId(originalTransaction, bucket, statementId, row);
        }
    }

    private Set<RowId> getDeletedRows()
    {
        checkState(state == State.LOADED, "expected LOADED state but was %s", state);
        verify(deletedRows != null, "deleted rows null despite LOADED state");
        return deletedRows;
    }

    /**
     * Triggers loading of deleted rows ids. Single call to the method may load just part of ids.
     * If more ids to be loaded remain,  method returns false and should be called once again.
     * Final call will return true and the loaded ids can be consumed via {@link #maskPage(SourcePage, OptionalLong)}
     *
     * @return true when fully loaded, and false if this method should be called again
     */
    public boolean loadOrYield()
    {
        checkState(state != State.CLOSED, "already closed");

        if (state == State.NOT_LOADED) {
            loader = new Loader();
            state = State.LOADING;
        }

        if (state == State.LOADING) {
            verify(loader != null, "loader not set despite LOADING state");
            Optional<Set<RowId>> loadedRowIds = loader.loadOrYield();
            if (loadedRowIds.isPresent()) {
                deletedRows = loadedRowIds.get();
                try {
                    loader.close();
                }
                catch (IOException e) {
                    throw new TrinoException(HIVE_CURSOR_ERROR, "Failed to close deletedRows loader", e);
                }
                loader = null;
                state = State.LOADED;
            }
        }

        return state == State.LOADED;
    }

    public void close()
            throws IOException
    {
        if (state == State.CLOSED) {
            return;
        }
        if (loader != null) {
            loader.close();
            loader = null;
        }
        state = State.CLOSED;
    }

    private class Loader
    {
        private final ImmutableSet.Builder<RowId> deletedRowsBuilder = ImmutableSet.builder();
        private int deletedRowsBuilderSize;
        @Nullable
        private Iterator<String> deleteDeltaDirectories;
        @Nullable
        private ConnectorPageSource currentPageSource;
        @Nullable
        private Location currentPath;
        @Nullable
        private SourcePage currentPage;
        private int currentPagePosition;

        public Optional<Set<RowId>> loadOrYield()
        {
            long initialMemorySize = retainedMemorySize(deletedRowsBuilderSize, currentPage);

            if (deleteDeltaDirectories == null) {
                deleteDeltaDirectories = acidInfo.getDeleteDeltaDirectories().iterator();
            }

            while (deleteDeltaDirectories.hasNext() || currentPageSource != null) {
                try {
                    if (currentPageSource == null) {
                        String deleteDeltaDirectory = deleteDeltaDirectories.next();
                        currentPath = createPath(acidInfo, deleteDeltaDirectory, sourceFileName);
                        TrinoInputFile inputFile = fileSystem.newInputFile(currentPath);
                        if (inputFile.exists()) {
                            currentPageSource = pageSourceFactory.createPageSource(inputFile).orElseGet(EmptyPageSource::new);
                        }
                    }

                    if (currentPageSource != null) {
                        while (!currentPageSource.isFinished() || currentPage != null) {
                            if (currentPage == null) {
                                currentPage = currentPageSource.getNextSourcePage();
                                currentPagePosition = 0;
                            }

                            if (currentPage == null) {
                                continue;
                            }

                            while (currentPagePosition < currentPage.getPositionCount()) {
                                long originalTransaction = BIGINT.getLong(currentPage.getBlock(ORIGINAL_TRANSACTION_INDEX), currentPagePosition);
                                int encodedBucketValue = INTEGER.getInt(currentPage.getBlock(BUCKET_ID_INDEX), currentPagePosition);
                                AcidBucketCodec bucketCodec = AcidBucketCodec.forBucket(encodedBucketValue);
                                int bucket = bucketCodec.decodeWriterId(encodedBucketValue);
                                int statement = bucketCodec.decodeStatementId(encodedBucketValue);
                                long row = BIGINT.getLong(currentPage.getBlock(ROW_ID_INDEX), currentPagePosition);
                                RowId rowId = new RowId(originalTransaction, bucket, statement, row);
                                deletedRowsBuilder.add(rowId);
                                deletedRowsBuilderSize++;
                                currentPagePosition++;

                                if (deletedRowsBuilderSize % 1000 == 0) {
                                    long currentMemorySize = retainedMemorySize(deletedRowsBuilderSize, currentPage);
                                    if (currentMemorySize - initialMemorySize >= DELETED_ROWS_MEMORY_INCREASE_YIELD_THREHOLD) {
                                        memoryUsage.setBytes(currentMemorySize);
                                        return Optional.empty();
                                    }
                                }
                            }
                            currentPage = null;
                        }
                        currentPageSource.close();
                        currentPageSource = null;
                    }
                }
                catch (TrinoException e) {
                    throw e;
                }
                catch (OrcCorruptionException e) {
                    throw new TrinoException(HIVE_BAD_DATA, "Failed to read ORC delete delta file: " + currentPath, e);
                }
                catch (RuntimeException | IOException e) {
                    throw new TrinoException(HIVE_CURSOR_ERROR, "Failed to read ORC delete delta file: " + currentPath, e);
                }
            }

            Set<RowId> builtDeletedRows = deletedRowsBuilder.build();
            memoryUsage.setBytes(retainedMemorySize(builtDeletedRows.size(), null));
            return Optional.of(builtDeletedRows);
        }

        public void close()
                throws IOException
        {
            if (currentPageSource != null) {
                currentPageSource.close();
                currentPageSource = null;
            }
        }
    }

    private static long retainedMemorySize(int rowCount, @Nullable SourcePage currentPage)
    {
        long pageSize = (currentPage != null) ? currentPage.getRetainedSizeInBytes() : 0;
        return sizeOfObjectArray(rowCount) + ((long) rowCount * RowId.INSTANCE_SIZE) + pageSize;
    }

    private static Location createPath(AcidInfo acidInfo, String deleteDeltaDirectory, String fileName)
    {
        Location directory = Location.of(acidInfo.getPartitionLocation()).appendPath(deleteDeltaDirectory);

        // When direct insert is enabled base and delta directories contain bucket_[id]_[attemptId] files
        // but delete delta directories contain bucket files without attemptId so we have to remove it from filename.
        if (hasAttemptId(fileName)) {
            return directory.appendPath(fileName.substring(0, fileName.lastIndexOf('_')));
        }

        if (!acidInfo.getOriginalFiles().isEmpty()) {
            // Original file format is different from delete delta, construct delete delta file path from bucket ID of original file.
            return bucketFileName(directory, acidInfo.getBucketId());
        }
        return directory.appendPath(fileName);
    }

    private static class RowId
    {
        public static final int INSTANCE_SIZE = instanceSize(RowId.class);

        private final long originalTransaction;
        private final int bucket;
        private final int statementId;
        private final long rowId;

        public RowId(long originalTransaction, int bucket, int statementId, long rowId)
        {
            this.originalTransaction = originalTransaction;
            this.bucket = bucket;
            this.statementId = statementId;
            this.rowId = rowId;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            RowId other = (RowId) o;
            return originalTransaction == other.originalTransaction &&
                    bucket == other.bucket &&
                    statementId == other.statementId &&
                    rowId == other.rowId;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(originalTransaction, bucket, statementId, rowId);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("originalTransaction", originalTransaction)
                    .add("bucket", bucket)
                    .add("statementId", statementId)
                    .add("rowId", rowId)
                    .toString();
        }
    }
}
