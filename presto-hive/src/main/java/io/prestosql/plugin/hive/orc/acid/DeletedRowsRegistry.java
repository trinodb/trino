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
package io.prestosql.plugin.hive.orc.acid;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.AbstractCache;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.google.common.cache.Weigher;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.plugin.hive.DeleteDeltaLocations;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveType;
import io.prestosql.plugin.hive.orc.OrcPageSourceFactory;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.predicate.TupleDomain;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.joda.time.DateTimeZone;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.orc.AcidConstants.ACID_BUCKET_INDEX;
import static io.prestosql.orc.AcidConstants.ACID_META_COLUMNS;
import static io.prestosql.orc.AcidConstants.ACID_ORIGINAL_TRANSACTION_INDEX;
import static io.prestosql.orc.AcidConstants.ACID_ROWID_INDEX;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_BAD_DATA;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_UNKNOWN_ERROR;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;

public class DeletedRowsRegistry
{
    // variables to track cache hit rate, Guava Cache.stats() is not useful because multiple threads loading same
    // value get synchronised and only one of them load value but all of them update cache-miss count.
    // We want to know how many times did we read from cache rather
    private static final AtomicLong totalRequests = new AtomicLong();
    private static final AtomicLong requestsNeedingLoad = new AtomicLong();

    // For a query, DELETE_DELTA rows for a partition location will not change, they are cached in this cache
    private static Cache<String, List<RowId>> cache; // cachekey = QueryId_PartitionLocation_FileName

    private static final Object lock = new Object();
    private static final List<HiveColumnHandle> ACID_ROW_ID_COLUMN_HANDLES = createAcidRowIdMetaColumns();
    private static final List<DeletedRowsLoader> EMPTY_LOADERS = ImmutableList.of();
    private static final CacheStats EMPTY_STATS = new AbstractCache.SimpleStatsCounter().snapshot();

    private static Cache<String, List<LocatedFileStatus>> fileStatusCache;

    private final List<DeletedRowsLoader> loaders;
    private final String queryId;
    private final boolean deletedRowsPresent;

    private int[] validPositions;
    private AtomicReference<Set<RowId>> deletedRows = new AtomicReference<>();

    public DeletedRowsRegistry(
            Path splitPath,
            OrcPageSourceFactory pageSourceFactory,
            ConnectorSession session,
            Configuration configuration,
            DateTimeZone hiveStorageTimeZone,
            HdfsEnvironment hdfsEnvironment,
            DataSize cacheSize,
            Duration cacheTtl,
            Optional<DeleteDeltaLocations> deleteDeltaLocations)
            throws ExecutionException
    {
        if (cache == null) {
            synchronized (lock) {
                if (cache == null) {
                    cache = CacheBuilder.newBuilder()
                            .expireAfterAccess(cacheTtl.toMillis(), TimeUnit.MILLISECONDS)
                            .maximumWeight((int) cacheSize.toBytes())
                            .weigher((Weigher<String, List<RowId>>) (key, value) -> RowId.INSTANCE_SIZE * value.size())
                            .recordStats()
                            .build();
                }
            }
        }

        if (fileStatusCache == null) {
            synchronized (lock) {
                if (fileStatusCache == null) {
                    fileStatusCache = CacheBuilder.newBuilder()
                            .expireAfterAccess(cacheTtl.toMillis(), TimeUnit.MILLISECONDS)
                            .build();
                }
            }
        }

        this.deletedRowsPresent = deleteDeltaLocations.map(DeleteDeltaLocations::hadDeletedRows).orElse(false);

        if (!deletedRowsPresent) {
            loaders = EMPTY_LOADERS;
        }
        else {
            ImmutableList.Builder<DeletedRowsLoader> loaders = ImmutableList.builder();

            // First list all the delete delta files using fileStatusCache
            String partitionLocation = deleteDeltaLocations.get().getPartitionLocation();

            List<LocatedFileStatus> deleteDeltaFiles = fileStatusCache.get(session.getQueryId() + partitionLocation,
                    () -> listDeltaFiles(deleteDeltaLocations.get(), hdfsEnvironment, session, configuration));

            // Now filter out only the delete delta files which have same name as split's file.
            ImmutableList.Builder<LocatedFileStatus> usefulDeleteDeltaFiles = ImmutableList.builder();
            for (LocatedFileStatus fileStatus : deleteDeltaFiles) {
                if (fileStatus.getPath().getName().equals(splitPath.getName())) {
                    usefulDeleteDeltaFiles.add(fileStatus);
                }
            }

            loaders.add(new DeletedRowsLoader(partitionLocation,
                    usefulDeleteDeltaFiles.build(),
                    splitPath.getName(),
                    pageSourceFactory,
                    configuration,
                    session,
                    hiveStorageTimeZone));

            this.loaders = loaders.build();
        }

        this.queryId = session.getQueryId();
    }

    private List<LocatedFileStatus> listDeltaFiles(DeleteDeltaLocations deleteDeltaLocations, HdfsEnvironment hdfsEnvironment, ConnectorSession session, Configuration configuration)
            throws IOException
    {
        ImmutableList.Builder<LocatedFileStatus> builder = ImmutableList.builder();
        String partitionLocation = deleteDeltaLocations.getPartitionLocation();
        List<DeleteDeltaLocations.DeleteDeltaInfo> deleteDeltaInfos = deleteDeltaLocations.getDeleteDeltas();
        for (DeleteDeltaLocations.DeleteDeltaInfo deleteDeltaInfo : deleteDeltaInfos) {
            Path path = createPath(partitionLocation, deleteDeltaInfo);
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(session.getUser(), path, configuration);
            RemoteIterator<LocatedFileStatus> fileStatuses = fileSystem.listFiles(path, false);

            while (fileStatuses.hasNext()) {
                LocatedFileStatus fileStatus = fileStatuses.next();
                if (!AcidUtils.hiddenFileFilter.accept(fileStatus.getPath())) {
                    // Ignore hidden files
                    continue;
                }
                builder.add(fileStatus);
            }
        }
        return builder.build();
    }

    private Path createPath(String partitionLocation, DeleteDeltaLocations.DeleteDeltaInfo deleteDeltaInfo)
    {
        return new Path(partitionLocation,
                AcidUtils.deleteDeltaSubdir(
                        deleteDeltaInfo.getMinWriteId(),
                        deleteDeltaInfo.getMaxWriteId(),
                        deleteDeltaInfo.getStatementId()));
    }

    public ValidPositions getValidPositions(int positions, Block originalTransaction, Block bucket, Block rowId)
    {
        checkState(deletedRowsPresent, "Cannot create valid positions if deleted rows are not present");
        Page input = new Page(originalTransaction, bucket, rowId);
        if (validPositions == null || validPositions.length < positions) {
            validPositions = new int[positions];
        }

        if (deletedRows.get() == null) {
            deletedRows.set(loadDeletedRows());
        }
        int index = 0;
        for (int position = 0; position < positions; position++) {
            if (!deletedRows.get().contains(createRowId(input, position))) {
                validPositions[index++] = position;
            }
        }
        return new ValidPositions(validPositions, index);
    }

    private String getCacheKey(String queryId, String partitionLocation, String splitFilename)
    {
        return new StringBuilder().append(queryId).append(partitionLocation).append(splitFilename).toString();
    }

    private static RowId createRowId(Page page, int position)
    {
        return new RowId(
                page.getBlock(0).getLong(position, 0),
                page.getBlock(1).getInt(position, 0),
                page.getBlock(2).getLong(position, 0));
    }

    @VisibleForTesting
    public Set<RowId> loadDeletedRows()
    {
        ImmutableSet.Builder<RowId> allDeletedRows = ImmutableSet.builder();
        for (DeletedRowsLoader loader : loaders) {
            try {
                totalRequests.incrementAndGet();
                allDeletedRows.addAll(cache.get(getCacheKey(queryId, loader.getPartitionLocation(), loader.getSplitFilename()), loader));
            }
            catch (ExecutionException e) {
                throw new PrestoException(HIVE_UNKNOWN_ERROR, "Could not load deleted rows for location: " + loader.getPartitionLocation(), e);
            }
        }
        return allDeletedRows.build();
    }

    private static List<HiveColumnHandle> createAcidRowIdMetaColumns()
    {
        ImmutableList.Builder<HiveColumnHandle> physicalColumns = ImmutableList.builder();

        // Add only those ACID Meta columns that are used to create row identifier
        physicalColumns.add(new HiveColumnHandle(
                ACID_META_COLUMNS[ACID_ORIGINAL_TRANSACTION_INDEX],
                HiveType.HIVE_LONG,
                HiveType.HIVE_LONG.getTypeSignature(),
                ACID_ORIGINAL_TRANSACTION_INDEX,
                REGULAR,
                Optional.empty()));

        physicalColumns.add(new HiveColumnHandle(
                ACID_META_COLUMNS[ACID_BUCKET_INDEX],
                HiveType.HIVE_INT,
                HiveType.HIVE_INT.getTypeSignature(),
                ACID_BUCKET_INDEX,
                REGULAR,
                Optional.empty()));

        physicalColumns.add(new HiveColumnHandle(
                ACID_META_COLUMNS[ACID_ROWID_INDEX],
                HiveType.HIVE_LONG,
                HiveType.HIVE_LONG.getTypeSignature(),
                ACID_ROWID_INDEX,
                REGULAR,
                Optional.empty()));

        return physicalColumns.build();
    }

    public static CacheStats getCacheStats()
    {
        if (cache == null) {
            return EMPTY_STATS;
        }

        long missCount = requestsNeedingLoad.get();
        CacheStats stats = cache.stats();
        return new CacheStats(totalRequests.get() - missCount,
                missCount,
                stats.loadSuccessCount(),
                stats.loadExceptionCount(),
                stats.totalLoadTime(),
                stats.evictionCount());
    }

    public static long getCacheSize()
    {
        return cache.asMap().values().stream().map(rowIds -> (long) rowIds.size()).reduce(0L, Long::sum) * RowId.INSTANCE_SIZE;
    }

    private class DeletedRowsLoader
            implements Callable<List<RowId>>
    {
        private String partitionLocation;
        private List<LocatedFileStatus> deleteDeltaFiles;
        private String filename;
        private Configuration configuration;
        private DateTimeZone hiveStorageTimeZone;
        private OrcPageSourceFactory pageSourceFactory;
        private ConnectorSession session;

        public DeletedRowsLoader(
                String partitionLocation,
                List<LocatedFileStatus> deleteDeltaFiles,
                String filename,
                OrcPageSourceFactory pageSourceFactory,
                Configuration configuration,
                ConnectorSession session,
                DateTimeZone hiveStorageTimeZone)
        {
            this.partitionLocation = partitionLocation;
            this.deleteDeltaFiles = deleteDeltaFiles;
            this.filename = filename;
            this.pageSourceFactory = pageSourceFactory;
            this.configuration = configuration;
            this.session = session;
            this.hiveStorageTimeZone = hiveStorageTimeZone;
        }

        public String getPartitionLocation()
        {
            return partitionLocation;
        }

        public String getSplitFilename()
        {
            return filename;
        }

        @Override
        public List<RowId> call()
        {
            ImmutableList.Builder<RowId> builder = ImmutableList.builder();
            requestsNeedingLoad.incrementAndGet();
            Properties orcProperty = new Properties();
            orcProperty.setProperty(SERIALIZATION_LIB, OrcSerde.class.getName());
            for (LocatedFileStatus fileStatus : deleteDeltaFiles) {
                ConnectorPageSource pageSource = pageSourceFactory.createPageSource(
                        configuration,
                        session,
                        fileStatus.getPath(),
                        0,
                        fileStatus.getLen(),
                        fileStatus.getLen(),
                        orcProperty,
                        ACID_ROW_ID_COLUMN_HANDLES,
                        TupleDomain.all(),
                        hiveStorageTimeZone,
                        Optional.empty())
                        .orElseThrow(() -> new PrestoException(
                                HIVE_BAD_DATA,
                                "Could not create page source for delete delta ORC file: " + fileStatus.getPath()));

                while (!pageSource.isFinished()) {
                    Page page = pageSource.getNextPage();
                    if (page != null) {
                        for (int i = 0; i < page.getPositionCount(); i++) {
                            builder.add(createRowId(page, i));
                        }
                    }
                }
            }
            return builder.build();
        }
    }

    @VisibleForTesting
    public static class RowId
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(RowId.class).instanceSize() +
                ClassLayout.parseClass(Byte.class).instanceSize() * 2 +
                ClassLayout.parseClass(Integer.class).instanceSize();

        private final long originalTransaction;
        private final int bucket;
        private final long rowId;

        public RowId(long originalTransaction, int bucket, long rowId)
        {
            this.originalTransaction = originalTransaction;
            this.bucket = bucket;
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

            RowId rowId1 = (RowId) o;
            return originalTransaction == rowId1.originalTransaction &&
                    bucket == rowId1.bucket &&
                    rowId == rowId1.rowId;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(originalTransaction, bucket, rowId);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("originalTransaction", originalTransaction)
                    .add("bucket", bucket)
                    .add("rowId", rowId)
                    .toString();
        }
    }
}
