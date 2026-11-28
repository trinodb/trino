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
package io.trino.plugin.deltalake.statistics;

import com.google.common.cache.Cache;
import com.google.common.cache.Weigher;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import io.airlift.slice.SizeOf;
import io.trino.cache.CacheStatsMBean;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.plugin.deltalake.DeltaLakeColumnHandle;
import io.trino.plugin.deltalake.DeltaLakeConfig;
import io.trino.plugin.deltalake.DeltaLakeTableCredentials;
import io.trino.plugin.deltalake.DeltaLakeTableHandle;
import io.trino.plugin.deltalake.transactionlog.TableSnapshot;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.DoubleRange;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.TypeManager;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.cache.CacheUtils.invalidateAllIf;
import static io.trino.cache.CacheUtils.uncheckedCacheGet;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.isExtendedStatisticsEnabled;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class CachingTableStatisticsProvider
        implements DeltaLakeTableStatisticsProvider
{
    private static final long TABLE_STATISTICS_INSTANCE_SIZE = instanceSize(TableStatistics.class) + instanceSize(Estimate.class);
    private static final long COLUMN_STATISTICS_INSTANCE_SIZE = instanceSize(ColumnStatistics.class) + 4 * instanceSize(Estimate.class) + instanceSize(DoubleRange.class);

    private final FileBasedTableStatisticsProvider delegate;
    private final Cache<CacheKey, TableStatistics> cache;

    @Inject
    public CachingTableStatisticsProvider(
            TypeManager typeManager,
            TransactionLogAccess transactionLogAccess,
            CachingExtendedStatisticsAccess statisticsAccess,
            DeltaLakeConfig config)
    {
        this(new FileBasedTableStatisticsProvider(typeManager, transactionLogAccess, statisticsAccess), config);
    }

    public CachingTableStatisticsProvider(FileBasedTableStatisticsProvider delegate, DeltaLakeConfig config)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.cache = EvictableCacheBuilder.newBuilder()
                .expireAfterWrite(config.getTableStatisticsCacheTtl().toMillis(), MILLISECONDS)
                .weigher((Weigher<CacheKey, TableStatistics>) (key, value) -> Ints.saturatedCast(key.retainedSizeInBytes() + retainedSizeInBytes(value)))
                .maximumWeight(config.getTableStatisticsCacheMaxRetainedSize().toBytes())
                .shareNothingWhenDisabled()
                .recordStats()
                .build();
    }

    @Managed
    @Nested
    public CacheStatsMBean getTableStatisticsCacheStats()
    {
        return new CacheStatsMBean(cache);
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, DeltaLakeTableHandle tableHandle, TableSnapshot tableSnapshot, Optional<DeltaLakeTableCredentials> tableCredentials)
    {
        CacheKey key = new CacheKey(
                tableHandle.getSchemaTableName(),
                tableHandle.getLocation(),
                tableSnapshot.getVersion(),
                tableHandle.getEnforcedPartitionConstraint(),
                tableHandle.getNonPartitionConstraint(),
                tableHandle.getProjectedColumns().map(columns -> columns.stream()
                        .map(DeltaLakeColumnHandle::baseColumnName)
                        .collect(toImmutableSet())),
                isExtendedStatisticsEnabled(session));
        return uncheckedCacheGet(cache, key, () -> delegate.getTableStatistics(session, tableHandle, tableSnapshot, tableCredentials));
    }

    public void flushCache()
    {
        cache.invalidateAll();
    }

    @Override
    public void invalidateCache(SchemaTableName schemaTableName, Optional<String> tableLocation)
    {
        tableLocation.ifPresent(location -> invalidateAllIf(cache, key -> key.location().equals(location)));
        invalidateAllIf(cache, key -> key.schemaTableName().equals(schemaTableName));
    }

    private static long retainedSizeInBytes(TableStatistics statistics)
    {
        return TABLE_STATISTICS_INSTANCE_SIZE + estimatedSizeOf(
                statistics.getColumnStatistics(),
                column -> ((DeltaLakeColumnHandle) column).retainedSizeInBytes(),
                _ -> COLUMN_STATISTICS_INSTANCE_SIZE);
    }

    private record CacheKey(
            SchemaTableName schemaTableName,
            String location,
            long version,
            TupleDomain<DeltaLakeColumnHandle> enforcedPartitionConstraint,
            TupleDomain<DeltaLakeColumnHandle> nonPartitionConstraint,
            Optional<Set<String>> projectedColumnNames,
            boolean extendedStatisticsEnabled)
    {
        private static final int INSTANCE_SIZE = instanceSize(CacheKey.class);

        private CacheKey
        {
            requireNonNull(schemaTableName, "schemaTableName is null");
            requireNonNull(location, "location is null");
            requireNonNull(enforcedPartitionConstraint, "enforcedPartitionConstraint is null");
            requireNonNull(nonPartitionConstraint, "nonPartitionConstraint is null");
            requireNonNull(projectedColumnNames, "projectedColumnNames is null");
        }

        public long retainedSizeInBytes()
        {
            return INSTANCE_SIZE
                    + schemaTableName.getRetainedSizeInBytes()
                    + estimatedSizeOf(location)
                    + enforcedPartitionConstraint.getRetainedSizeInBytes(DeltaLakeColumnHandle::retainedSizeInBytes)
                    + nonPartitionConstraint.getRetainedSizeInBytes(DeltaLakeColumnHandle::retainedSizeInBytes)
                    + projectedColumnNames.map(names -> estimatedSizeOf(names, SizeOf::estimatedSizeOf)).orElse(0L);
        }
    }
}
