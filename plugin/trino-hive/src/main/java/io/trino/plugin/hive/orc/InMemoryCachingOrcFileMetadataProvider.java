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

import com.google.common.cache.Cache;
import com.google.common.cache.Weigher;
import io.airlift.stats.CounterStat;
import io.trino.collect.cache.EvictableCacheBuilder;
import io.trino.orc.FileStatusAwareOrcFileTail;
import io.trino.orc.FileStatusInfo;
import io.trino.orc.OrcDataSource;
import io.trino.orc.OrcDataSourceId;
import io.trino.orc.OrcFileMetadataProvider;
import io.trino.orc.OrcFileTail;
import io.trino.orc.OrcFileTailReader;
import io.trino.orc.OrcWriteValidation;
import io.trino.orc.metadata.MetadataReader;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.inject.Inject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static java.lang.String.format;

public class InMemoryCachingOrcFileMetadataProvider
        implements OrcFileMetadataProvider
{
    private final Cache<OrcDataSourceId, FileStatusAwareOrcFileTail> cache;
    private final CacheStats cacheStats;

    @Inject
    public InMemoryCachingOrcFileMetadataProvider(OrcMetadataCacheConfig metadataCacheConfig)
    {
        EvictableCacheBuilder<OrcDataSourceId, FileStatusAwareOrcFileTail> cacheBuilder = EvictableCacheBuilder.newBuilder()
                .maximumWeight(metadataCacheConfig.getFileTailCacheSize().toBytes())
                .weigher((Weigher<OrcDataSourceId, FileStatusAwareOrcFileTail>) (id, value) -> {
                    Optional<OrcFileTail> tail = value.orcFileTailOptional();
                    return id.toString().getBytes(StandardCharsets.UTF_8).length +
                            tail.map(OrcFileTail::getRetainedSize).orElse(0);
                })
                .expireAfterAccess(metadataCacheConfig.getFileTailCacheTtlSinceLastAccess().toMillis(), TimeUnit.MILLISECONDS)
                .recordStats();

        this.cache = cacheBuilder.build();
        this.cacheStats = new CacheStats();
    }

    @Override
    public Optional<OrcFileTail> getOrcFileTail(Optional<FileStatusInfo> fileStatusInfo, OrcDataSource orcDataSource, MetadataReader metadataReader, Optional<OrcWriteValidation> writeValidation)
            throws IOException
    {
        checkArgument(
                fileStatusInfo != null && fileStatusInfo.isPresent(),
                "Cannot use cached metadata without file status information");

        try {
            cacheStats.recordAccess();
            FileStatusAwareOrcFileTail fileStatusAwareOrcFileTail = cache.get(orcDataSource.getId(),
                    () -> loadOrcFileTail(orcDataSource, metadataReader, writeValidation));

            if (fileStatusAwareOrcFileTail.loadingTime() < fileStatusInfo.orElseThrow().modificationTime()) {
                cache.invalidate(orcDataSource.getId());
                cacheStats.recordInvalidation();
                fileStatusAwareOrcFileTail = cache.get(orcDataSource.getId(),
                        () -> loadOrcFileTail(orcDataSource, metadataReader, writeValidation));
            }

            return fileStatusAwareOrcFileTail.orcFileTailOptional();
        }
        catch (ExecutionException e) {
            throwIfInstanceOf(e.getCause(), IOException.class);
            throw new IOException(
                    format("Unexpected error in orc file tail reading after cache miss for: " + orcDataSource.getId()),
                    e.getCause());
        }
    }

    @Managed
    @Nested
    public CacheStats getCacheStats()
    {
        return cacheStats;
    }

    private FileStatusAwareOrcFileTail loadOrcFileTail(
            OrcDataSource orcDataSource,
            MetadataReader metadataReader,
            Optional<OrcWriteValidation> orcWriteValidation)
            throws IOException
    {
        cacheStats.recordMiss();
        long currentTimeMillis = System.currentTimeMillis();
        Optional<OrcFileTail> fileTail = OrcFileTailReader.getOrcFileTail(orcDataSource, metadataReader, orcWriteValidation);
        return new FileStatusAwareOrcFileTail(currentTimeMillis, fileTail);
    }

    private static class CacheStats
    {
        private final CounterStat accesses = new CounterStat();
        private final CounterStat misses = new CounterStat();
        private final CounterStat invalidations = new CounterStat();

        public CacheStats() {}

        @Managed
        @Nested
        public CounterStat getAccesses()
        {
            return accesses;
        }

        @Managed
        @Nested
        public CounterStat getMisses()
        {
            return misses;
        }

        @Managed
        @Nested
        public CounterStat getInvalidations()
        {
            return invalidations;
        }

        public void recordAccess()
        {
            accesses.update(1);
        }

        public void recordMiss()
        {
            misses.update(1);
        }

        public void recordInvalidation()
        {
            invalidations.update(1);
        }
    }
}
