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
package io.trino.plugin.hive.fs;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.fs.TransactionScopeCachingDirectoryLister.FetchingValueHolder;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class TransactionScopeCachingDirectoryListerFactory
{
    //TODO use a cache key based on Path & SchemaTableName and iterate over the cache keys
    // to deal more efficiently with cache invalidation scenarios for partitioned tables.
    private final Optional<Cache<TransactionDirectoryListingCacheKey, FetchingValueHolder>> cache;
    private final AtomicLong nextTransactionId = new AtomicLong();

    @Inject
    public TransactionScopeCachingDirectoryListerFactory(HiveConfig hiveConfig)
    {
        this(requireNonNull(hiveConfig, "hiveConfig is null").getPerTransactionFileStatusCacheMaxRetainedSize(), Optional.empty());
    }

    @VisibleForTesting
    TransactionScopeCachingDirectoryListerFactory(DataSize maxSize, Optional<Integer> concurrencyLevel)
    {
        if (maxSize.toBytes() > 0) {
            EvictableCacheBuilder<TransactionDirectoryListingCacheKey, FetchingValueHolder> cacheBuilder = EvictableCacheBuilder.newBuilder()
                    .maximumWeight(maxSize.toBytes())
                    .weigher((key, value) -> toIntExact(key.getRetainedSizeInBytes() + value.getRetainedSizeInBytes()));
            concurrencyLevel.ifPresent(cacheBuilder::concurrencyLevel);
            this.cache = Optional.of(cacheBuilder.build());
        }
        else {
            cache = Optional.empty();
        }
    }

    public DirectoryLister get(DirectoryLister delegate)
    {
        return cache
                .map(cache -> (DirectoryLister) new TransactionScopeCachingDirectoryLister(delegate, nextTransactionId.getAndIncrement(), cache))
                .orElse(delegate);
    }
}
