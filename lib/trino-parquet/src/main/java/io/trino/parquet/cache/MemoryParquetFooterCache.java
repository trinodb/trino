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
package io.trino.parquet.cache;

import com.google.common.cache.Cache;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.cache.EvictableCacheBuilder;

import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class MemoryParquetFooterCache
        implements ParquetFooterCache
{
    private final Cache<ParquetFooterCacheKey, Slice> cache;
    private final long maxSizeInBytes;

    public MemoryParquetFooterCache(DataSize maxSize)
    {
        requireNonNull(maxSize, "maxSize is null");
        this.maxSizeInBytes = maxSize.toBytes();
        this.cache = EvictableCacheBuilder.newBuilder()
                .maximumWeight(maxSizeInBytes)
                .weigher((ParquetFooterCacheKey _, Slice footerBytes) -> toIntExact(footerBytes.length()))
                .build();
    }

    @Override
    public Optional<Slice> get(ParquetFooterCacheKey key)
    {
        return Optional.ofNullable(cache.getIfPresent(key));
    }

    @Override
    public void put(ParquetFooterCacheKey key, Slice footerBytes)
    {
        if (footerBytes.length() > maxSizeInBytes) {
            return;
        }
        try {
            // Do not replace an existing value, and only copy footer bytes when the cache is missing this key.
            cache.get(key, footerBytes::copy);
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void invalidate(ParquetFooterCacheKey key)
    {
        cache.invalidate(key);
    }
}
