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
package io.trino.cache;

import io.trino.spi.cache.Blob;
import io.trino.spi.cache.BlobCache;
import io.trino.spi.cache.BlobSource;
import io.trino.spi.cache.CacheKey;
import io.trino.spi.catalog.CatalogName;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

/**
 * Scopes every {@link CacheKey} under a {@code [catalog, usage]} prefix: consumers sharing a
 * {@link io.trino.spi.cache.BlobCacheManager} never see each other's entries, a whole catalog
 * is invalidated with the {@code [catalog]} key prefix, and a single usage within it with
 * {@code [catalog, usage]}.
 */
final class ScopedBlobCache
        implements BlobCache
{
    private final BlobCache delegate;
    private final CacheKey scope;

    ScopedBlobCache(BlobCache delegate, CatalogName catalog, String usageName)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.scope = CacheKey.of(
                requireNonNull(catalog, "catalog is null").toString(),
                requireNonNull(usageName, "usageName is null"));
    }

    @Override
    public Blob get(CacheKey key, BlobSource source)
            throws IOException
    {
        return delegate.get(scope.append(key), source);
    }

    @Override
    public void tryInvalidate(CacheKey prefix)
    {
        delegate.tryInvalidate(scope.append(prefix));
    }
}
