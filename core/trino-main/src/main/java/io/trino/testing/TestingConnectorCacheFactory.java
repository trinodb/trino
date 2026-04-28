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
package io.trino.testing;

import io.trino.spi.cache.Blob;
import io.trino.spi.cache.BlobCache;
import io.trino.spi.cache.BlobSource;
import io.trino.spi.cache.CacheKey;
import io.trino.spi.cache.CacheTier;
import io.trino.spi.cache.ConnectorCacheFactory;
import io.trino.spi.cache.PassThroughBlob;

import java.util.Collection;

public class TestingConnectorCacheFactory
        implements ConnectorCacheFactory
{
    @Override
    public BlobCache createBlobCache(CacheTier tier)
    {
        return new TestingBlobCache();
    }

    private static class TestingBlobCache
            implements BlobCache
    {
        @Override
        public Blob get(CacheKey key, BlobSource source)
        {
            return new PassThroughBlob(source);
        }

        @Override
        public void invalidate(CacheKey key) {}

        @Override
        public void invalidate(Collection<CacheKey> keys) {}
    }
}
