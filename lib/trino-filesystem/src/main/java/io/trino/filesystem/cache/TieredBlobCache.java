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
package io.trino.filesystem.cache;

import io.trino.spi.cache.Blob;
import io.trino.spi.cache.BlobCache;
import io.trino.spi.cache.BlobSource;
import io.trino.spi.cache.CacheKey;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

/**
 * Two caches serving one file system: a lookup goes to {@code fast} and only reads through to
 * {@code slow} when the faster tier does not hold the entry, so content both tiers hold is
 * served without touching the slower one. Content the fast tier declines to cache, such as a
 * file exceeding its entry size limit, passes through it and is still served and cached by the
 * slow tier.
 * <p>
 * The tiers cache independently: an entry populates both on the way back, and either may evict
 * it on its own.
 */
public final class TieredBlobCache
        implements BlobCache
{
    private final BlobCache fast;
    private final BlobCache slow;

    public TieredBlobCache(BlobCache fast, BlobCache slow)
    {
        this.fast = requireNonNull(fast, "fast is null");
        this.slow = requireNonNull(slow, "slow is null");
    }

    @Override
    public Blob get(CacheKey key, BlobSource source)
            throws IOException
    {
        return fast.get(key, new BlobCacheSource(slow, key, source));
    }

    @Override
    public void tryInvalidate(CacheKey prefix)
    {
        fast.tryInvalidate(prefix);
        slow.tryInvalidate(prefix);
    }

    @Override
    public String toString()
    {
        return "%s over %s".formatted(fast, slow);
    }
}
