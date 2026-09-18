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
package io.trino.spi.cache;

import java.io.IOException;

/**
 * A filesystem-agnostic blob cache. Callers identify cached bytes with a
 * {@link CacheKey} and supply a {@link BlobSource} used to populate the entry
 * on miss. The returned {@link Blob} reads from the cached entry on
 * subsequent calls.
 */
public interface BlobCache
{
    /**
     * Returns the blob cached under the given key, populating the entry from {@code source}
     * on miss. Implementations that cannot or choose not to cache the entry (for example
     * when it exceeds a size limit) return a blob reading through to the source instead.
     * The cache takes ownership of the source: it closes it once the entry is fully cached
     * and on every failure path of this method; only when the returned blob reads through
     * to the source does the source stay open, owned and closed by that blob.
     */
    Blob get(CacheKey key, BlobSource source)
            throws IOException;

    /**
     * Requests invalidation of every entry whose key {@link CacheKey#startsWith starts with}
     * the given prefix: a full key targets a single entry, a shorter key all entries under
     * it (for example all versions and variants of a file, or a whole catalog).
     * <p>
     * Invalidation is best effort and not guaranteed: implementations that cannot enumerate
     * entries by prefix may invalidate only some of the matching entries, or none at all.
     * Callers must not rely on it for correctness and should instead encode the content
     * identity (such as a file version) in the key, so that stale entries are never looked
     * up; invalidation only helps reclaim their space sooner.
     */
    void tryInvalidate(CacheKey prefix);
}
