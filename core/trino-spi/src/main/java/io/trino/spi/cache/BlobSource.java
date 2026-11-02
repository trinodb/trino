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

import java.io.Closeable;
import java.io.IOException;

/**
 * Lazy source of bytes backing a cached blob. Used by {@link BlobCache} to
 * populate a cache entry on miss.
 * <p>
 * A source represents immutable content: every read of the same source — and of any source
 * supplied for the same {@link CacheKey} — yields the same bytes, so reads are idempotent.
 * This is what makes best-effort {@link BlobCache#tryInvalidate invalidation} safe: an entry
 * can only ever serve the bytes its key was populated with, so an entry that outlives an
 * invalidation request wastes space but never serves invalid data.
 */
public interface BlobSource
        extends Closeable
{
    /**
     * Exact size of the content to cache.
     */
    long length()
            throws IOException;

    /**
     * Reads exactly {@code length} bytes at {@code position} of the underlying object into
     * {@code buffer} at {@code offset}, or fails without a partial read when the range is
     * not fully available.
     */
    void readFully(long position, byte[] buffer, int offset, int length)
            throws IOException;

    /**
     * Releases any resources held for reading. The blob cache closes the source once an entry
     * is fully cached; a blob reading through to the source closes it when the blob is closed.
     * <p>
     * Closing is terminal: the owner must not read the source afterwards, and implementations
     * are free to fail such a read.
     */
    @Override
    default void close()
            throws IOException
    {}
}
