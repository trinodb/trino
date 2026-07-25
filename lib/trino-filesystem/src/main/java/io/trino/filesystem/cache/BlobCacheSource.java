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
 * Content of a cache entry read through another {@link BlobCache}: populating a cache from this
 * source populates that one on the way, and a source that is never read leaves it untouched.
 */
final class BlobCacheSource
        implements BlobSource
{
    private final BlobCache cache;
    private final CacheKey key;
    private BlobSource source;
    private Blob blob;
    private boolean closed;

    BlobCacheSource(BlobCache cache, CacheKey key, BlobSource source)
    {
        this.cache = requireNonNull(cache, "cache is null");
        this.key = requireNonNull(key, "key is null");
        this.source = requireNonNull(source, "source is null");
    }

    @Override
    public long length()
            throws IOException
    {
        return blob().length();
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length)
            throws IOException
    {
        blob().read(position, buffer, offset, length);
    }

    // The blob is held open across reads for the same reason the underlying source is: reads
    // repeat, and a lookup per read would defeat the tier below. Whoever owns this source
    // closes it when done.
    private Blob blob()
            throws IOException
    {
        // Close is terminal, matching every other source: a read by an owner that already
        // released this source fails instead of reopening what nothing would close
        if (closed) {
            throw new IOException("Blob source is closed: " + this);
        }
        if (blob == null) {
            // The cache takes ownership of the source, including on the failure paths of the
            // lookup, so this source must not close it afterwards
            BlobSource lookupSource = source;
            source = null;
            blob = cache.get(key, lookupSource);
        }
        return blob;
    }

    @Override
    public void close()
            throws IOException
    {
        closed = true;
        // At most one of the two is held: ownership of the source passes to the cache when it
        // hands out the blob
        if (blob != null) {
            Blob toClose = blob;
            blob = null;
            toClose.close();
            return;
        }
        if (source != null) {
            BlobSource toClose = source;
            source = null;
            toClose.close();
        }
    }

    @Override
    public String toString()
    {
        return key.toString();
    }
}
