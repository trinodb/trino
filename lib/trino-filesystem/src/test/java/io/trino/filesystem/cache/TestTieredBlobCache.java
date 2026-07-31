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
import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.Math.toIntExact;
import static java.util.Objects.checkFromIndexSize;
import static org.assertj.core.api.Assertions.assertThat;

final class TestTieredBlobCache
{
    private static final CacheKey KEY = CacheKey.of("catalog", "memory:///file", "v1");
    private static final byte[] CONTENT = "the quick brown fox".getBytes(StandardCharsets.UTF_8);

    @Test
    void testHitInFastTierDoesNotReachSlowTier()
            throws IOException
    {
        TestingBlobCache fast = new TestingBlobCache(true);
        TestingBlobCache slow = new TestingBlobCache(true);
        TieredBlobCache cache = new TieredBlobCache(fast, slow);

        TestingBlobSource origin = new TestingBlobSource();
        assertThat(read(cache, origin)).isEqualTo(CONTENT);
        // The miss populates both tiers, reading the origin once
        assertThat(slow.loads).isEqualTo(1);
        assertThat(origin.reads).isEqualTo(1);
        assertThat(origin.closed).isTrue();

        TestingBlobSource unusedOrigin = new TestingBlobSource();
        assertThat(read(cache, unusedOrigin)).isEqualTo(CONTENT);
        assertThat(fast.lookups).isEqualTo(2);
        assertThat(slow.lookups).isEqualTo(1);
        assertThat(unusedOrigin.reads).isEqualTo(0);
        assertThat(unusedOrigin.closed).isTrue();
    }

    @Test
    void testSlowTierServesWhatFastTierDeclinesToCache()
            throws IOException
    {
        // A fast tier that never caches, standing in for content over its entry size limit
        TestingBlobCache fast = new TestingBlobCache(false);
        TestingBlobCache slow = new TestingBlobCache(true);
        TieredBlobCache cache = new TieredBlobCache(fast, slow);

        TestingBlobSource origin = new TestingBlobSource();
        assertThat(read(cache, origin)).isEqualTo(CONTENT);
        assertThat(origin.reads).isEqualTo(1);
        assertThat(origin.closed).isTrue();

        TestingBlobSource unusedOrigin = new TestingBlobSource();
        assertThat(read(cache, unusedOrigin)).isEqualTo(CONTENT);
        // Every lookup reaches the slow tier, but the origin is only read on the first one
        assertThat(slow.lookups).isEqualTo(2);
        assertThat(slow.loads).isEqualTo(1);
        assertThat(unusedOrigin.reads).isEqualTo(0);
        assertThat(unusedOrigin.closed).isTrue();
    }

    @Test
    void testInvalidationReachesBothTiers()
            throws IOException
    {
        TestingBlobCache fast = new TestingBlobCache(true);
        TestingBlobCache slow = new TestingBlobCache(true);
        TieredBlobCache cache = new TieredBlobCache(fast, slow);

        read(cache, new TestingBlobSource());
        CacheKey prefix = CacheKey.of("catalog", "memory:///file");
        cache.tryInvalidate(prefix);

        assertThat(fast.invalidations).containsExactly(prefix);
        assertThat(slow.invalidations).containsExactly(prefix);

        TestingBlobSource origin = new TestingBlobSource();
        assertThat(read(cache, origin)).isEqualTo(CONTENT);
        assertThat(origin.reads).isEqualTo(1);
    }

    private static byte[] read(BlobCache cache, BlobSource source)
            throws IOException
    {
        try (Blob blob = cache.get(KEY, source)) {
            byte[] buffer = new byte[toIntExact(blob.length())];
            blob.read(0, buffer, 0, buffer.length);
            return buffer;
        }
    }

    /**
     * Caches whole entries in a map, or, when not caching, hands out a blob reading through to
     * the source, the way a cache declining an oversized entry does.
     */
    private static final class TestingBlobCache
            implements BlobCache
    {
        private final Map<CacheKey, byte[]> entries = new HashMap<>();
        private final List<CacheKey> invalidations = new ArrayList<>();
        private final boolean caching;
        private int lookups;
        private int loads;

        TestingBlobCache(boolean caching)
        {
            this.caching = caching;
        }

        @Override
        public Blob get(CacheKey key, BlobSource source)
                throws IOException
        {
            lookups++;
            byte[] cached = entries.get(key);
            if (cached != null) {
                source.close();
                return new TestingBlob(cached);
            }
            if (!caching) {
                return new PassThroughBlob(source);
            }
            loads++;
            byte[] content = new byte[toIntExact(source.length())];
            source.readFully(0, content, 0, content.length);
            source.close();
            entries.put(key, content);
            return new TestingBlob(content);
        }

        @Override
        public void tryInvalidate(CacheKey prefix)
        {
            invalidations.add(prefix);
            entries.keySet().removeIf(key -> key.startsWith(prefix));
        }
    }

    private record TestingBlob(byte[] content)
            implements Blob
    {
        @Override
        public long length()
        {
            return content.length;
        }

        @Override
        public void read(long position, byte[] buffer, int offset, int length)
        {
            System.arraycopy(content, toIntExact(position), buffer, offset, length);
        }

        @Override
        public long cachedSize()
        {
            return content.length;
        }

        @Override
        public long loadedSize()
        {
            return 0;
        }

        @Override
        public void close() {}
    }

    private record PassThroughBlob(BlobSource source)
            implements Blob
    {
        @Override
        public long length()
                throws IOException
        {
            return source.length();
        }

        @Override
        public void read(long position, byte[] buffer, int offset, int length)
                throws IOException
        {
            source.readFully(position, buffer, offset, length);
        }

        @Override
        public long cachedSize()
        {
            return 0;
        }

        @Override
        public long loadedSize()
        {
            return 0;
        }

        @Override
        public void close()
                throws IOException
        {
            source.close();
        }
    }

    private static final class TestingBlobSource
            implements BlobSource
    {
        private int reads;
        private boolean closed;

        @Override
        public long length()
        {
            return CONTENT.length;
        }

        @Override
        public void readFully(long position, byte[] buffer, int offset, int length)
                throws IOException
        {
            checkFromIndexSize(offset, length, buffer.length);
            if (position < 0 || position + length > CONTENT.length) {
                throw new EOFException("Read of %s bytes at %s is outside the content".formatted(length, position));
            }
            reads++;
            System.arraycopy(CONTENT, toIntExact(position), buffer, offset, length);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }
}
