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
package io.trino.filesystem.tracing;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.trino.spi.cache.Blob;
import io.trino.spi.cache.BlobCache;
import io.trino.spi.cache.BlobSource;
import io.trino.spi.cache.CacheKey;

import java.io.IOException;
import java.util.Collection;

import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_LOCATION;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_KEY;
import static io.trino.filesystem.tracing.Tracing.withTracing;
import static java.util.Objects.requireNonNull;

public class TracingBlobCache
        implements BlobCache
{
    private final Tracer tracer;
    private final BlobCache delegate;

    public TracingBlobCache(Tracer tracer, BlobCache delegate)
    {
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public Blob get(CacheKey key, BlobSource source)
            throws IOException
    {
        Span span = tracer.spanBuilder("BlobCache.get")
                .setAttribute(CACHE_FILE_LOCATION, source.toString())
                .setAttribute(CACHE_KEY, key.key())
                .startSpan();

        return withTracing(span, () -> delegate.get(key, source));
    }

    @Override
    public void invalidate(CacheKey key)
    {
        Span span = tracer.spanBuilder("BlobCache.invalidate")
                .setAttribute(CACHE_KEY, key.key())
                .startSpan();

        withTracing(span, () -> delegate.invalidate(key));
    }

    @Override
    public void invalidate(Collection<CacheKey> keys)
    {
        Span span = tracer.spanBuilder("BlobCache.invalidateBatch").startSpan();

        withTracing(span, () -> delegate.invalidate(keys));
    }
}
