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
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.filesystem.cache.TrinoFileSystemCache;

import java.io.IOException;

import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_LOCATION;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_KEY;
import static io.trino.filesystem.tracing.Tracing.withTracing;
import static java.util.Objects.requireNonNull;

public class TracingFileSystemCache
        implements TrinoFileSystemCache
{
    private final Tracer tracer;
    private final TrinoFileSystemCache delegate;

    public TracingFileSystemCache(Tracer tracer, TrinoFileSystemCache delegate)
    {
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public TrinoInput cacheInput(TrinoInputFile delegate, String key)
            throws IOException
    {
        Span span = tracer.spanBuilder("FileSystemCache.cacheInput")
                .setAttribute(CACHE_FILE_LOCATION, delegate.location().toString())
                .setAttribute(CACHE_KEY, key)
                .startSpan();

        return withTracing(span, () -> this.delegate.cacheInput(delegate, key));
    }

    @Override
    public TrinoInputStream cacheStream(TrinoInputFile delegate, String key)
            throws IOException
    {
        Span span = tracer.spanBuilder("FileSystemCache.cacheStream")
                .setAttribute(CACHE_FILE_LOCATION, delegate.location().toString())
                .setAttribute(CACHE_KEY, key)
                .startSpan();

        return withTracing(span, () -> this.delegate.cacheStream(delegate, key));
    }

    @Override
    public void expire(Location location)
            throws IOException
    {
        Span span = tracer.spanBuilder("FileSystemCache.expire")
                .setAttribute(CACHE_FILE_LOCATION, location.toString())
                .startSpan();

        withTracing(span, () -> delegate.expire(location));
    }
}
