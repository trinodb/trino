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
package io.trino.spooling.filesystem;

import io.airlift.slice.Slice;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.trino.spi.spool.SpooledLocation;
import io.trino.spi.spool.SpooledLocation.DirectLocation;
import io.trino.spi.spool.SpooledSegmentHandle;
import io.trino.spi.spool.SpoolingContext;
import io.trino.spi.spool.SpoolingManager;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.opentelemetry.api.common.AttributeKey.longKey;
import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static io.trino.filesystem.tracing.Tracing.EXCEPTION_ESCAPED;
import static java.util.Objects.requireNonNull;

public class TracingSpoolingManager
        implements SpoolingManager
{
    public static final AttributeKey<String> SEGMENT_ID = stringKey("trino.segment.id");
    public static final AttributeKey<String> SEGMENT_QUERY_ID = stringKey("trino.segment.query_id");
    public static final AttributeKey<String> SEGMENT_ENCODING = stringKey("trino.segment.encoding");
    public static final AttributeKey<Long> SEGMENT_SIZE = longKey("trino.segment.size");
    public static final AttributeKey<Long> SEGMENT_ROWS = longKey("trino.segment.rows");
    public static final AttributeKey<String> SEGMENT_EXPIRATION = stringKey("trino.segment.expiration");

    private final Tracer tracer;
    private final SpoolingManager delegate;

    TracingSpoolingManager(Tracer tracer, SpoolingManager delegate)
    {
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public SpooledSegmentHandle create(SpoolingContext context)
    {
        Span span = tracer.spanBuilder("SpoolingManager.create")
                .setAttribute(SEGMENT_QUERY_ID, context.queryId().toString())
                .setAttribute(SEGMENT_ENCODING, context.encoding())
                .setAttribute(SEGMENT_ROWS, context.rows())
                .setAttribute(SEGMENT_SIZE, context.size())
                .startSpan();
        return withTracing(span, () -> delegate.create(context));
    }

    @Override
    public OutputStream createOutputStream(SpooledSegmentHandle handle)
            throws IOException
    {
        return withTracing(span(tracer, handle, "createOutputStream"), () -> delegate.createOutputStream(handle));
    }

    @Override
    public InputStream openInputStream(SpooledSegmentHandle handle)
            throws IOException
    {
        return withTracing(span(tracer, handle, "openInputStream"), () -> delegate.openInputStream(handle));
    }

    @Override
    public void acknowledge(SpooledSegmentHandle handle)
            throws IOException
    {
        withTracing(span(tracer, handle, "acknowledge"), () -> delegate.acknowledge(handle));
    }

    @Override
    public Optional<DirectLocation> directLocation(SpooledSegmentHandle handle)
            throws IOException
    {
        return withTracing(span(tracer, handle, "directLocation"), () -> delegate.directLocation(handle));
    }

    @Override
    public SpooledLocation location(SpooledSegmentHandle handle)
            throws IOException
    {
        return withTracing(span(tracer, handle, "location"), () -> delegate.location(handle));
    }

    @Override
    public SpooledSegmentHandle handle(Slice identifier, Map<String, List<String>> headers)
    {
        return delegate.handle(identifier, headers);
    }

    public static <E extends Exception> void withTracing(Span span, CheckedRunnable<E> runnable)
            throws E
    {
        withTracing(span, () -> {
            runnable.run();
            return null;
        });
    }

    public static Span span(Tracer tracer, SpooledSegmentHandle handle, String name)
    {
        return tracer
                .spanBuilder("SpoolingManager." + name)
                .setAttribute(SEGMENT_ID, handle.identifier())
                .setAttribute(SEGMENT_ENCODING, handle.encoding())
                .setAttribute(SEGMENT_EXPIRATION, handle.expirationTime().toString())
                .startSpan();
    }

    public static <T, E extends Exception> T withTracing(Span span, CheckedSupplier<T, E> supplier)
            throws E
    {
        try (var _ = span.makeCurrent()) {
            return supplier.get();
        }
        catch (Throwable t) {
            span.setStatus(StatusCode.ERROR, t.getMessage());
            span.recordException(t, Attributes.of(EXCEPTION_ESCAPED, true));
            throw t;
        }
        finally {
            span.end();
        }
    }

    public interface CheckedRunnable<E extends Exception>
    {
        void run()
                throws E;
    }

    public interface CheckedSupplier<T, E extends Exception>
    {
        T get()
                throws E;
    }
}
