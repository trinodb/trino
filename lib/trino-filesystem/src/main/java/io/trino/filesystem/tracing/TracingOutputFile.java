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
import io.trino.filesystem.TrinoOutputFile;
import io.trino.memory.context.AggregatedMemoryContext;

import java.io.IOException;
import java.io.OutputStream;

import static io.trino.filesystem.tracing.Tracing.withTracing;
import static java.util.Objects.requireNonNull;

final class TracingOutputFile
        implements TrinoOutputFile
{
    private final Tracer tracer;
    private final TrinoOutputFile delegate;

    public TracingOutputFile(Tracer tracer, TrinoOutputFile delegate)
    {
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.delegate = requireNonNull(delegate, "delete is null");
    }

    @Override
    public OutputStream create()
            throws IOException
    {
        Span span = tracer.spanBuilder("OutputFile.create")
                .setAttribute(FileSystemAttributes.FILE_LOCATION, toString())
                .startSpan();
        return withTracing(span, () -> delegate.create());
    }

    @Override
    public OutputStream createOrOverwrite()
            throws IOException
    {
        Span span = tracer.spanBuilder("OutputFile.createOrOverwrite")
                .setAttribute(FileSystemAttributes.FILE_LOCATION, toString())
                .startSpan();
        return withTracing(span, () -> delegate.createOrOverwrite());
    }

    @Override
    public OutputStream create(AggregatedMemoryContext memoryContext)
            throws IOException
    {
        Span span = tracer.spanBuilder("OutputFile.create")
                .setAttribute(FileSystemAttributes.FILE_LOCATION, toString())
                .startSpan();
        return withTracing(span, () -> delegate.create(memoryContext));
    }

    @Override
    public OutputStream createOrOverwrite(AggregatedMemoryContext memoryContext)
            throws IOException
    {
        Span span = tracer.spanBuilder("OutputFile.createOrOverwrite")
                .setAttribute(FileSystemAttributes.FILE_LOCATION, toString())
                .startSpan();
        return withTracing(span, () -> delegate.createOrOverwrite(memoryContext));
    }

    @Override
    public Location location()
    {
        return delegate.location();
    }

    @Override
    public String toString()
    {
        return location().toString();
    }
}
