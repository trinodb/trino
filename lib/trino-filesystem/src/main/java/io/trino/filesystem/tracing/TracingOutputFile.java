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
import io.trino.filesystem.TrinoOutputStream;
import io.trino.memory.context.AggregatedMemoryContext;

import java.io.IOException;

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
    public TrinoOutputStream create()
            throws IOException
    {
        Span span = tracer.spanBuilder("OutputFile.create")
                .setAttribute(FileSystemAttributes.FILE_LOCATION, location().toString())
                .startSpan();
        return withTracing(span, () -> delegate.create());
    }

    @Override
    public void createOrOverwrite(byte[] data)
            throws IOException
    {
        Span span = tracer.spanBuilder("OutputFile.createOrOverwrite")
                .setAttribute(FileSystemAttributes.FILE_LOCATION, location().toString())
                .startSpan();
        withTracing(span, () -> delegate.createOrOverwrite(data));
    }

    @Override
    public void createExclusive(byte[] data)
            throws IOException
    {
        Span span = tracer.spanBuilder("OutputFile.createExclusive")
                .setAttribute(FileSystemAttributes.FILE_LOCATION, location().toString())
                .startSpan();
        withTracing(span, () -> delegate.createExclusive(data));
    }

    @Override
    public TrinoOutputStream create(AggregatedMemoryContext memoryContext)
            throws IOException
    {
        Span span = tracer.spanBuilder("OutputFile.create")
                .setAttribute(FileSystemAttributes.FILE_LOCATION, location().toString())
                .startSpan();
        return withTracing(span, () -> delegate.create(memoryContext));
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
