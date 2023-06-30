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

import io.airlift.slice.Slice;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.Tracer;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;

import java.io.IOException;
import java.util.Optional;

import static io.trino.filesystem.tracing.Tracing.attribute;
import static io.trino.filesystem.tracing.Tracing.withTracing;
import static java.util.Objects.requireNonNull;

final class TracingInput
        implements TrinoInput
{
    private final Tracer tracer;
    private final TrinoInput delegate;
    private final Location location;
    private final Optional<Long> fileLength;

    public TracingInput(Tracer tracer, TrinoInput delegate, Location location, Optional<Long> fileLength)
    {
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.location = requireNonNull(location, "location is null");
        this.fileLength = requireNonNull(fileLength, "fileLength is null");
    }

    @Override
    public void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        Span span = spanBuilder("Input.readFully", bufferLength)
                .setAttribute(FileSystemAttributes.FILE_READ_POSITION, position)
                .startSpan();
        withTracing(span, () -> delegate.readFully(position, buffer, bufferOffset, bufferLength));
    }

    @Override
    public int readTail(byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        Span span = spanBuilder("Input.readTail", bufferLength)
                .startSpan();
        return withTracing(span, () -> delegate.readTail(buffer, bufferOffset, bufferLength));
    }

    @Override
    public Slice readFully(long position, int length)
            throws IOException
    {
        Span span = spanBuilder("Input.readFully", length)
                .setAttribute(FileSystemAttributes.FILE_READ_POSITION, position)
                .startSpan();
        return withTracing(span, () -> delegate.readFully(position, length));
    }

    @Override
    public Slice readTail(int length)
            throws IOException
    {
        Span span = spanBuilder("Input.readTail", length)
                .startSpan();
        return withTracing(span, () -> delegate.readTail(length));
    }

    @Override
    public void close()
            throws IOException
    {
        delegate.close();
    }

    @Override
    public String toString()
    {
        return location.toString();
    }

    private SpanBuilder spanBuilder(String name, long readLength)
    {
        return tracer.spanBuilder(name)
                .setAttribute(FileSystemAttributes.FILE_LOCATION, toString())
                .setAllAttributes(attribute(FileSystemAttributes.FILE_SIZE, fileLength))
                .setAttribute(FileSystemAttributes.FILE_READ_SIZE, readLength);
    }
}
