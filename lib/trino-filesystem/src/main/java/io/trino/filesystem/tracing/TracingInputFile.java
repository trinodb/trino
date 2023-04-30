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

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;

import static io.trino.filesystem.tracing.Tracing.attribute;
import static io.trino.filesystem.tracing.Tracing.withTracing;
import static java.util.Objects.requireNonNull;

final class TracingInputFile
        implements TrinoInputFile
{
    private final Tracer tracer;
    private final TrinoInputFile delegate;
    private final Optional<Long> length;

    public TracingInputFile(Tracer tracer, TrinoInputFile delegate, Optional<Long> length)
    {
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.length = requireNonNull(length, "length is null");
    }

    @Override
    public TrinoInput newInput()
            throws IOException
    {
        Span span = tracer.spanBuilder("InputFile.newInput")
                .setAttribute(FileSystemAttributes.FILE_LOCATION, toString())
                .setAllAttributes(attribute(FileSystemAttributes.FILE_SIZE, length))
                .startSpan();
        return withTracing(span, () -> new TracingInput(tracer, delegate.newInput(), location(), length));
    }

    @Override
    public TrinoInputStream newStream()
            throws IOException
    {
        Span span = tracer.spanBuilder("InputFile.newStream")
                .setAttribute(FileSystemAttributes.FILE_LOCATION, toString())
                .setAllAttributes(attribute(FileSystemAttributes.FILE_SIZE, length))
                .startSpan();
        return withTracing(span, delegate::newStream);
    }

    @Override
    public long length()
            throws IOException
    {
        // skip tracing if length is cached, but delegate anyway
        if (length.isPresent()) {
            return delegate.length();
        }

        Span span = tracer.spanBuilder("InputFile.length")
                .setAttribute(FileSystemAttributes.FILE_LOCATION, toString())
                .startSpan();
        return withTracing(span, delegate::length);
    }

    @Override
    public Instant lastModified()
            throws IOException
    {
        Span span = tracer.spanBuilder("InputFile.lastModified")
                .setAttribute(FileSystemAttributes.FILE_LOCATION, toString())
                .startSpan();
        return withTracing(span, delegate::lastModified);
    }

    @Override
    public boolean exists()
            throws IOException
    {
        Span span = tracer.spanBuilder("InputFile.exists")
                .setAttribute(FileSystemAttributes.FILE_LOCATION, toString())
                .startSpan();
        return withTracing(span, delegate::exists);
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
