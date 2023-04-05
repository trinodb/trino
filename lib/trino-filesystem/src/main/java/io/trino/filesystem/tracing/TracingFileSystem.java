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
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;

import static io.trino.filesystem.tracing.Tracing.withTracing;
import static java.util.Objects.requireNonNull;

final class TracingFileSystem
        implements TrinoFileSystem
{
    private final Tracer tracer;
    private final TrinoFileSystem delegate;

    public TracingFileSystem(Tracer tracer, TrinoFileSystem delegate)
    {
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public TrinoInputFile newInputFile(String location)
    {
        return new TracingInputFile(tracer, delegate.newInputFile(location), Optional.empty());
    }

    @Override
    public TrinoInputFile newInputFile(String location, long length)
    {
        return new TracingInputFile(tracer, delegate.newInputFile(location, length), Optional.of(length));
    }

    @Override
    public TrinoOutputFile newOutputFile(String location)
    {
        return new TracingOutputFile(tracer, delegate.newOutputFile(location));
    }

    @Override
    public void deleteFile(String location)
            throws IOException
    {
        Span span = tracer.spanBuilder("FileSystem.deleteFile")
                .setAttribute(FileSystemAttributes.FILE_LOCATION, location)
                .startSpan();
        withTracing(span, () -> delegate.deleteFile(location));
    }

    @Override
    public void deleteFiles(Collection<String> locations)
            throws IOException
    {
        Span span = tracer.spanBuilder("FileSystem.deleteFiles")
                .setAttribute(FileSystemAttributes.FILE_LOCATION_COUNT, (long) locations.size())
                .startSpan();
        withTracing(span, () -> delegate.deleteFiles(locations));
    }

    @Override
    public void deleteDirectory(String location)
            throws IOException
    {
        Span span = tracer.spanBuilder("FileSystem.deleteDirectory")
                .setAttribute(FileSystemAttributes.FILE_LOCATION, location)
                .startSpan();
        withTracing(span, () -> delegate.deleteDirectory(location));
    }

    @Override
    public void renameFile(String source, String target)
            throws IOException
    {
        Span span = tracer.spanBuilder("FileSystem.renameFile")
                .setAttribute(FileSystemAttributes.FILE_LOCATION, source)
                .startSpan();
        withTracing(span, () -> delegate.renameFile(source, target));
    }

    @Override
    public FileIterator listFiles(String location)
            throws IOException
    {
        Span span = tracer.spanBuilder("FileSystem.listFiles")
                .setAttribute(FileSystemAttributes.FILE_LOCATION, location)
                .startSpan();
        return withTracing(span, () -> delegate.listFiles(location));
    }
}
