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

import io.airlift.units.Duration;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.UriLocation;
import io.trino.filesystem.encryption.EncryptionKey;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;

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
    public TrinoInputFile newInputFile(Location location)
    {
        return new TracingInputFile(tracer, delegate.newInputFile(location), Optional.empty(), Optional.empty());
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length)
    {
        return new TracingInputFile(tracer, delegate.newInputFile(location, length), Optional.of(length), Optional.empty());
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length, Instant lastModified)
    {
        return new TracingInputFile(tracer, delegate.newInputFile(location, length, lastModified), Optional.of(length), Optional.of(lastModified));
    }

    @Override
    public TrinoOutputFile newOutputFile(Location location)
    {
        return new TracingOutputFile(tracer, delegate.newOutputFile(location));
    }

    @Override
    public void deleteFile(Location location)
            throws IOException
    {
        Span span = tracer.spanBuilder("FileSystem.deleteFile")
                .setAttribute(FileSystemAttributes.FILE_LOCATION, location.toString())
                .startSpan();
        withTracing(span, () -> delegate.deleteFile(location));
    }

    @Override
    public void deleteFiles(Collection<Location> locations)
            throws IOException
    {
        Span span = tracer.spanBuilder("FileSystem.deleteFiles")
                .setAttribute(FileSystemAttributes.FILE_LOCATION_COUNT, (long) locations.size())
                .startSpan();
        withTracing(span, () -> delegate.deleteFiles(locations));
    }

    @Override
    public void deleteDirectory(Location location)
            throws IOException
    {
        Span span = tracer.spanBuilder("FileSystem.deleteDirectory")
                .setAttribute(FileSystemAttributes.FILE_LOCATION, location.toString())
                .startSpan();
        withTracing(span, () -> delegate.deleteDirectory(location));
    }

    @Override
    public void renameFile(Location source, Location target)
            throws IOException
    {
        Span span = tracer.spanBuilder("FileSystem.renameFile")
                .setAttribute(FileSystemAttributes.FILE_LOCATION, source.toString())
                .startSpan();
        withTracing(span, () -> delegate.renameFile(source, target));
    }

    @Override
    public FileIterator listFiles(Location location)
            throws IOException
    {
        Span span = tracer.spanBuilder("FileSystem.listFiles")
                .setAttribute(FileSystemAttributes.FILE_LOCATION, location.toString())
                .startSpan();
        return withTracing(span, () -> delegate.listFiles(location));
    }

    @Override
    public Optional<Boolean> directoryExists(Location location)
            throws IOException
    {
        Span span = tracer.spanBuilder("FileSystem.directoryExists")
                .setAttribute(FileSystemAttributes.FILE_LOCATION, location.toString())
                .startSpan();
        return withTracing(span, () -> delegate.directoryExists(location));
    }

    @Override
    public void createDirectory(Location location)
            throws IOException
    {
        Span span = tracer.spanBuilder("FileSystem.createDirectory")
                .setAttribute(FileSystemAttributes.FILE_LOCATION, location.toString())
                .startSpan();
        withTracing(span, () -> delegate.createDirectory(location));
    }

    @Override
    public void renameDirectory(Location source, Location target)
            throws IOException
    {
        Span span = tracer.spanBuilder("FileSystem.renameDirectory")
                .setAttribute(FileSystemAttributes.FILE_LOCATION, source.toString())
                .startSpan();
        withTracing(span, () -> delegate.renameDirectory(source, target));
    }

    @Override
    public Set<Location> listDirectories(Location location)
            throws IOException
    {
        Span span = tracer.spanBuilder("FileSystem.listDirectories")
                .setAttribute(FileSystemAttributes.FILE_LOCATION, location.toString())
                .startSpan();
        return withTracing(span, () -> delegate.listDirectories(location));
    }

    @Override
    public Optional<Location> createTemporaryDirectory(Location targetPath, String temporaryPrefix, String relativePrefix)
            throws IOException
    {
        Span span = tracer.spanBuilder("FileSystem.createTemporaryDirectory")
                .setAttribute(FileSystemAttributes.FILE_LOCATION, targetPath.toString())
                .startSpan();
        return withTracing(span, () -> delegate.createTemporaryDirectory(targetPath, temporaryPrefix, relativePrefix));
    }

    @Override
    public Optional<UriLocation> preSignedUri(Location location, Duration ttl)
            throws IOException
    {
        Span span = tracer.spanBuilder("FileSystem.preSignedUri")
                .setAttribute(FileSystemAttributes.FILE_LOCATION, location.toString())
                .startSpan();
        return withTracing(span, () -> delegate.preSignedUri(location, ttl));
    }

    @Override
    public TrinoInputFile newEncryptedInputFile(Location location, EncryptionKey key)
    {
        Span span = tracer.spanBuilder("FileSystem.newEncryptedInputFile")
                .setAttribute(FileSystemAttributes.FILE_LOCATION, location.toString())
                .startSpan();
        return withTracing(span, () -> delegate.newEncryptedInputFile(location, key));
    }

    @Override
    public TrinoInputFile newEncryptedInputFile(Location location, long length, EncryptionKey key)
    {
        Span span = tracer.spanBuilder("FileSystem.newEncryptedInputFile")
                .setAttribute(FileSystemAttributes.FILE_LOCATION, location.toString())
                .startSpan();
        return withTracing(span, () -> delegate.newEncryptedInputFile(location, length, key));
    }

    @Override
    public TrinoInputFile newEncryptedInputFile(Location location, long length, Instant lastModified, EncryptionKey key)
    {
        Span span = tracer.spanBuilder("FileSystem.newEncryptedInputFile")
                .setAttribute(FileSystemAttributes.FILE_LOCATION, location.toString())
                .startSpan();
        return withTracing(span, () -> delegate.newEncryptedInputFile(location, length, lastModified, key));
    }

    @Override
    public TrinoOutputFile newEncryptedOutputFile(Location location, EncryptionKey key)
    {
        Span span = tracer.spanBuilder("FileSystem.newEncryptedOutputFile")
                .setAttribute(FileSystemAttributes.FILE_LOCATION, location.toString())
                .startSpan();
        return withTracing(span, () -> delegate.newEncryptedOutputFile(location, key));
    }

    @Override
    public Optional<UriLocation> encryptedPreSignedUri(Location location, Duration ttl, EncryptionKey key)
            throws IOException
    {
        Span span = tracer.spanBuilder("FileSystem.encryptedPreSignedUri")
                .setAttribute(FileSystemAttributes.FILE_LOCATION, location.toString())
                .startSpan();
        return withTracing(span, () -> delegate.encryptedPreSignedUri(location, ttl, key));
    }
}
