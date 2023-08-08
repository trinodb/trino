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
package io.trino.filesystem.manager;

import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;

final class SwitchingFileSystem
        implements TrinoFileSystem
{
    private final Optional<ConnectorSession> session;
    private final Optional<ConnectorIdentity> identity;
    private final Optional<TrinoFileSystemFactory> hdfsFactory;
    private final Map<String, TrinoFileSystemFactory> factories;

    public SwitchingFileSystem(
            Optional<ConnectorSession> session,
            Optional<ConnectorIdentity> identity,
            Optional<TrinoFileSystemFactory> hdfsFactory,
            Map<String, TrinoFileSystemFactory> factories)
    {
        checkArgument(session.isPresent() != identity.isPresent(), "exactly one of session and identity must be present");
        this.session = session;
        this.identity = identity;
        this.hdfsFactory = requireNonNull(hdfsFactory, "hdfsFactory is null");
        this.factories = ImmutableMap.copyOf(requireNonNull(factories, "factories is null"));
    }

    @Override
    public TrinoInputFile newInputFile(Location location)
    {
        return fileSystem(location).newInputFile(location);
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length)
    {
        return fileSystem(location).newInputFile(location, length);
    }

    @Override
    public TrinoOutputFile newOutputFile(Location location)
    {
        return fileSystem(location).newOutputFile(location);
    }

    @Override
    public void deleteFile(Location location)
            throws IOException
    {
        fileSystem(location).deleteFile(location);
    }

    @Override
    public void deleteFiles(Collection<Location> locations)
            throws IOException
    {
        var groups = locations.stream().collect(groupingBy(this::determineFactory));
        for (var entry : groups.entrySet()) {
            createFileSystem(entry.getKey()).deleteFiles(entry.getValue());
        }
    }

    @Override
    public void deleteDirectory(Location location)
            throws IOException
    {
        fileSystem(location).deleteDirectory(location);
    }

    @Override
    public void renameFile(Location source, Location target)
            throws IOException
    {
        fileSystem(source).renameFile(source, target);
    }

    @Override
    public FileIterator listFiles(Location location)
            throws IOException
    {
        return fileSystem(location).listFiles(location);
    }

    @Override
    public Optional<Boolean> directoryExists(Location location)
            throws IOException
    {
        return fileSystem(location).directoryExists(location);
    }

    @Override
    public void createDirectory(Location location)
            throws IOException
    {
        fileSystem(location).createDirectory(location);
    }

    @Override
    public void renameDirectory(Location source, Location target)
            throws IOException
    {
        fileSystem(source).renameDirectory(source, target);
    }

    private TrinoFileSystem fileSystem(Location location)
    {
        return createFileSystem(determineFactory(location));
    }

    private TrinoFileSystemFactory determineFactory(Location location)
    {
        return location.scheme()
                .map(factories::get)
                .or(() -> hdfsFactory)
                .orElseThrow(() -> new IllegalArgumentException("No factory for location: " + location));
    }

    private TrinoFileSystem createFileSystem(TrinoFileSystemFactory factory)
    {
        return session.map(factory::create).orElseGet(() ->
                factory.create(identity.orElseThrow()));
    }
}
