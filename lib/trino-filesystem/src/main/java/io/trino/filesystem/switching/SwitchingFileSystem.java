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
package io.trino.filesystem.switching;

import io.airlift.units.Duration;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.UriLocation;
import io.trino.filesystem.encryption.EncryptionKey;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;

final class SwitchingFileSystem
        implements TrinoFileSystem
{
    private final Optional<ConnectorSession> session;
    private final Optional<ConnectorIdentity> identity;
    private final Function<Location, TrinoFileSystemFactory> loader;

    public SwitchingFileSystem(
            Optional<ConnectorSession> session,
            Optional<ConnectorIdentity> identity,
            Function<Location, TrinoFileSystemFactory> loader)
    {
        checkArgument(session.isPresent() != identity.isPresent(), "exactly one of session and identity must be present");
        this.session = session;
        this.identity = identity;
        this.loader = requireNonNull(loader, "loader is null");
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
    public TrinoInputFile newInputFile(Location location, long length, Instant lastModified)
    {
        return fileSystem(location).newInputFile(location, length, lastModified);
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
        var groups = locations.stream().collect(groupingBy(loader));
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

    @Override
    public Set<Location> listDirectories(Location location)
            throws IOException
    {
        return fileSystem(location).listDirectories(location);
    }

    @Override
    public Optional<Location> createTemporaryDirectory(Location targetPath, String temporaryPrefix, String relativePrefix)
            throws IOException
    {
        return fileSystem(targetPath).createTemporaryDirectory(targetPath, temporaryPrefix, relativePrefix);
    }

    @Override
    public Optional<UriLocation> preSignedUri(Location targetPath, Duration ttl)
            throws IOException
    {
        return fileSystem(targetPath).preSignedUri(targetPath, ttl);
    }

    @Override
    public TrinoInputFile newEncryptedInputFile(Location location, EncryptionKey key)
    {
        return fileSystem(location).newEncryptedInputFile(location, key);
    }

    @Override
    public TrinoInputFile newEncryptedInputFile(Location location, long length, EncryptionKey key)
    {
        return fileSystem(location).newEncryptedInputFile(location, length, key);
    }

    @Override
    public TrinoInputFile newEncryptedInputFile(Location location, long length, Instant lastModified, EncryptionKey key)
    {
        return fileSystem(location).newEncryptedInputFile(location, length, lastModified, key);
    }

    @Override
    public TrinoOutputFile newEncryptedOutputFile(Location location, EncryptionKey key)
    {
        return fileSystem(location).newEncryptedOutputFile(location, key);
    }

    @Override
    public Optional<UriLocation> encryptedPreSignedUri(Location location, Duration ttl, EncryptionKey key)
            throws IOException
    {
        return fileSystem(location).encryptedPreSignedUri(location, ttl, key);
    }

    private TrinoFileSystem fileSystem(Location location)
    {
        return createFileSystem(loader.apply(location));
    }

    private TrinoFileSystem createFileSystem(TrinoFileSystemFactory factory)
    {
        return session.map(factory::create).orElseGet(() ->
                factory.create(identity.orElseThrow()));
    }
}
