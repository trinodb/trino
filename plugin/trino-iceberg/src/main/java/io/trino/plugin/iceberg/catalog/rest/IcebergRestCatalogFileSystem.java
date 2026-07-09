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
package io.trino.plugin.iceberg.catalog.rest;

import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.UriLocation;
import io.trino.filesystem.encryption.EncryptionKey;
import io.trino.plugin.iceberg.IcebergStorageCredentials;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;

public class IcebergRestCatalogFileSystem
        implements TrinoFileSystem
{
    protected List<IcebergStorageCredentials> storageCredentials = ImmutableList.of();
    private final IcebergRestCatalogFileSystemLoader loader;

    public IcebergRestCatalogFileSystem(IcebergRestCatalogFileSystemLoader loader)
    {
        this.loader = requireNonNull(loader, "loader is null");
    }

    public void setCredentials(List<IcebergStorageCredentials> storageCredentials)
    {
        this.storageCredentials = ImmutableList.copyOf(storageCredentials);
        loader.setStorageCredentials(storageCredentials);
    }

    @Override
    public TrinoInputFile newInputFile(Location location)
    {
        return fileSystem(location).newInputFile(location);
    }

    @Override
    public TrinoInputFile newEncryptedInputFile(Location location, EncryptionKey key)
    {
        return fileSystem(location).newEncryptedInputFile(location, key);
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length)
    {
        return fileSystem(location).newInputFile(location, length);
    }

    @Override
    public TrinoInputFile newEncryptedInputFile(Location location, long length, EncryptionKey key)
    {
        return fileSystem(location).newEncryptedInputFile(location, length, key);
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length, Instant lastModified)
    {
        return fileSystem(location).newInputFile(location, length, lastModified);
    }

    @Override
    public TrinoInputFile newEncryptedInputFile(Location location, long length, Instant lastModified, EncryptionKey key)
    {
        return fileSystem(location).newEncryptedInputFile(location, length, lastModified, key);
    }

    @Override
    public TrinoOutputFile newOutputFile(Location location)
    {
        return fileSystem(location).newOutputFile(location);
    }

    @Override
    public TrinoOutputFile newEncryptedOutputFile(Location location, EncryptionKey key)
    {
        return fileSystem(location).newEncryptedOutputFile(location, key);
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
        // Group by the longest matching storage-credential prefix first so each group is served
        // by a single credential set. Locations that match no prefix fall back to grouping by
        // scheme, which is safe because all such locations share the root fileIoProperties.
        Map<String, List<Location>> groups = locations.stream()
                .collect(groupingBy(this::locationPrefix));
        for (List<Location> group : groups.values()) {
            fileSystem(group.getFirst()).deleteFiles(group);
        }
    }

    private String locationPrefix(Location location)
    {
        return storageCredentials.stream()
                .filter(credential -> location.toString().startsWith(credential.prefix()))
                .max(Comparator.comparingInt(credential -> credential.prefix().length()))
                .map(IcebergStorageCredentials::prefix)
                .orElseGet(() -> location.scheme().orElseThrow());
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
    public FileIterator listFilesStartingFrom(Location location, String startingFrom)
            throws IOException
    {
        return fileSystem(location).listFilesStartingFrom(location, startingFrom);
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
    public Optional<UriLocation> preSignedUri(Location location, Duration ttl)
            throws IOException
    {
        return fileSystem(location).preSignedUri(location, ttl);
    }

    @Override
    public Optional<UriLocation> encryptedPreSignedUri(Location location, Duration ttl, EncryptionKey key)
            throws IOException
    {
        return fileSystem(location).encryptedPreSignedUri(location, ttl, key);
    }

    private TrinoFileSystem fileSystem(Location location)
    {
        return loader.create(location);
    }
}
