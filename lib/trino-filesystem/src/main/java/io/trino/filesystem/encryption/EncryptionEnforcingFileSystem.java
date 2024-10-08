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
package io.trino.filesystem.encryption;

import io.airlift.units.Duration;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.UriLocation;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * File system implementation that enforces encrypted file system calls.
 */
public class EncryptionEnforcingFileSystem
        implements TrinoFileSystem
{
    private final TrinoFileSystem delegate;
    private final EncryptionKey key;

    public EncryptionEnforcingFileSystem(TrinoFileSystem delegate, EncryptionKey key)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.key = requireNonNull(key, "key is null");
    }

    @Override
    public TrinoInputFile newInputFile(Location location)
    {
        return newEncryptedInputFile(location, key);
    }

    @Override
    public TrinoInputFile newEncryptedInputFile(Location location, EncryptionKey key)
    {
        checkArgument(this.key.equals(key), "Provided key is not the same as the class encryption key");
        return delegate.newEncryptedInputFile(location, key);
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length)
    {
        return delegate.newEncryptedInputFile(location, length, key);
    }

    @Override
    public TrinoInputFile newEncryptedInputFile(Location location, long length, EncryptionKey key)
    {
        checkArgument(this.key.equals(key), "Provided key is not the same as the class encryption key");
        return delegate.newEncryptedInputFile(location, length, key);
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length, Instant lastModified)
    {
        return delegate.newEncryptedInputFile(location, length, lastModified, key);
    }

    @Override
    public TrinoInputFile newEncryptedInputFile(Location location, long length, Instant lastModified, EncryptionKey key)
    {
        checkArgument(this.key.equals(key), "Provided key is not the same as the class encryption key");
        return delegate.newEncryptedInputFile(location, length, key);
    }

    @Override
    public TrinoOutputFile newOutputFile(Location location)
    {
        return delegate.newEncryptedOutputFile(location, key);
    }

    @Override
    public TrinoOutputFile newEncryptedOutputFile(Location location, EncryptionKey key)
    {
        checkArgument(this.key.equals(key), "Provided key is not the same as the class encryption key");
        return delegate.newEncryptedOutputFile(location, key);
    }

    @Override
    public void deleteFile(Location location)
            throws IOException
    {
        delegate.deleteFile(location);
    }

    @Override
    public void deleteFiles(Collection<Location> locations)
            throws IOException
    {
        delegate.deleteFiles(locations);
    }

    @Override
    public void deleteDirectory(Location location)
            throws IOException
    {
        delegate.deleteDirectory(location);
    }

    @Override
    public void renameFile(Location source, Location target)
            throws IOException
    {
        delegate.renameFile(source, target);
    }

    @Override
    public FileIterator listFiles(Location location)
            throws IOException
    {
        return delegate.listFiles(location);
    }

    @Override
    public Optional<Boolean> directoryExists(Location location)
            throws IOException
    {
        return delegate.directoryExists(location);
    }

    @Override
    public void createDirectory(Location location)
            throws IOException
    {
        delegate.createDirectory(location);
    }

    @Override
    public void renameDirectory(Location source, Location target)
            throws IOException
    {
        delegate.renameDirectory(source, target);
    }

    @Override
    public Set<Location> listDirectories(Location location)
            throws IOException
    {
        return delegate.listDirectories(location);
    }

    @Override
    public Optional<Location> createTemporaryDirectory(Location targetPath, String temporaryPrefix, String relativePrefix)
            throws IOException
    {
        return delegate.createTemporaryDirectory(targetPath, temporaryPrefix, relativePrefix);
    }

    @Override
    public Optional<UriLocation> preSignedUri(Location location, Duration ttl)
            throws IOException
    {
        return delegate.encryptedPreSignedUri(location, ttl, key);
    }

    @Override
    public Optional<UriLocation> encryptedPreSignedUri(Location location, Duration ttl, EncryptionKey key)
            throws IOException
    {
        checkArgument(this.key.equals(key), "Provided key is not the same as the class encryption key");
        return delegate.encryptedPreSignedUri(location, ttl, key);
    }

    public TrinoFileSystem getDelegate()
    {
        return delegate;
    }
}
