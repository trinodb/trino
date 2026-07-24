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
package io.trino.filesystem.cache;

import io.airlift.units.Duration;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.UriLocation;
import io.trino.filesystem.encryption.EncryptionKey;
import io.trino.spi.cache.BlobCache;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public final class CacheFileSystem
        implements TrinoFileSystem
{
    private final TrinoFileSystem delegate;
    private final BlobCache cache;
    private final CacheKeyProvider keyProvider;

    public CacheFileSystem(TrinoFileSystem delegate, BlobCache cache, CacheKeyProvider keyProvider)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.cache = requireNonNull(cache, "cache is null");
        this.keyProvider = requireNonNull(keyProvider, "keyProvider is null");
    }

    @Override
    public TrinoInputFile newInputFile(Location location)
    {
        return new CacheInputFile(delegate.newInputFile(location), cache, keyProvider, OptionalLong.empty(), Optional.empty());
    }

    @Override
    public TrinoInputFile newEncryptedInputFile(Location location, EncryptionKey key)
    {
        return new CacheInputFile(delegate.newEncryptedInputFile(location, key), cache, keyProvider, OptionalLong.empty(), Optional.empty());
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length)
    {
        return new CacheInputFile(delegate.newInputFile(location, length), cache, keyProvider, OptionalLong.of(length), Optional.empty());
    }

    @Override
    public TrinoInputFile newEncryptedInputFile(Location location, long length, EncryptionKey key)
    {
        return new CacheInputFile(delegate.newEncryptedInputFile(location, length, key), cache, keyProvider, OptionalLong.of(length), Optional.empty());
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length, Instant lastModified)
    {
        return new CacheInputFile(delegate.newInputFile(location, length, lastModified), cache, keyProvider, OptionalLong.of(length), Optional.of(lastModified));
    }

    @Override
    public TrinoInputFile newEncryptedInputFile(Location location, long length, Instant lastModified, EncryptionKey key)
    {
        return new CacheInputFile(delegate.newEncryptedInputFile(location, length, lastModified, key), cache, keyProvider, OptionalLong.of(length), Optional.of(lastModified));
    }

    @Override
    public TrinoOutputFile newOutputFile(Location location)
    {
        // Invalidation must follow the write, not precede it: a concurrent read during the
        // write could otherwise repopulate the entry with the previous content
        return new CacheOutputFile(delegate.newOutputFile(location), () -> invalidate(location));
    }

    @Override
    public TrinoOutputFile newEncryptedOutputFile(Location location, EncryptionKey key)
    {
        return new CacheOutputFile(delegate.newEncryptedOutputFile(location, key), () -> invalidate(location));
    }

    @Override
    public void deleteFile(Location location)
            throws IOException
    {
        try {
            delegate.deleteFile(location);
        }
        finally {
            // Invalidate even when the delete fails: it may have partially taken effect
            invalidate(location);
        }
    }

    @Override
    public void deleteDirectory(Location location)
            throws IOException
    {
        // Nothing is invalidated: cache keys identify a single file, so reclaiming the entries
        // under a directory would mean listing it while the files still exist, doubling the
        // listing traffic of every directory drop and failing the delete when the listing
        // fails. Keys are versioned, so entries left behind are never looked up again and are
        // reclaimed by TTL and size eviction, which is all the best-effort BlobCache
        // invalidation contract promises anyway
        delegate.deleteDirectory(location);
    }

    @Override
    public void renameFile(Location source, Location target)
            throws IOException
    {
        try {
            delegate.renameFile(source, target);
        }
        finally {
            invalidate(source);
            invalidate(target);
        }
    }

    @Override
    public FileIterator listFiles(Location location)
            throws IOException
    {
        return delegate.listFiles(location);
    }

    @Override
    public FileIterator listFilesStartingFrom(Location location, String startingFrom)
            throws IOException
    {
        return delegate.listFilesStartingFrom(location, startingFrom);
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
        // Entries under either directory are left to TTL and eviction, for the same reason as
        // in deleteDirectory
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
    public void deleteFiles(Collection<Location> locations)
            throws IOException
    {
        try {
            delegate.deleteFiles(locations);
        }
        finally {
            // Invalidate even when the delete fails: it may have partially taken effect
            locations.forEach(this::invalidate);
        }
    }

    private void invalidate(Location location)
    {
        keyProvider.getCacheKeyPrefix(location).ifPresent(cache::tryInvalidate);
    }

    @Override
    public Optional<UriLocation> encryptedPreSignedUri(Location location, Duration ttl, EncryptionKey key)
            throws IOException
    {
        return delegate.encryptedPreSignedUri(location, ttl, key);
    }
}
