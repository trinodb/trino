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

import io.airlift.log.Logger;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
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
    private static final Logger log = Logger.get(CacheFileSystem.class);

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
    public TrinoInputFile newInputFile(Location location, long length)
    {
        return new CacheInputFile(delegate.newInputFile(location, length), cache, keyProvider, OptionalLong.of(length), Optional.empty());
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length, Instant lastModified)
    {
        return new CacheInputFile(delegate.newInputFile(location, length, lastModified), cache, keyProvider, OptionalLong.of(length), Optional.of(lastModified));
    }

    @Override
    public TrinoOutputFile newOutputFile(Location location)
    {
        invalidate(location);
        return delegate.newOutputFile(location);
    }

    @Override
    public void deleteFile(Location location)
            throws IOException
    {
        invalidate(location);
        delegate.deleteFile(location);
    }

    @Override
    public void deleteDirectory(Location location)
            throws IOException
    {
        invalidateDirectoryEntries(location);
        delegate.deleteDirectory(location);
    }

    @Override
    public void renameFile(Location source, Location target)
            throws IOException
    {
        invalidate(source);
        invalidate(target);
        delegate.renameFile(source, target);
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
        invalidateDirectoryEntries(source);
        invalidateDirectoryEntries(target);
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
        locations.forEach(this::invalidate);
        delegate.deleteFiles(locations);
    }

    private void invalidate(Location location)
    {
        try {
            keyProvider.getCacheKey(delegate.newInputFile(location))
                    .ifPresent(cache::invalidate);
        }
        catch (IOException e) {
            log.warn(e, "Failed to invalidate cache entry for %s", location);
        }
    }

    private void invalidateDirectoryEntries(Location location)
    {
        try {
            FileIterator iterator = delegate.listFiles(location);
            while (iterator.hasNext()) {
                invalidate(iterator.next().location());
            }
        }
        catch (IOException e) {
            log.warn(e, "Failed to invalidate cache entries under %s", location);
        }
    }
}
