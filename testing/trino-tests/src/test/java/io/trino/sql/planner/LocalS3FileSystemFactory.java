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
package io.trino.sql.planner;

import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.local.LocalFileSystem;
import io.trino.filesystem.local.LocalInputFile;
import io.trino.spi.security.ConnectorIdentity;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

/// Serves `s3://bucket/key` locations from `root/bucket/key` on the local file system,
/// so tables whose metadata references S3 locations can be read without an object store.
final class LocalS3FileSystemFactory
        implements TrinoFileSystemFactory
{
    private final TrinoFileSystem fileSystem;

    LocalS3FileSystemFactory(Path root)
    {
        this.fileSystem = new LocalS3FileSystem(new LocalFileSystem(root));
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity)
    {
        return fileSystem;
    }

    private record LocalS3FileSystem(LocalFileSystem delegate)
            implements TrinoFileSystem
    {
        private LocalS3FileSystem
        {
            requireNonNull(delegate, "delegate is null");
        }

        @Override
        public TrinoInputFile newInputFile(Location location)
        {
            return new LocalInputFile(location, delegate.toFilePath(toLocal(location)));
        }

        @Override
        public TrinoInputFile newInputFile(Location location, long length)
        {
            return new LocalInputFile(location, delegate.toFilePath(toLocal(location)), length, null);
        }

        @Override
        public TrinoInputFile newInputFile(Location location, long length, Instant lastModified)
        {
            return new LocalInputFile(location, delegate.toFilePath(toLocal(location)), length, lastModified);
        }

        @Override
        public TrinoOutputFile newOutputFile(Location location)
        {
            throw new UnsupportedOperationException("Read-only file system: " + location);
        }

        @Override
        public void deleteFile(Location location)
        {
            throw new UnsupportedOperationException("Read-only file system: " + location);
        }

        @Override
        public void deleteDirectory(Location location)
        {
            throw new UnsupportedOperationException("Read-only file system: " + location);
        }

        @Override
        public void renameFile(Location source, Location target)
        {
            throw new UnsupportedOperationException("Read-only file system: " + source);
        }

        @Override
        public FileIterator listFiles(Location location)
                throws IOException
        {
            FileIterator files = delegate.listFiles(toLocal(location));
            return new FileIterator()
            {
                @Override
                public boolean hasNext()
                        throws IOException
                {
                    return files.hasNext();
                }

                @Override
                public FileEntry next()
                        throws IOException
                {
                    FileEntry entry = files.next();
                    return new FileEntry(toS3(entry.location()), entry.length(), entry.lastModified(), entry.blocks(), entry.tags());
                }
            };
        }

        @Override
        public Optional<Boolean> directoryExists(Location location)
        {
            return delegate.directoryExists(toLocal(location));
        }

        @Override
        public void createDirectory(Location location)
        {
            throw new UnsupportedOperationException("Read-only file system: " + location);
        }

        @Override
        public void renameDirectory(Location source, Location target)
        {
            throw new UnsupportedOperationException("Read-only file system: " + source);
        }

        @Override
        public Set<Location> listDirectories(Location location)
                throws IOException
        {
            return delegate.listDirectories(toLocal(location)).stream()
                    .map(LocalS3FileSystem::toS3)
                    .collect(toImmutableSet());
        }

        @Override
        public Optional<Location> createTemporaryDirectory(Location targetPath, String temporaryPrefix, String relativePrefix)
        {
            throw new UnsupportedOperationException("Read-only file system: " + targetPath);
        }

        private static Location toLocal(Location location)
        {
            checkArgument(location.scheme().equals(Optional.of("s3")), "Only the 's3' scheme is supported: %s", location);
            return Location.of("local:///" + location.host().orElseThrow() + "/" + location.path());
        }

        private static Location toS3(Location location)
        {
            String path = location.path();
            int bucketEnd = path.indexOf('/');
            checkArgument(bucketEnd > 0, "Location outside a bucket: %s", location);
            return Location.of("s3://" + path.substring(0, bucketEnd) + "/" + path.substring(bucketEnd + 1));
        }
    }
}
