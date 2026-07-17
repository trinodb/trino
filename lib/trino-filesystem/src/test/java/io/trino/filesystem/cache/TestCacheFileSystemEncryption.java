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

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.UriLocation;
import io.trino.filesystem.encryption.EncryptionKey;
import io.trino.filesystem.memory.MemoryFileSystemCache;
import io.trino.filesystem.memory.MemoryFileSystemCacheConfig;
import io.trino.memory.context.AggregatedMemoryContext;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.URI;
import java.time.Instant;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that {@link CacheFileSystem} forwards the encrypted-file methods to
 * the underlying filesystem rather than throwing
 * {@link UnsupportedOperationException} from the default {@link TrinoFileSystem}
 * interface implementations.
 */
final class TestCacheFileSystemEncryption
{
    private static final Location LOCATION = Location.of("memory:///file");
    private static final EncryptionKey KEY = EncryptionKey.randomAes256();

    @Test
    void testNewEncryptedInputFileDelegates()
    {
        RecordingFileSystem delegate = new RecordingFileSystem();
        CacheFileSystem cacheFileSystem = newCacheFileSystem(delegate);

        assertThat(cacheFileSystem.newEncryptedInputFile(LOCATION, KEY)).isNotNull();
        assertThat(cacheFileSystem.newEncryptedInputFile(LOCATION, 42L, KEY)).isNotNull();
        assertThat(cacheFileSystem.newEncryptedInputFile(LOCATION, 42L, Instant.EPOCH, KEY)).isNotNull();
        assertThat(delegate.encryptedInputFileCalls).isEqualTo(3);
    }

    @Test
    void testNewEncryptedOutputFileDelegates()
    {
        RecordingFileSystem delegate = new RecordingFileSystem();
        CacheFileSystem cacheFileSystem = newCacheFileSystem(delegate);

        assertThat(cacheFileSystem.newEncryptedOutputFile(LOCATION, KEY)).isNotNull();
        assertThat(delegate.encryptedOutputFileCalls).isEqualTo(1);
    }

    @Test
    void testEncryptedPreSignedUriDelegates()
            throws Exception
    {
        RecordingFileSystem delegate = new RecordingFileSystem();
        CacheFileSystem cacheFileSystem = newCacheFileSystem(delegate);

        Optional<UriLocation> uri = cacheFileSystem.encryptedPreSignedUri(LOCATION, new Duration(1, TimeUnit.MINUTES), KEY);
        assertThat(uri).isPresent();
        assertThat(delegate.encryptedPreSignedUriCalls).isEqualTo(1);
    }

    private static CacheFileSystem newCacheFileSystem(TrinoFileSystem delegate)
    {
        return new CacheFileSystem(
                delegate,
                new MemoryFileSystemCache(new MemoryFileSystemCacheConfig()),
                new DefaultCacheKeyProvider());
    }

    private static final class RecordingFileSystem
            implements TrinoFileSystem
    {
        int encryptedInputFileCalls;
        int encryptedOutputFileCalls;
        int encryptedPreSignedUriCalls;

        @Override
        public TrinoInputFile newInputFile(Location location)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public TrinoInputFile newInputFile(Location location, long length)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public TrinoInputFile newInputFile(Location location, long length, Instant lastModified)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public TrinoInputFile newEncryptedInputFile(Location location, EncryptionKey key)
        {
            encryptedInputFileCalls++;
            return new StubInputFile(location);
        }

        @Override
        public TrinoInputFile newEncryptedInputFile(Location location, long length, EncryptionKey key)
        {
            encryptedInputFileCalls++;
            return new StubInputFile(location);
        }

        @Override
        public TrinoInputFile newEncryptedInputFile(Location location, long length, Instant lastModified, EncryptionKey key)
        {
            encryptedInputFileCalls++;
            return new StubInputFile(location);
        }

        @Override
        public TrinoOutputFile newOutputFile(Location location)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public TrinoOutputFile newEncryptedOutputFile(Location location, EncryptionKey key)
        {
            encryptedOutputFileCalls++;
            return new StubOutputFile(location);
        }

        @Override
        public Optional<UriLocation> encryptedPreSignedUri(Location location, Duration ttl, EncryptionKey key)
        {
            encryptedPreSignedUriCalls++;
            return Optional.of(new UriLocation(URI.create("https://example.com/signed"), ImmutableMap.of()));
        }

        @Override
        public void deleteFile(Location location) {}

        @Override
        public void deleteFiles(Collection<Location> locations) {}

        @Override
        public void deleteDirectory(Location location) {}

        @Override
        public void renameFile(Location source, Location target) {}

        @Override
        public FileIterator listFiles(Location location)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<Boolean> directoryExists(Location location)
        {
            return Optional.empty();
        }

        @Override
        public void createDirectory(Location location) {}

        @Override
        public void renameDirectory(Location source, Location target) {}

        @Override
        public Set<Location> listDirectories(Location location)
        {
            return Set.of();
        }

        @Override
        public Optional<Location> createTemporaryDirectory(Location targetPath, String temporaryPrefix, String relativePrefix)
        {
            return Optional.empty();
        }
    }

    private static final class StubInputFile
            implements TrinoInputFile
    {
        private final Location location;

        StubInputFile(Location location)
        {
            this.location = location;
        }

        @Override
        public TrinoInput newInput()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public TrinoInputStream newStream()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long length()
        {
            return 0;
        }

        @Override
        public Instant lastModified()
        {
            return Instant.EPOCH;
        }

        @Override
        public boolean exists()
        {
            return true;
        }

        @Override
        public Location location()
        {
            return location;
        }
    }

    private static final class StubOutputFile
            implements TrinoOutputFile
    {
        private final Location location;

        StubOutputFile(Location location)
        {
            this.location = location;
        }

        @Override
        public void createOrOverwrite(byte[] data) {}

        @Override
        public OutputStream create(AggregatedMemoryContext memoryContext)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Location location()
        {
            return location;
        }
    }
}
