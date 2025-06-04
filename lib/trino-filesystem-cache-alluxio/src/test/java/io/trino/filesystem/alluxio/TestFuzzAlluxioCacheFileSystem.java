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
package io.trino.filesystem.alluxio;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.tracing.Tracing;
import io.airlift.units.DataSize;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.cache.CacheFileSystem;
import io.trino.filesystem.memory.MemoryFileSystemFactory;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(Lifecycle.PER_METHOD)
public class TestFuzzAlluxioCacheFileSystem
{
    private static final DataSize CACHE_SIZE = DataSize.ofBytes(8 * 1024);
    private static final DataSize PAGE_SIZE = DataSize.ofBytes(128);

    @Test
    public void testFuzzTrinoInputReadFully()
            throws IOException
    {
        fuzzTrinoInputOperation((fs, l) -> fs.newInputFile(l).newInput(), (trinoInput, position, buffer, bufferOffset, bufferLength) -> {
            trinoInput.readFully(position, buffer, bufferOffset, bufferLength);
            return bufferLength - bufferOffset;
        });
    }

    @Test
    public void testFuzzTrinoInputReadTail()
            throws IOException
    {
        fuzzTrinoInputOperation((fs, l) -> fs.newInputFile(l).newInput(), (input, position, buffer, bufferOffset, bufferLength) -> input.readTail(buffer, bufferOffset, bufferLength));
    }

    @Test
    public void testFuzzTrinoInputStreamRead()
            throws IOException
    {
        fuzzTrinoInputOperation((fs, l) -> fs.newInputFile(l).newStream(), (input, position, buffer, bufferOffset, bufferLength) -> {
            input.seek(position);
            return input.read(buffer, bufferOffset, bufferLength);
        });
    }

    @Test
    public void testFuzzTrinoInputStreamReadSkip()
            throws IOException
    {
        fuzzTrinoInputOperation((fs, l) -> fs.newInputFile(l).newStream(), (input, position, buffer, bufferOffset, bufferLength) -> {
            input.skip(position);
            return input.read(buffer, bufferOffset, bufferLength);
        });
    }

    private <T> void fuzzTrinoInputOperation(CreateTrinoInput<T> createInput, TrinoInputOperation<T> operation)
            throws IOException
    {
        Random random = new Random();
        try (TestFileSystem expectedFileSystemState = new TestMemoryFileSystem()) {
            try (TestFileSystem actualFileSystemState = new TestAlluxioFileSystem()) {
                TrinoFileSystem expectedFileSystem = expectedFileSystemState.create();
                TrinoFileSystem testFileSystem = actualFileSystemState.create();

                Location expectedLocation = expectedFileSystemState.tempLocation();
                Location testLocation = actualFileSystemState.tempLocation();

                int fileSize = random.nextInt(0, toIntExact(CACHE_SIZE.toBytes() / 2));

                createTestFile(expectedFileSystem, expectedLocation, fileSize);
                createTestFile(testFileSystem, testLocation, fileSize);

                T expectedInput = createInput.apply(expectedFileSystem, expectedLocation);
                T actualInput = createInput.apply(testFileSystem, testLocation);

                for (int i = 0; i < 1000; i++) {
                    applyOperation(random, fileSize, expectedInput, actualInput, operation);
                }
            }
        }
    }

    public <T> void applyOperation(Random random, int fileSize, T expectedInput, T actualInput, TrinoInputOperation<T> operation)
            throws IOException
    {
        long position = random.nextLong(0, fileSize + 1);
        int bufferSize = random.nextInt(0, fileSize + 1);
        int bufferOffset = random.nextInt(0, bufferSize + 1);
        int length = bufferSize - bufferOffset;
        byte[] bufferExpected = new byte[bufferSize];
        byte[] bufferActual = new byte[bufferSize];

        int expectedBytesRead = operation.apply(expectedInput, position, bufferExpected, bufferOffset, length);
        int actualBytesRead = operation.apply(actualInput, position, bufferActual, bufferOffset, length);

        assertThat(actualBytesRead).isEqualTo(expectedBytesRead);
        assertThat(Slices.wrappedBuffer(bufferActual)).isEqualTo(Slices.wrappedBuffer(bufferExpected));
    }

    private static void createTestFile(TrinoFileSystem fileSystem, Location location, int fileSize)
            throws IOException
    {
        try (OutputStream output = fileSystem.newOutputFile(location).create()) {
            byte[] bytes = new byte[4];
            Slice slice = Slices.wrappedBuffer(bytes);
            for (int i = 0; i < fileSize; i++) {
                slice.setInt(0, i);
                output.write(bytes, 0, min(fileSize - i, 4));
            }
        }
    }

    private interface TestFileSystem
            extends Closeable
    {
        TrinoFileSystem create()
                throws IOException;

        Location tempLocation();
    }

    private static class TestMemoryFileSystem
            implements TestFileSystem
    {
        @Override
        public TrinoFileSystem create()
        {
            return new MemoryFileSystemFactory().create(ConnectorIdentity.ofUser(""));
        }

        @Override
        public Location tempLocation()
        {
            return Location.of("memory:///fuzz");
        }

        @Override
        public void close() {}
    }

    private static class TestAlluxioFileSystem
            implements TestFileSystem
    {
        private Path tempDirectory;

        @Override
        public TrinoFileSystem create()
                throws IOException
        {
            tempDirectory = Files.createTempDirectory("test");
            Path cacheDirectory = Files.createDirectory(tempDirectory.resolve("cache"));

            AlluxioFileSystemCacheConfig configuration = new AlluxioFileSystemCacheConfig()
                    .setCacheDirectories(ImmutableList.of(cacheDirectory.toAbsolutePath().toString()))
                    .setCachePageSize(PAGE_SIZE)
                    .disableTTL()
                    .setMaxCacheSizes(ImmutableList.of(CACHE_SIZE));
            AlluxioFileSystemCache alluxioCache = new AlluxioFileSystemCache(Tracing.noopTracer(), configuration, new AlluxioCacheStats());
            return new CacheFileSystem(new IncompleteStreamMemoryFileSystem(), alluxioCache, new TestingCacheKeyProvider());
        }

        @Override
        public Location tempLocation()
        {
            return Location.of("memory:///fuzz");
        }

        @Override
        public void close()
        {
            tempDirectory.toFile().delete();
        }
    }

    private interface TrinoInputOperation<T>
    {
        int apply(T input, long position, byte[] buffer, int bufferOffset, int bufferLength)
                throws IOException;
    }

    private interface CreateTrinoInput<T>
    {
        T apply(TrinoFileSystem fileSystem, Location location)
                throws IOException;
    }
}
