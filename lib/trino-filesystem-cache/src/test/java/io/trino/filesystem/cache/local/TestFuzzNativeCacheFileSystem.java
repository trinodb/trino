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
package io.trino.filesystem.cache.local;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.cache.CacheFileSystem;
import io.trino.filesystem.cache.DefaultCacheKeyProvider;
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
import java.util.List;
import java.util.Random;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(Lifecycle.PER_METHOD)
class TestFuzzNativeCacheFileSystem
{
    private static final DataSize CACHE_SIZE = DataSize.ofBytes(8 * 1024);
    private static final DataSize PAGE_SIZE = DataSize.ofBytes(128);

    @Test
    void testFuzzTrinoInputReadFully()
            throws IOException
    {
        fuzzTrinoInputOperation(
                (fileSystem, location) -> fileSystem.newInputFile(location).newInput(),
                (input, position, buffer, bufferOffset, readLength) -> {
                    input.readFully(position, buffer, bufferOffset, readLength);
                    return readLength;
                });
    }

    @Test
    void testFuzzTrinoInputReadTail()
            throws IOException
    {
        fuzzTrinoInputOperation(
                (fileSystem, location) -> fileSystem.newInputFile(location).newInput(),
                (input, _, buffer, bufferOffset, readLength) -> input.readTail(buffer, bufferOffset, readLength));
    }

    @Test
    void testFuzzTrinoInputStreamRead()
            throws IOException
    {
        fuzzTrinoInputOperation(
                (fileSystem, location) -> fileSystem.newInputFile(location).newStream(),
                (input, position, buffer, bufferOffset, readLength) -> {
                    input.seek(position);
                    return input.read(buffer, bufferOffset, readLength);
                });
    }

    @Test
    void testFuzzTrinoInputStreamReadSkip()
            throws IOException
    {
        fuzzTrinoInputOperation(
                (fileSystem, location) -> fileSystem.newInputFile(location).newStream(),
                (input, position, buffer, bufferOffset, readLength) -> {
                    input.skip(position);
                    return input.read(buffer, bufferOffset, readLength);
                });
    }

    private static <T> void fuzzTrinoInputOperation(CreateTrinoInput<T> createInput, TrinoInputOperation<T> operation)
            throws IOException
    {
        Random random = new Random(42);
        try (TestFileSystem expectedFileSystemState = new TestMemoryFileSystem();
                TestFileSystem actualFileSystemState = new TestNativeCacheFileSystem()) {
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

    private static <T> void applyOperation(Random random, int fileSize, T expectedInput, T actualInput, TrinoInputOperation<T> operation)
            throws IOException
    {
        long position = random.nextLong(0, fileSize + 1);
        int bufferSize = random.nextInt(0, fileSize + 1);
        int bufferOffset = random.nextInt(0, bufferSize + 1);
        int readLength = bufferSize - bufferOffset;
        byte[] bufferExpected = new byte[bufferSize];
        byte[] bufferActual = new byte[bufferSize];

        int expectedBytesRead = operation.apply(expectedInput, position, bufferExpected, bufferOffset, readLength);
        int actualBytesRead = operation.apply(actualInput, position, bufferActual, bufferOffset, readLength);

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

    private static class TestNativeCacheFileSystem
            implements TestFileSystem
    {
        private Path tempDirectory;

        @Override
        public TrinoFileSystem create()
                throws IOException
        {
            tempDirectory = Files.createTempDirectory("test");
            Path cacheDirectory = Files.createDirectory(tempDirectory.resolve("cache"));
            return new CacheFileSystem(
                    new IncompleteStreamMemoryFileSystem(),
                    new NativeFileSystemCache(List.of(cacheDirectory), PAGE_SIZE, new NativeFileSystemCacheStats()),
                    new DefaultCacheKeyProvider());
        }

        @Override
        public Location tempLocation()
        {
            return Location.of("memory:///fuzz");
        }

        @Override
        public void close()
                throws IOException
        {
            deleteRecursively(tempDirectory, ALLOW_INSECURE);
        }
    }

    private interface TrinoInputOperation<T>
    {
        int apply(T input, long position, byte[] buffer, int bufferOffset, int readLength)
                throws IOException;
    }

    private interface CreateTrinoInput<T>
    {
        T apply(TrinoFileSystem fileSystem, Location location)
                throws IOException;
    }
}
