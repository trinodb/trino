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

import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import io.airlift.slice.Slices;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrackingFileSystemFactory;
import io.trino.filesystem.TrackingFileSystemFactory.OperationType;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.memory.MemoryFileSystemFactory;
import io.trino.spi.block.TestingSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import static io.trino.testing.MultisetAssertions.assertMultisetsEqual;
import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.toCollection;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class TestCacheFileSystemAccessOperations
{
    private TrackingFileSystemFactory trackingFileSystemFactory;
    private CacheFileSystem fileSystem;

    @BeforeAll
    void setUp()
    {
        trackingFileSystemFactory = new TrackingFileSystemFactory(new MemoryFileSystemFactory());
        fileSystem = new CacheFileSystem(trackingFileSystemFactory.create(TestingSession.SESSION), new TestingMemoryFileSystemCache(), new DefaultCacheKeyProvider());
    }

    @AfterAll
    void tearDown()
    {
        trackingFileSystemFactory = null;
        fileSystem = null;
    }

    @Test
    void testCache()
            throws IOException
    {
        Location location = getRootLocation().appendPath("hello");
        byte[] content = "hello world".getBytes(StandardCharsets.UTF_8);
        try (OutputStream output = fileSystem.newOutputFile(location).create()) {
            output.write(content);
        }

        assertReadOperations(location, content,
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(location, OperationType.INPUT_FILE_NEW_STREAM))
                        .add(new FileOperation(location, OperationType.INPUT_FILE_LAST_MODIFIED))
                        .build());
        assertReadOperations(location, content,
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(location, OperationType.INPUT_FILE_LAST_MODIFIED))
                        .build());

        byte[] modifiedContent = "modified content".getBytes(StandardCharsets.UTF_8);
        fileSystem.newOutputFile(location).createOrOverwrite(modifiedContent);

        assertReadOperations(location, modifiedContent,
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(location, OperationType.INPUT_FILE_NEW_STREAM))
                        .add(new FileOperation(location, OperationType.INPUT_FILE_LAST_MODIFIED))
                        .build());
    }

    private Location getRootLocation()
    {
        return Location.of("memory://");
    }

    private void assertReadOperations(Location location, byte[] content, Multiset<FileOperation> fileOperations)
            throws IOException
    {
        TrinoInputFile file = fileSystem.newInputFile(location);
        int length = (int) file.length();
        trackingFileSystemFactory.reset();
        try (TrinoInput input = file.newInput()) {
            assertThat(input.readFully(0, length)).isEqualTo(Slices.wrappedBuffer(content));
        }
        assertMultisetsEqual(fileOperations, getOperations());
    }

    private Multiset<FileOperation> getOperations()
    {
        return trackingFileSystemFactory.getOperationCounts()
                .entrySet().stream()
                .flatMap(entry -> nCopies(entry.getValue(), new FileOperation(
                        entry.getKey().location(),
                        entry.getKey().operationType())).stream())
                .collect(toCollection(HashMultiset::create));
    }

    private record FileOperation(Location path, OperationType operationType) {}
}
