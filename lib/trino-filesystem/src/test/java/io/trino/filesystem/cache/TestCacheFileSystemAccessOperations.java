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
import io.opentelemetry.sdk.trace.data.SpanData;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.memory.MemoryFileSystemFactory;
import io.trino.filesystem.tracing.TracingFileSystemFactory;
import io.trino.spi.block.TestingSession;
import io.trino.testing.TestingTelemetry;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static io.trino.filesystem.tracing.FileSystemAttributes.FILE_LOCATION;
import static io.trino.testing.MultisetAssertions.assertMultisetsEqual;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class TestCacheFileSystemAccessOperations
{
    private TrinoFileSystemFactory tracingFileSystemFactory;
    private CacheFileSystem fileSystem;
    private final TestingTelemetry telemetry = TestingTelemetry.create("cache-file-system");

    @BeforeAll
    void setUp()
    {
        tracingFileSystemFactory = new TracingFileSystemFactory(telemetry.getTracer(), new MemoryFileSystemFactory());
        fileSystem = new CacheFileSystem(tracingFileSystemFactory.create(TestingSession.SESSION), new TestingMemoryFileSystemCache(), new DefaultCacheKeyProvider());
    }

    @AfterAll
    void tearDown()
    {
        tracingFileSystemFactory = null;
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
                        .add(new FileOperation(location, "InputFile.length"))
                        .add(new FileOperation(location, "InputFile.newStream"))
                        .add(new FileOperation(location, "InputFile.lastModified"))
                        .build());
        assertReadOperations(location, content,
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(location, "InputFile.length"))
                        .add(new FileOperation(location, "InputFile.lastModified"))
                        .build());

        byte[] modifiedContent = "modified content".getBytes(StandardCharsets.UTF_8);
        fileSystem.newOutputFile(location).createOrOverwrite(modifiedContent);

        assertReadOperations(location, modifiedContent,
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(location, "InputFile.length"))
                        .add(new FileOperation(location, "InputFile.newStream"))
                        .add(new FileOperation(location, "InputFile.lastModified"))
                        .build());
    }

    private Location getRootLocation()
    {
        return Location.of("memory://");
    }

    private void assertReadOperations(Location location, byte[] content, Multiset<FileOperation> fileOperations)
            throws IOException
    {
        List<SpanData> spans = telemetry.captureSpans(() -> {
            TrinoInputFile file = fileSystem.newInputFile(location);
            int length = (int) file.length();
            try (TrinoInput input = file.newInput()) {
                assertThat(input.readFully(0, length)).isEqualTo(Slices.wrappedBuffer(content));
            }
        });

        assertMultisetsEqual(fileOperations, getOperations(spans));
    }

    private Multiset<FileOperation> getOperations(List<SpanData> spans)
    {
        HashMultiset<@Nullable FileOperation> operations = HashMultiset.create();
        for (SpanData span : spans) {
            if (span.getName().startsWith("InputFile.")) {
                operations.add(new FileOperation(Location.of(span.getAttributes().get(FILE_LOCATION)), span.getName()));
            }
        }
        return operations;
    }

    private record FileOperation(Location path, String operationType) {}
}
