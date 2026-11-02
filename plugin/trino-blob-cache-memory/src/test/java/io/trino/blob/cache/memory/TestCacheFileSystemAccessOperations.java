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
package io.trino.blob.cache.memory;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.filesystem.cache.CacheFileSystem;
import io.trino.filesystem.cache.DefaultCacheKeyProvider;
import io.trino.filesystem.memory.MemoryFileSystemFactory;
import io.trino.filesystem.tracing.TracingFileSystemFactory;
import io.trino.testing.TestingTelemetry;
import io.trino.testing.connector.TestingConnectorSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;

import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.trino.filesystem.tracing.FileSystemAttributes.FILE_LOCATION;
import static io.trino.testing.MultisetAssertions.assertMultisetsEqual;
import static java.lang.Math.toIntExact;
import static java.util.concurrent.TimeUnit.HOURS;
import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(Lifecycle.PER_CLASS)
public class TestCacheFileSystemAccessOperations
{
    private static final DataSize MAX_CONTENT_LENGTH = DataSize.of(64, KILOBYTE);

    private TrinoFileSystemFactory tracingFileSystemFactory;
    private CacheFileSystem fileSystem;
    private final TestingTelemetry telemetry = TestingTelemetry.create("cache-file-system");

    @BeforeAll
    void setUp()
    {
        tracingFileSystemFactory = new TracingFileSystemFactory(telemetry.getTracer(), new MemoryFileSystemFactory());
        MemoryBlobCacheConfig configuration = new MemoryBlobCacheConfig()
                .setCacheTtl(new Duration(24, HOURS))
                .setMaxContentLength(MAX_CONTENT_LENGTH);
        fileSystem = new CacheFileSystem(tracingFileSystemFactory.create(TestingConnectorSession.SESSION), new MemoryBlobCache(configuration), new DefaultCacheKeyProvider());
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
                        .add(new FileOperation(location, "InputFile.newInput"))
                        .add(new FileOperation(location, "Input.readFully"))
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
                        .add(new FileOperation(location, "InputFile.newInput"))
                        .add(new FileOperation(location, "Input.readFully"))
                        .add(new FileOperation(location, "InputFile.lastModified"))
                        .build());
    }

    @Test
    void testStreamOfCachedFile()
            throws IOException
    {
        Location location = getRootLocation().appendPath("cached-stream");
        byte[] content = randomContent(toIntExact(MAX_CONTENT_LENGTH.toBytes()) / 2);
        fileSystem.newOutputFile(location).createOrOverwrite(content);

        assertStreamOperations(location, content,
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(location, "InputFile.length"))
                        .add(new FileOperation(location, "InputFile.newInput"))
                        .add(new FileOperation(location, "Input.readFully"))
                        .add(new FileOperation(location, "InputFile.lastModified"))
                        .build());
        assertStreamOperations(location, content,
                ImmutableMultiset.<FileOperation>builder()
                        .add(new FileOperation(location, "InputFile.length"))
                        .add(new FileOperation(location, "InputFile.lastModified"))
                        .build());
    }

    @Test
    void testStreamOfFileExceedingMaxContentLength()
            throws IOException
    {
        Location location = getRootLocation().appendPath("large-stream");
        byte[] content = randomContent(toIntExact(MAX_CONTENT_LENGTH.toBytes()) * 4);
        fileSystem.newOutputFile(location).createOrOverwrite(content);

        Multiset<FileOperation> singleStream = ImmutableMultiset.<FileOperation>builder()
                .add(new FileOperation(location, "InputFile.length"))
                .add(new FileOperation(location, "InputFile.newStream"))
                .add(new FileOperation(location, "InputFile.lastModified"))
                .build();
        assertStreamOperations(location, content, singleStream);
        assertStreamOperations(location, content, singleStream);
    }

    @Test
    void testInputOfFileExceedingMaxContentLength()
            throws IOException
    {
        Location location = getRootLocation().appendPath("large-input");
        byte[] content = randomContent(toIntExact(MAX_CONTENT_LENGTH.toBytes()) * 4);
        fileSystem.newOutputFile(location).createOrOverwrite(content);

        Multiset<FileOperation> singleInput = ImmutableMultiset.<FileOperation>builder()
                .add(new FileOperation(location, "InputFile.length"))
                .add(new FileOperation(location, "InputFile.newInput"))
                .add(new FileOperation(location, "Input.readFully"))
                .add(new FileOperation(location, "InputFile.lastModified"))
                .build();
        assertReadOperations(location, content, singleInput);
        assertReadOperations(location, content, singleInput);
    }

    private Location getRootLocation()
    {
        return Location.of("memory://");
    }

    private static byte[] randomContent(int length)
    {
        byte[] content = new byte[length];
        new Random(42).nextBytes(content);
        return content;
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

        assertMultisetsEqual(getOperations(spans), fileOperations);
    }

    private void assertStreamOperations(Location location, byte[] content, Multiset<FileOperation> fileOperations)
            throws IOException
    {
        List<SpanData> spans = telemetry.captureSpans(() -> {
            TrinoInputFile file = fileSystem.newInputFile(location);
            try (TrinoInputStream stream = file.newStream()) {
                assertThat(stream.readAllBytes()).isEqualTo(content);
            }
        });

        assertMultisetsEqual(getOperations(spans), fileOperations);
    }

    private Multiset<FileOperation> getOperations(List<SpanData> spans)
    {
        HashMultiset<FileOperation> operations = HashMultiset.create();
        for (SpanData span : spans) {
            if (span.getName().startsWith("InputFile.") || span.getName().startsWith("Input.")) {
                operations.add(new FileOperation(Location.of(span.getAttributes().get(FILE_LOCATION)), span.getName()));
            }
        }
        return operations;
    }

    private record FileOperation(Location path, String operationType) {}
}
