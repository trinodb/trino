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

import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import io.airlift.units.DataSize;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.filesystem.cache.CacheFileSystem;
import io.trino.filesystem.cache.CacheKeyProvider;
import io.trino.filesystem.memory.MemoryFileSystemFactory;
import io.trino.filesystem.tracing.TracingFileSystemCache;
import io.trino.filesystem.tracing.TracingFileSystemFactory;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.TestingTelemetry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_LOCATION;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_READ_POSITION;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_READ_SIZE;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_WRITE_POSITION;
import static io.trino.filesystem.tracing.CacheSystemAttributes.CACHE_FILE_WRITE_SIZE;
import static io.trino.filesystem.tracing.FileSystemAttributes.FILE_LOCATION;
import static io.trino.filesystem.tracing.FileSystemAttributes.FILE_READ_POSITION;
import static io.trino.filesystem.tracing.FileSystemAttributes.FILE_READ_SIZE;
import static io.trino.testing.MultisetAssertions.assertMultisetsEqual;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;
import static org.assertj.core.api.Assertions.assertThat;

final class TestNativeCacheFileSystemAccessOperations
{
    private static final int PAGE_SIZE = 128;
    private static final CacheKeyProvider PATH_CACHE_KEY_PROVIDER = inputFile -> Optional.of(inputFile.location().path());

    @Test
    void testPositionedReadTracing(@TempDir Path cacheRoot)
            throws IOException
    {
        TestingCacheFileSystem fileSystem = createFileSystem(cacheRoot);
        Location location = Location.of("memory:///positioned");
        byte[] content = "hello world".getBytes(UTF_8);
        fileSystem.write(location, content);

        List<SpanData> spans = fileSystem.telemetry().captureSpans(() -> {
            TrinoInputFile inputFile = fileSystem.fileSystem().newInputFile(location);
            try (TrinoInput input = inputFile.newInput()) {
                assertThat(input.readFully(0, content.length)).isEqualTo(wrappedBuffer(content));
                assertThat(input.readFully(0, content.length)).isEqualTo(wrappedBuffer(content));
            }
        });

        assertMultisetsEqual(getCacheOperations(spans), ImmutableMultiset.<CacheOperationSpan>builder()
                .addCopies(new CacheOperationSpan("NativeCache.readCached", location.toString(), 0, content.length), 2)
                .add(new CacheOperationSpan("Input.readFully", location.toString(), 0, content.length))
                .add(new CacheOperationSpan("NativeCache.readExternalStream", location.toString(), 0, content.length))
                .add(new CacheOperationSpan("NativeCache.writeCache", location.toString(), 0, content.length))
                .build());
    }

    @Test
    void testStreamReadTracing(@TempDir Path cacheRoot)
            throws IOException
    {
        TestingCacheFileSystem fileSystem = createFileSystem(cacheRoot);
        Location location = Location.of("memory:///stream");
        byte[] content = "hello world".getBytes(UTF_8);
        fileSystem.write(location, content);

        List<SpanData> spans = fileSystem.telemetry().captureSpans(() -> {
            byte[] buffer = new byte[content.length];
            try (TrinoInputStream stream = fileSystem.fileSystem().newInputFile(location).newStream()) {
                assertThat(stream.read(buffer, 0, buffer.length)).isEqualTo(buffer.length);
            }
            assertThat(buffer).isEqualTo(content);

            try (TrinoInputStream stream = fileSystem.fileSystem().newInputFile(location).newStream()) {
                assertThat(stream.read(buffer, 0, buffer.length)).isEqualTo(buffer.length);
            }
            assertThat(buffer).isEqualTo(content);
        });

        assertMultisetsEqual(getCacheOperations(spans), ImmutableMultiset.<CacheOperationSpan>builder()
                .addCopies(new CacheOperationSpan("NativeCache.readCached", location.toString(), 0, content.length), 2)
                .add(new CacheOperationSpan("NativeCache.readExternalStream", location.toString(), 0, content.length))
                .add(new CacheOperationSpan("NativeCache.writeCache", location.toString(), 0, content.length))
                .build());
    }

    private static TestingCacheFileSystem createFileSystem(Path cacheRoot)
    {
        TestingTelemetry telemetry = TestingTelemetry.create("local-cache");
        TracingFileSystemFactory tracingFileSystemFactory = new TracingFileSystemFactory(telemetry.getTracer(), new MemoryFileSystemFactory());
        NativeFileSystemCache cache = new NativeFileSystemCache(telemetry.getTracer(), List.of(cacheRoot), DataSize.ofBytes(PAGE_SIZE), new NativeFileSystemCacheStats());
        CacheFileSystem fileSystem = new CacheFileSystem(
                tracingFileSystemFactory.create(ConnectorIdentity.ofUser("test")),
                new TracingFileSystemCache(telemetry.getTracer(), cache),
                PATH_CACHE_KEY_PROVIDER);
        return new TestingCacheFileSystem(fileSystem, telemetry);
    }

    private static Multiset<CacheOperationSpan> getCacheOperations(List<SpanData> spans)
    {
        return spans.stream()
                .filter(span -> span.getName().startsWith("Input.") || span.getName().startsWith("NativeCache."))
                .map(CacheOperationSpan::create)
                .collect(toCollection(HashMultiset::create));
    }

    private record TestingCacheFileSystem(CacheFileSystem fileSystem, TestingTelemetry telemetry)
    {
        void write(Location location, byte[] content)
                throws IOException
        {
            try (OutputStream output = fileSystem.newOutputFile(location).create()) {
                output.write(content);
            }
        }
    }

    private record CacheOperationSpan(String spanName, String location, long position, long length)
    {
        static CacheOperationSpan create(SpanData span)
        {
            Attributes attributes = span.getAttributes();

            long length = switch (span.getName()) {
                case "NativeCache.readCached", "NativeCache.readExternalStream" -> requireNonNull(attributes.get(CACHE_FILE_READ_SIZE));
                case "NativeCache.writeCache" -> requireNonNull(attributes.get(CACHE_FILE_WRITE_SIZE));
                case "Input.readFully" -> requireNonNull(attributes.get(FILE_READ_SIZE));
                default -> throw new IllegalArgumentException("Unrecognized span " + span.getName() + " [" + span.getAttributes() + "]");
            };

            long position = switch (span.getName()) {
                case "NativeCache.readCached", "NativeCache.readExternalStream" -> requireNonNull(attributes.get(CACHE_FILE_READ_POSITION));
                case "NativeCache.writeCache" -> requireNonNull(attributes.get(CACHE_FILE_WRITE_POSITION));
                case "Input.readFully" -> requireNonNull(attributes.get(FILE_READ_POSITION));
                default -> throw new IllegalArgumentException("Unrecognized span " + span.getName() + " [" + span.getAttributes() + "]");
            };

            return new CacheOperationSpan(span.getName(), getLocation(span), position, length);
        }

        @Override
        public String toString()
        {
            return "CacheOperationSpan(\"%s\", \"%s\", %d, %d)".formatted(spanName, location, position, length);
        }
    }

    private static String getLocation(SpanData span)
    {
        if (span.getName().startsWith("Input.")) {
            return requireNonNull(span.getAttributes().get(FILE_LOCATION));
        }
        return requireNonNull(span.getAttributes().get(CACHE_FILE_LOCATION));
    }
}
