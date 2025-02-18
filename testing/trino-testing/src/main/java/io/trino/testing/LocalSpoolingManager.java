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
package io.trino.testing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.DoNotCall;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.trino.spi.Plugin;
import io.trino.spi.spool.SpooledLocation;
import io.trino.spi.spool.SpooledLocation.DirectLocation;
import io.trino.spi.spool.SpooledSegmentHandle;
import io.trino.spi.spool.SpoolingContext;
import io.trino.spi.spool.SpoolingManager;
import io.trino.spi.spool.SpoolingManagerContext;
import io.trino.spi.spool.SpoolingManagerFactory;
import jakarta.annotation.PreDestroy;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.slice.Slices.utf8Slice;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.util.Objects.requireNonNull;

public class LocalSpoolingManager
        implements SpoolingManager
{
    private static final JsonCodec<LocalSpooledSegmentHandle> HANDLE_CODEC = jsonCodec(LocalSpooledSegmentHandle.class);
    private final Path rootPath;
    private final AtomicLong segmentId = new AtomicLong();

    public LocalSpoolingManager()
    {
        try {
            this.rootPath = Files.createTempDirectory("spooling");
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public SpooledSegmentHandle create(SpoolingContext context)
    {
        return new LocalSpooledSegmentHandle(context.encoding(), rootPath.resolve(segmentId.incrementAndGet() + "-" + UUID.randomUUID() + "." + context.encoding()));
    }

    @Override
    public OutputStream createOutputStream(SpooledSegmentHandle handle)
            throws IOException
    {
        LocalSpooledSegmentHandle localHandle = (LocalSpooledSegmentHandle) handle;
        return Files.newOutputStream(localHandle.getPath(), CREATE_NEW);
    }

    @Override
    public InputStream openInputStream(SpooledSegmentHandle handle)
            throws IOException
    {
        LocalSpooledSegmentHandle localHandle = (LocalSpooledSegmentHandle) handle;
        if (!Files.exists(localHandle.getPath())) {
            throw new IOException("Segment not found or expired");
        }
        return Files.newInputStream(localHandle.getPath());
    }

    @Override
    public SpooledSegmentHandle handle(Slice identifier, Map<String, List<String>> headers)
    {
        return HANDLE_CODEC.fromJson(identifier.toStringUtf8());
    }

    @Override
    public SpooledLocation location(SpooledSegmentHandle handle)
    {
        LocalSpooledSegmentHandle localHandle = (LocalSpooledSegmentHandle) handle;
        return SpooledLocation.coordinatorLocation(utf8Slice(HANDLE_CODEC.toJson(localHandle)), Map.of());
    }

    @Override
    public void acknowledge(SpooledSegmentHandle handle)
            throws IOException
    {
        LocalSpooledSegmentHandle localHandle = (LocalSpooledSegmentHandle) handle;
        if (!Files.exists(localHandle.getPath())) {
            throw new IOException("Segment not found or expired");
        }

        Files.delete(localHandle.getPath());
    }

    @Override
    public Optional<DirectLocation> directLocation(SpooledSegmentHandle handle)
    {
        throw new UnsupportedOperationException();
    }

    @PreDestroy
    public void close()
    {
        try {
            deleteRecursively(rootPath);
        }
        catch (IOException _) {
            // ignore
        }
    }

    public static class LocalSpoolingPlugin
            implements Plugin
    {
        @Override
        public Iterable<SpoolingManagerFactory> getSpoolingManagerFactories()
        {
            return List.of(new LocalSpoolingFactory());
        }
    }

    public static class LocalSpoolingFactory
            implements SpoolingManagerFactory
    {
        @Override
        public String getName()
        {
            return "test-local";
        }

        @Override
        public SpoolingManager create(Map<String, String> config, SpoolingManagerContext context)
        {
            return new LocalSpoolingManager();
        }
    }

    public static class LocalSpooledSegmentHandle
            implements SpooledSegmentHandle
    {
        private final String encoding;
        private final Path path;

        public LocalSpooledSegmentHandle(String encoding, Path path)
        {
            this.encoding = requireNonNull(encoding, "encoding is null");
            this.path = requireNonNull(path, "path is null");
        }

        @JsonIgnore
        @Override
        public Instant expirationTime()
        {
            return Instant.now().plus(5, MINUTES);
        }

        @JsonIgnore
        @Override
        public String identifier()
        {
            return path.getFileName().toString();
        }

        @JsonProperty
        @Override
        public String encoding()
        {
            return encoding;
        }

        @JsonIgnore
        public Path getPath()
        {
            return path;
        }

        @JsonProperty("path")
        public String getFilePath()
        {
            return path.toAbsolutePath().toString();
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("encoding", encoding)
                    .add("path", path)
                    .toString();
        }

        @DoNotCall
        @JsonCreator
        public static LocalSpooledSegmentHandle create(
                @JsonProperty("encoding") String encoding,
                @JsonProperty("path") String path)
        {
            return new LocalSpooledSegmentHandle(encoding, Paths.get(path));
        }
    }
}
