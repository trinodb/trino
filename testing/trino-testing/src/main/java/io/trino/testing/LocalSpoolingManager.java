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

import io.trino.spi.Plugin;
import io.trino.spi.protocol.SpooledLocation;
import io.trino.spi.protocol.SpooledLocation.CoordinatorLocation;
import io.trino.spi.protocol.SpooledSegmentHandle;
import io.trino.spi.protocol.SpoolingContext;
import io.trino.spi.protocol.SpoolingManager;
import io.trino.spi.protocol.SpoolingManagerContext;
import io.trino.spi.protocol.SpoolingManagerFactory;
import jakarta.annotation.PreDestroy;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static io.airlift.slice.Slices.utf8Slice;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.util.Objects.requireNonNull;

public class LocalSpoolingManager
        implements SpoolingManager
{
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
        return new LocalSpooledSegmentHandle(rootPath
                .resolve(context.queryId().getId() + "-" + segmentId.incrementAndGet() + "-" + UUID.randomUUID()));
    }

    @Override
    public OutputStream createOutputStream(SpooledSegmentHandle handle)
            throws IOException
    {
        LocalSpooledSegmentHandle localHandle = (LocalSpooledSegmentHandle) handle;
        return Files.newOutputStream(localHandle.path(), CREATE_NEW);
    }

    @Override
    public InputStream openInputStream(SpooledSegmentHandle handle)
            throws IOException
    {
        LocalSpooledSegmentHandle localHandle = (LocalSpooledSegmentHandle) handle;
        if (!Files.exists(localHandle.path())) {
            throw new IOException("Segment not found or expired");
        }
        return Files.newInputStream(localHandle.path());
    }

    @Override
    public SpooledSegmentHandle handle(SpooledLocation location)
    {
        if (!(location instanceof CoordinatorLocation coordinatorLocation)) {
            throw new IllegalArgumentException("Cannot convert direct location to handle");
        }
        return new LocalSpooledSegmentHandle(Paths.get(coordinatorLocation.identifier().toStringUtf8()));
    }

    @Override
    public SpooledLocation location(SpooledSegmentHandle handle)
    {
        LocalSpooledSegmentHandle localHandle = (LocalSpooledSegmentHandle) handle;
        return SpooledLocation.coordinatorLocation(utf8Slice(localHandle.path().toFile().getPath()), Map.of());
    }

    @Override
    public void acknowledge(SpooledSegmentHandle handle)
            throws IOException
    {
        LocalSpooledSegmentHandle localHandle = (LocalSpooledSegmentHandle) handle;
        if (!Files.exists(localHandle.path())) {
            throw new IOException("Segment not found or expired");
        }

        Files.delete(localHandle.path());
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

    private record LocalSpooledSegmentHandle(Path path)
            implements SpooledSegmentHandle
    {
        private LocalSpooledSegmentHandle
        {
            requireNonNull(path, "path is null");
        }
    }
}
