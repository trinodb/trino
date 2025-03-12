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
package io.trino.spooling.filesystem;

import com.google.common.collect.ImmutableList;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputStream;
import io.trino.filesystem.memory.MemoryFileSystem;
import io.trino.spi.QueryId;
import io.trino.spi.spool.SpoolingContext;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.temporal.ChronoUnit.MILLIS;
import static org.assertj.core.api.Assertions.assertThat;

abstract class AbstractFileSystemSegmentPrunerTest
{
    private static final Location TEST_LOCATION = Location.of("memory://");

    private static final FileSystemSpoolingConfig SPOOLING_CONFIG = new FileSystemSpoolingConfig()
            .setLocation(TEST_LOCATION.toString())
            .setPruningBatchSize(1);

    protected abstract FileSystemLayout layout();

    @Test
    public void shouldPruneExpiredSegments()
    {
        MemoryFileSystem fileSystem = new MemoryFileSystem();
        try (ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor()) {
            FileSystemSegmentPruner pruner = new FileSystemSegmentPruner(SPOOLING_CONFIG, _ -> fileSystem, layout(), executorService);

            Instant now = Instant.now();
            QueryId queryId = randomQueryId("prune_expired");

            writeDataSegment(fileSystem, queryId, now.minusSeconds(1));
            Location nonExpiredSegment = writeDataSegment(fileSystem, queryId, now.plusSeconds(1));

            pruner.pruneExpiredBefore(now.truncatedTo(MILLIS));

            List<Location> files = listFiles(fileSystem, queryId);
            assertThat(files)
                    .hasSize(1)
                    .containsOnly(nonExpiredSegment);
        }
    }

    @Test
    public void shouldPruneExpiredSegmentsOnceAndClear()
    {
        MemoryFileSystem fileSystem = new MemoryFileSystem();
        try (ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor()) {
            FileSystemSegmentPruner pruner = new FileSystemSegmentPruner(SPOOLING_CONFIG, _ -> fileSystem, layout(), executorService);

            Instant now = Instant.now();
            QueryId queryId = randomQueryId("prune_expired");

            writeDataSegment(fileSystem, queryId, now.minusSeconds(1));
            writeDataSegment(fileSystem, queryId, now.minusSeconds(1));
            writeDataSegment(fileSystem, queryId, now.minusSeconds(1));

            Location nonExpiredSegment = writeDataSegment(fileSystem, queryId, now.plusSeconds(1));

            assertThat(pruner.pruneExpiredBefore(now.truncatedTo(MILLIS)))
                    .isEqualTo(3);

            List<Location> files = listFiles(fileSystem, queryId);
            assertThat(files)
                    .hasSize(1)
                    .containsOnly(nonExpiredSegment);
        }
    }

    @Test
    public void shouldNotPruneLiveSegments()
    {
        MemoryFileSystem fileSystem = new MemoryFileSystem();
        try (ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor()) {
            FileSystemSegmentPruner pruner = new FileSystemSegmentPruner(SPOOLING_CONFIG, _ -> fileSystem, layout(), executorService);

            Instant now = Instant.now();

            QueryId queryId = randomQueryId("prune_live");

            writeDataSegment(fileSystem, queryId, now.plusSeconds(1));
            writeDataSegment(fileSystem, queryId, now.plusSeconds(2));

            pruner.pruneExpiredBefore(now.truncatedTo(MILLIS));

            List<Location> files = listFiles(fileSystem, queryId);
            assertThat(files)
                    .hasSize(2);
        }
    }

    @Test
    public void shouldNotPruneSegmentsIfNotStrictlyBeforeExpiration()
    {
        TrinoFileSystem memoryFileSystem = new MemoryFileSystem();

        try (ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor()) {
            FileSystemSegmentPruner pruner = new FileSystemSegmentPruner(SPOOLING_CONFIG, _ -> memoryFileSystem, layout(), executorService);

            Instant now = Instant.now();

            QueryId queryId = randomQueryId("prune_now");

            Location firstSegment = writeDataSegment(memoryFileSystem, queryId, now);
            Location secondSegment = writeDataSegment(memoryFileSystem, queryId, now);

            pruner.pruneExpiredBefore(now.truncatedTo(MILLIS));

            List<Location> files = listFiles(memoryFileSystem, queryId);
            assertThat(files)
                    .hasSize(2)
                    .containsOnly(firstSegment, secondSegment);
        }
    }

    private Location writeDataSegment(TrinoFileSystem fileSystem, QueryId queryId, Instant ttl)
    {
        SpoolingContext context = new SpoolingContext("encoding", queryId, 100, 1000);
        FileSystemSpooledSegmentHandle handle = FileSystemSpooledSegmentHandle.random(ThreadLocalRandom.current(), context, ttl);
        Location location = layout().location(TEST_LOCATION, handle);
        try (OutputStream stream = fileSystem.newOutputFile(location).create()) {
            stream.write(queryId.toString().getBytes(UTF_8));
            return location;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private List<Location> listFiles(TrinoFileSystem fileSystem, QueryId queryId)
    {
        ImmutableList.Builder<Location> files = ImmutableList.builder();

        try {
            FileIterator iterator = fileSystem.listFiles(TEST_LOCATION);
            while (iterator.hasNext()) {
                FileEntry entry = iterator.next();
                try (TrinoInputStream stream = fileSystem.newInputFile(entry.location()).newStream()) {
                    if (Arrays.equals(stream.readAllBytes(), queryId.toString().getBytes(UTF_8))) {
                        files.add(entry.location());
                    }
                }
            }
            return files.build();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static QueryId randomQueryId(String name)
    {
        return QueryId.valueOf(name + "_" + UUID.randomUUID().toString().replace("-", ""));
    }
}
