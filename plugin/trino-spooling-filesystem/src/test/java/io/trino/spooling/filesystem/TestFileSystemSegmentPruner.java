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
import io.airlift.units.DataSize;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.filesystem.s3.S3FileSystemFactory;
import io.trino.filesystem.s3.S3FileSystemStats;
import io.trino.spi.QueryId;
import io.trino.spi.protocol.SpoolingContext;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.containers.Minio;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;

import static io.opentelemetry.api.OpenTelemetry.noop;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.temporal.ChronoUnit.MILLIS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
class TestFileSystemSegmentPruner
{
    private Minio minio;
    private static final String BUCKET_NAME = "segments" + UUID.randomUUID().toString().replace("-", "");
    private static final Location LOCATION = Location.of("s3://" + BUCKET_NAME + "/");

    @BeforeAll
    public void setup()
    {
        minio = Minio.builder().build();
        minio.start();
        minio.createBucket(BUCKET_NAME);
    }

    @AfterAll
    public void teardown()
    {
        minio.stop();
    }

    @Test
    public void shouldPruneExpiredSegments()
    {
        try (ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor()) {
            TrinoFileSystemFactory fileSystemFactory = getFileSystemFactory();
            FileSystemSegmentPruner pruner = new FileSystemSegmentPruner(getSpoolingConfig(), fileSystemFactory, executorService);

            Instant now = Instant.now();
            QueryId queryId = QueryId.valueOf("prune_expired");

            Location _ = writeNewDummySegment(fileSystemFactory, queryId, now.minusSeconds(1));
            Location nonExpiredSegment = writeNewDummySegment(fileSystemFactory, queryId, now.plusSeconds(1));

            pruner.pruneExpiredBefore(now.truncatedTo(MILLIS));

            List<Location> files = listFiles(fileSystemFactory, queryId);
            assertThat(files)
                    .hasSize(1)
                    .containsOnly(nonExpiredSegment);
        }
    }

    @Test
    public void shouldNotPruneLiveSegments()
    {
        try (ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor()) {
            TrinoFileSystemFactory fileSystemFactory = getFileSystemFactory();
            FileSystemSegmentPruner pruner = new FileSystemSegmentPruner(getSpoolingConfig(), fileSystemFactory, executorService);

            Instant now = Instant.now();

            QueryId queryId = QueryId.valueOf("prune_live");

            Location _ = writeNewDummySegment(fileSystemFactory, queryId, now.plusSeconds(1));
            Location _ = writeNewDummySegment(fileSystemFactory, queryId, now.plusSeconds(2));

            pruner.pruneExpiredBefore(now.truncatedTo(MILLIS));

            List<Location> files = listFiles(fileSystemFactory, queryId);
            assertThat(files)
                    .hasSize(2);
        }
    }

    @Test
    public void shouldNotPruneSegmentsIfNotStrictlyBeforeExpiration()
    {
        try (ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor()) {
            TrinoFileSystemFactory fileSystemFactory = getFileSystemFactory();
            FileSystemSegmentPruner pruner = new FileSystemSegmentPruner(getSpoolingConfig(), fileSystemFactory, executorService);

            Instant now = Instant.now();

            QueryId queryId = QueryId.valueOf("prune_now");

            Location firstSegment = writeNewDummySegment(fileSystemFactory, queryId, now);
            Location secondSegment = writeNewDummySegment(fileSystemFactory, queryId, now);

            pruner.pruneExpiredBefore(now.truncatedTo(MILLIS));

            List<Location> files = listFiles(fileSystemFactory, queryId);
            assertThat(files)
                    .hasSize(2)
                    .containsOnly(firstSegment, secondSegment);
        }
    }

    private TrinoFileSystemFactory getFileSystemFactory()
    {
        S3FileSystemConfig filesystemConfig = new S3FileSystemConfig()
                .setEndpoint(minio.getMinioAddress())
                .setRegion(MINIO_REGION)
                .setPathStyleAccess(true)
                .setAwsAccessKey(Minio.MINIO_ACCESS_KEY)
                .setAwsSecretKey(Minio.MINIO_SECRET_KEY)
                .setStreamingPartSize(DataSize.valueOf("5.5MB"));
        return new S3FileSystemFactory(noop(), filesystemConfig, new S3FileSystemStats());
    }

    private Location writeNewDummySegment(TrinoFileSystemFactory fileSystemFactory, QueryId queryId, Instant ttl)
    {
        SpoolingContext context = new SpoolingContext("encoding", queryId, 100, 1000);
        FileSystemSpooledSegmentHandle handle = FileSystemSpooledSegmentHandle.random(ThreadLocalRandom.current(), context, ttl);
        try (OutputStream segment = createFileSystem(fileSystemFactory).newOutputFile(LOCATION.appendPath(handle.storageObjectName())).create()) {
            segment.write("dummy".getBytes(UTF_8));
            return LOCATION.appendPath(handle.storageObjectName());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private List<Location> listFiles(TrinoFileSystemFactory fileSystemFactory, QueryId queryId)
    {
        ImmutableList.Builder<Location> files = ImmutableList.builder();

        try {
            FileIterator iterator = createFileSystem(fileSystemFactory).listFiles(LOCATION);
            while (iterator.hasNext()) {
                FileEntry entry = iterator.next();
                if (entry.location().fileName().endsWith(queryId.toString())) {
                    files.add(entry.location());
                }
            }
            return files.build();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private TrinoFileSystem createFileSystem(TrinoFileSystemFactory fileSystemFactory)
    {
        return fileSystemFactory.create(ConnectorIdentity.ofUser("ignored"));
    }

    private FileSystemSpoolingConfig getSpoolingConfig()
    {
        return new FileSystemSpoolingConfig()
                .setS3Enabled(true)
                .setLocation(LOCATION.toString());
    }
}
