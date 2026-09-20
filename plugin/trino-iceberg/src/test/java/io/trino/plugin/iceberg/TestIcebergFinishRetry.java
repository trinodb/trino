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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import io.trino.execution.QueryInfo;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.local.LocalFileSystem;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * Task retries re-run the finish step of a write after a failure inside the connector's commit.
 */
@TestInstance(PER_CLASS)
@Execution(SAME_THREAD) // the failure injection state is shared by all tests
final class TestIcebergFinishRetry
        extends AbstractTestQueryFramework
{
    private static final String FLAKY_SCHEME = "flaky";
    private static final String STATISTICS_FILE = ".stats";
    private static final String DELETION_VECTOR_FILE = ".puffin";

    // Suffix of the next output file whose write fails, cleared once it has failed
    private final AtomicReference<String> fileSuffixToFail = new AtomicReference<>();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Path baseDataDir = createTempDirectory("iceberg_finish_retry");
        Path localFileSystemRoot = baseDataDir.resolve("iceberg_data");
        Files.createDirectories(localFileSystemRoot);
        Path exchangeDirectory = createTempDirectory("iceberg_finish_retry_exchange");

        TrinoFileSystemFactory flakyFileSystemFactory = _ -> new FlakyFileSystem(new LocalFileSystem(localFileSystemRoot));
        return IcebergQueryRunner.builder()
                .setBaseDataDir(Optional.of(baseDataDir))
                .setExtraProperties(ImmutableMap.<String, String>builder()
                        .put("retry-policy", "TASK")
                        .put("retry-initial-delay", "0s")
                        .put("fault-tolerant-execution-task-memory", "1GB")
                        .buildOrThrow())
                .setAdditionalOverrideModule(binder -> newMapBinder(binder, String.class, TrinoFileSystemFactory.class)
                        .addBinding(FLAKY_SCHEME)
                        .toInstance(flakyFileSystemFactory))
                .withExchange("filesystem", ImmutableMap.of("exchange.base-directories", exchangeDirectory.toString()))
                .build();
    }

    @Test
    void testInsertRetriedAfterStatisticsWriteFailure()
    {
        String tableName = createFlakyTable();
        try {
            assertRetried("INSERT INTO " + tableName + " VALUES 1, 2, 3", STATISTICS_FILE);

            assertThat(query("SELECT x FROM " + tableName)).matches("VALUES 1, 2, 3");
            assertThat(computeScalar("SELECT count(*) FROM \"" + tableName + "$snapshots\"")).isEqualTo(2L);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testCreateTableAsSelectRetriedAfterStatisticsWriteFailure()
    {
        String tableName = "test_finish_retry_" + randomNameSuffix();
        try {
            assertRetried("CREATE TABLE " + tableName + " WITH (" + flakyLocation(tableName) + ") AS SELECT * FROM (VALUES 1, 2, 3) t(x)", STATISTICS_FILE);

            assertThat(query("SELECT x FROM " + tableName)).matches("VALUES 1, 2, 3");
            assertThat(computeScalar("SELECT count(*) FROM \"" + tableName + "$snapshots\"")).isEqualTo(1L);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    void testCreateOrReplaceTableAsSelectRetriedAfterStatisticsWriteFailure()
    {
        String tableName = createFlakyTable();
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES 7, 8, 9", 3);

            assertRetried("CREATE OR REPLACE TABLE " + tableName + " WITH (" + flakyLocation(tableName) + ") AS SELECT * FROM (VALUES 1, 2, 3) t(x)", STATISTICS_FILE);

            assertThat(query("SELECT x FROM " + tableName)).matches("VALUES 1, 2, 3");
            assertThat(computeScalar("SELECT count(*) FROM \"" + tableName + "$snapshots\"")).isEqualTo(3L);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testOptimizeRetriedAfterStatisticsWriteFailure()
    {
        String tableName = createFlakyTable();
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES 1, 2, 3", 3);
            assertUpdate("INSERT INTO " + tableName + " VALUES 4, 5, 6", 3);
            assertUpdate("ANALYZE " + tableName);
            Set<String> filesBeforeOptimize = dataFiles(tableName);
            assertThat(filesBeforeOptimize).hasSize(2);

            assertRetried("ALTER TABLE " + tableName + " EXECUTE optimize", STATISTICS_FILE);

            assertThat(query("SELECT x FROM " + tableName)).matches("VALUES 1, 2, 3, 4, 5, 6");
            assertThat(dataFiles(tableName)).doesNotContainAnyElementsOf(filesBeforeOptimize);
            assertThat(computeScalar("SELECT count(*) FROM \"" + tableName + "$snapshots\"")).isEqualTo(4L);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testAnalyzeRetriedAfterStatisticsWriteFailure()
    {
        String tableName = createFlakyTable();
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES 1, 2, 3", 3);

            assertRetried("ANALYZE " + tableName, STATISTICS_FILE);

            assertThat(query("SHOW STATS FOR " + tableName))
                    .result()
                    .projected("column_name", "distinct_values_count")
                    .skippingTypesCheck()
                    .matches("VALUES ('x', 3e0), (null, null)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testUpdateRetriedAfterDeletionVectorWriteFailure()
    {
        String tableName = createFlakyTable(3);
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES 1, 2, 3", 3);

            assertRetried("UPDATE " + tableName + " SET x = x + 10 WHERE x = 2", DELETION_VECTOR_FILE);

            assertThat(query("SELECT x FROM " + tableName)).matches("VALUES 1, 12, 3");
            assertThat(computeScalar("SELECT count(*) FROM \"" + tableName + "$snapshots\"")).isEqualTo(3L);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    private String createFlakyTable()
    {
        return createFlakyTable(2);
    }

    // All files of the table go through the flaky file system
    private String createFlakyTable(int formatVersion)
    {
        String tableName = "test_finish_retry_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (x integer) WITH (format_version = " + formatVersion + ", " + flakyLocation(tableName) + ")");
        return tableName;
    }

    private static String flakyLocation(String tableName)
    {
        return "location = '" + FLAKY_SCHEME + ":///finish_retry/" + tableName + "'";
    }

    private Set<String> dataFiles(String tableName)
    {
        return computeActual("SELECT file_path FROM \"" + tableName + "$files\"").getOnlyColumnAsSet().stream()
                .map(String.class::cast)
                .collect(toImmutableSet());
    }

    // Fails the first write of a file with the given suffix and expects the query to recover through a task retry
    private void assertRetried(String sql, String fileSuffix)
    {
        fileSuffixToFail.set(fileSuffix);
        MaterializedResultWithPlan result = getDistributedQueryRunner().executeWithPlan(getSession(), sql);
        QueryInfo queryInfo = getDistributedQueryRunner().getCoordinator().getFullQueryInfo(result.queryId());
        assertThat(fileSuffixToFail.get()).isNull();
        assertThat(queryInfo.getQueryStats().getFailedTasks()).isEqualTo(1);
    }

    private final class FlakyFileSystem
            implements TrinoFileSystem
    {
        private final TrinoFileSystem delegate;

        private FlakyFileSystem(TrinoFileSystem delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public TrinoInputFile newInputFile(Location location)
        {
            return new RelocatedInputFile(location, delegate.newInputFile(toLocal(location)));
        }

        @Override
        public TrinoInputFile newInputFile(Location location, long length)
        {
            return new RelocatedInputFile(location, delegate.newInputFile(toLocal(location), length));
        }

        @Override
        public TrinoInputFile newInputFile(Location location, long length, Instant lastModified)
        {
            return new RelocatedInputFile(location, delegate.newInputFile(toLocal(location), length, lastModified));
        }

        @Override
        public TrinoOutputFile newOutputFile(Location location)
        {
            String suffix = fileSuffixToFail.get();
            if (suffix != null && location.path().endsWith(suffix) && fileSuffixToFail.compareAndSet(suffix, null)) {
                return new FailingOutputFile(location);
            }
            return delegate.newOutputFile(toLocal(location));
        }

        @Override
        public void deleteFile(Location location)
                throws IOException
        {
            delegate.deleteFile(toLocal(location));
        }

        @Override
        public void deleteDirectory(Location location)
                throws IOException
        {
            delegate.deleteDirectory(toLocal(location));
        }

        @Override
        public void renameFile(Location source, Location target)
                throws IOException
        {
            delegate.renameFile(toLocal(source), toLocal(target));
        }

        @Override
        public FileIterator listFiles(Location location)
                throws IOException
        {
            return delegate.listFiles(toLocal(location));
        }

        @Override
        public Optional<Boolean> directoryExists(Location location)
                throws IOException
        {
            return delegate.directoryExists(toLocal(location));
        }

        @Override
        public void createDirectory(Location location)
                throws IOException
        {
            delegate.createDirectory(toLocal(location));
        }

        @Override
        public void renameDirectory(Location source, Location target)
                throws IOException
        {
            delegate.renameDirectory(toLocal(source), toLocal(target));
        }

        @Override
        public Set<Location> listDirectories(Location location)
                throws IOException
        {
            return delegate.listDirectories(toLocal(location));
        }

        @Override
        public Optional<Location> createTemporaryDirectory(Location targetPath, String temporaryPrefix, String relativePrefix)
                throws IOException
        {
            return delegate.createTemporaryDirectory(toLocal(targetPath), temporaryPrefix, relativePrefix);
        }

        @Override
        public void deleteFiles(Collection<Location> locations)
                throws IOException
        {
            for (Location location : locations) {
                deleteFile(location);
            }
        }

        private static Location toLocal(Location location)
        {
            if (location.scheme().equals(Optional.of(FLAKY_SCHEME))) {
                return Location.of("local://" + location.toString().substring((FLAKY_SCHEME + "://").length()));
            }
            return location;
        }
    }

    // Reports the flaky location so that metadata read through it keeps the location recorded in the catalog
    private record RelocatedInputFile(Location location, TrinoInputFile delegate)
            implements TrinoInputFile
    {
        @Override
        public TrinoInput newInput()
                throws IOException
        {
            return delegate.newInput();
        }

        @Override
        public TrinoInputStream newStream()
                throws IOException
        {
            return delegate.newStream();
        }

        @Override
        public long length()
                throws IOException
        {
            return delegate.length();
        }

        @Override
        public Instant lastModified()
                throws IOException
        {
            return delegate.lastModified();
        }

        @Override
        public boolean exists()
                throws IOException
        {
            return delegate.exists();
        }
    }

    private record FailingOutputFile(Location location)
            implements TrinoOutputFile
    {
        @Override
        public OutputStream create(AggregatedMemoryContext memoryContext)
                throws IOException
        {
            throw new IOException("Simulated failure writing " + location);
        }

        @Override
        public void createOrOverwrite(byte[] data)
                throws IOException
        {
            throw new IOException("Simulated failure writing " + location);
        }
    }
}
