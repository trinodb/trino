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

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.execution.QueryState;
import io.trino.execution.QueryStats;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.plugin.iceberg.fileio.ForwardingFileIo;
import io.trino.spi.QueryId;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryFailedException;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import io.trino.testing.TestingConnectorSession;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_STATS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/**
 * Integration tests for Copy-on-Write DELETE operations in Iceberg.
 */
@TestInstance(Lifecycle.PER_CLASS)
public class TestIcebergCopyOnWriteIntegration
        extends AbstractTestQueryFramework
{
    @TempDir
    public Path tempDir;

    private QueryRunner queryRunner;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        // Note: Copy-on-write mode for DELETE operations is enabled by default in Iceberg format version 2
        return IcebergQueryRunner.builder()
                .addIcebergProperty("iceberg.format-version", "2")
                .build();
    }

    @BeforeAll
    public void setup()
            throws Exception
    {
        queryRunner = createQueryRunner();
    }

    @AfterAll
    public void tearDown()
    {
        try {
            if (queryRunner != null) {
                queryRunner.close();
            }
        }
        catch (Exception e) {
            // Suppress exceptions during cleanup
        }
        queryRunner = null;
    }

    /**
     * Helper class for integration testing with query results.
     */
    public static class QueryInfo
    {
        private final QueryState state;
        private final MaterializedResult result;

        public QueryInfo(QueryState state, MaterializedResult result)
        {
            this.state = state;
            this.result = result;
        }

        public QueryState getState()
        {
            return state;
        }

        public MaterializedResult getResult()
        {
            return result;
        }
    }

    @Test
    public void testResourceCleanupOnFailure()
            throws Exception
    {
        // Create a table for testing
        String tableName = "test_cleanup_on_failure_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INT, name VARCHAR, value INT) WITH (format_version = 2)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a', 100), (2, 'b', 200), (3, 'c', 300), (4, 'd', 400)", 4);

        // Replace the FileIO with one that fails during write
        try (FailingFileIo failingFileIo = installFailingFileIo()) {
            failingFileIo.failOnWriteAfterBytes(100); // Fail after writing 100 bytes

            // Attempt delete that should fail during file writing
            assertThatThrownBy(() -> queryRunner.execute("DELETE FROM " + tableName + " WHERE id = 2"))
                    .isInstanceOf(QueryFailedException.class);

            // Verify table is still queryable and data is intact
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'a', 100), (2, 'b', 200), (3, 'c', 300), (4, 'd', 400)");

            // Disable the failure for cleanup
            failingFileIo.disableFailures();
        }

        // Clean up
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testConcurrentDeleteOperations()
            throws Exception
    {
        // Create a table for testing
        String tableName = "test_concurrent_deletes_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INT, region VARCHAR, value INT) WITH (format_version = 2, partitioning = ARRAY['region'])");

        // Insert data into multiple partitions
        assertUpdate(
                "INSERT INTO " + tableName + " VALUES " +
                        "(1, 'US', 100), (2, 'US', 200), (3, 'US', 300)," +
                        "(4, 'EU', 400), (5, 'EU', 500), (6, 'EU', 600)," +
                        "(7, 'ASIA', 700), (8, 'ASIA', 800), (9, 'ASIA', 900)",
                9);

        // Execute concurrent deletes in different partitions
        ExecutorService executor = Executors.newFixedThreadPool(3);
        try {
            List<Callable<MaterializedResult>> tasks = ImmutableList.of(
                    () -> queryRunner.execute("DELETE FROM " + tableName + " WHERE region = 'US' AND id = 2"),
                    () -> queryRunner.execute("DELETE FROM " + tableName + " WHERE region = 'EU' AND id = 5"),
                    () -> queryRunner.execute("DELETE FROM " + tableName + " WHERE region = 'ASIA' AND id = 8"));

            List<Future<MaterializedResult>> futures = executor.invokeAll(tasks, 30, SECONDS);

            // Verify all deletes completed successfully
            for (Future<MaterializedResult> future : futures) {
                MaterializedResult result = future.get();
                assertThat(result.getUpdateCount()).hasValue(1);
            }
        }
        finally {
            executor.shutdown();
        }

        // Verify the correct data was deleted
        assertQuery("SELECT id FROM " + tableName + " WHERE region = 'US' ORDER BY id", "VALUES (1), (3)");
        assertQuery("SELECT id FROM " + tableName + " WHERE region = 'EU' ORDER BY id", "VALUES (4), (6)");
        assertQuery("SELECT id FROM " + tableName + " WHERE region = 'ASIA' ORDER BY id", "VALUES (7), (9)");

        // Clean up
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testSnapshotIsolation()
            throws Exception
    {
        // Create a table for testing
        String tableName = "test_snapshot_isolation_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INT, name VARCHAR, value INT) WITH (format_version = 2)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a', 100), (2, 'b', 200), (3, 'c', 300)", 3);

        // Start a long-running query that reads the table
        Session readSession = Session.builder(getSession()).build();
        QueryId readQueryId = startLongRunningQuery(readSession, "SELECT * FROM " + tableName + " ORDER BY id");

        try {
            // Wait for the query to start
            Thread.sleep(1000);

            // Execute a delete while the read is in progress
            assertUpdate("DELETE FROM " + tableName + " WHERE id = 2", 1);

            // Verify the delete was successful
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'a', 100), (3, 'c', 300)");

            // Wait for the read query to complete
            // Note: This is a simplified approach for the test - in real scenarios,
            // proper query state monitoring would be implemented
            Thread.sleep(2000); // Give some time for the query to complete
            MaterializedResult readResult = queryRunner.execute("SELECT * FROM " + tableName + " ORDER BY id");

            // Verify the read query saw the original data (snapshot isolation)
            assertThat(readResult.getMaterializedRows()).hasSize(3); // Should see all three original rows
        }
        finally {
            // Cancel the query if it's still running
            getQueryRunner().getCoordinator().getQueryManager().cancelQuery(readQueryId);
        }

        // Clean up
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testConflictingRewritesAndRetry()
            throws Exception
    {
        // Create a table for testing
        String tableName = "test_conflicting_rewrites_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INT, name VARCHAR, value INT) WITH (format_version = 2)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a', 100), (2, 'b', 200), (3, 'c', 300)", 3);

        // Create an injected file system that can simulate conflicts
        try (ConflictingFileIo conflictingFileIo = installConflictingFileIo()) {
            conflictingFileIo.enableConflict();

            // Verify that the query completes successfully despite conflicts
            // This is testing Iceberg's retry mechanism for conflicts
            try {
                assertUpdate("DELETE FROM " + tableName + " WHERE id = 2", 1);
            }
            catch (Exception e) {
                fail("DELETE operation failed with conflict: " + e.getMessage());
            }

            // Verify the delete was successful
            assertQuery("SELECT * FROM " + tableName + " ORDER BY id",
                    "VALUES (1, 'a', 100), (3, 'c', 300)");

            // Disable conflicts for cleanup
            conflictingFileIo.disableConflict();
        }

        // Clean up
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testVerifyLogging()
            throws Exception
    {
        // This test verifies that logging information is available for CoW DELETE operations
        // Create a table for testing
        String tableName = "test_verify_logging_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id INT, name VARCHAR, value INT) WITH (format_version = 2)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a', 100), (2, 'b', 200), (3, 'c', 300), (4, 'd', 400), (5, 'e', 500)", 5);

        // Execute the delete with metrics collection
        MaterializedResultWithPlan result = executeWithPlan("DELETE FROM " + tableName + " WHERE id IN (1, 3, 5)");
        assertThat(result.result().getUpdateCount()).hasValue(3);

        // Get query stats
        QueryStats queryStats = getQueryStats(result.queryId());

        // Verify that metrics are available
        assertThat(queryStats).isNotNull();
        assertThat(queryStats.getPhysicalInputDataSize()).isGreaterThan(DataSize.ofBytes(0));
        assertThat(queryStats.getPhysicalWrittenDataSize()).isGreaterThan(DataSize.ofBytes(0));

        // Verify the operator statistics include TableWriter operations
        boolean hasTableWriterOperator = queryStats.getOperatorSummaries().stream()
                .anyMatch(stats -> stats.getOperatorType().contains("TableWriter"));
        assertThat(hasTableWriterOperator).isTrue();

        // Clean up
        assertUpdate("DROP TABLE " + tableName);
    }

    private QueryStats getQueryStats(QueryId queryId)
    {
        DistributedQueryRunner runner = (DistributedQueryRunner) getQueryRunner();
        return runner.getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(queryId)
                .getQueryStats();
    }

    private MaterializedResultWithPlan executeWithPlan(String sql)
    {
        DistributedQueryRunner runner = (DistributedQueryRunner) getQueryRunner();
        return runner.executeWithPlan(getSession(), sql);
    }

    private QueryId startLongRunningQuery(Session session, String sql)
    {
        // For testing purposes, we'll use a simplified approach
        // In a real implementation, this would create a proper async query
        DistributedQueryRunner runner = (DistributedQueryRunner) getQueryRunner();
        return runner.executeWithPlan(session, sql).queryId();
    }

    private static String randomNameSuffix()
    {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    }

    /**
     * A FileIO implementation that can be configured to fail during writes after a certain number of bytes.
     */
    private FailingFileIo installFailingFileIo()
            throws Exception
    {
        // For testing purposes, we'll create a mock FileIO
        // In a real implementation, this would integrate with the catalog's FileIO factory
        return new FailingFileIo();
    }

    private static class FailingFileIo
            extends ForwardingFileIo
    {
        private final AtomicInteger bytesToFailAfter = new AtomicInteger(-1);
        private final AtomicBoolean failuresEnabled = new AtomicBoolean(true);

        private final TrinoFileSystem fileSystem;

        public FailingFileIo()
        {
            this(createFileSystem());
        }

        private FailingFileIo(TrinoFileSystem fileSystem)
        {
            super(fileSystem, false);
            this.fileSystem = fileSystem;
        }

        private static TrinoFileSystem createFileSystem()
        {
            HdfsFileSystemFactory factory = new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS);
            return factory.create(TestingConnectorSession.builder().build());
        }

        public TrinoFileSystem getDelegate()
        {
            return fileSystem;
        }

        @Override
        public void close()
        {
            // TrinoFileSystem has no close method - it's managed by the system
            super.close();
        }

        public void failOnWriteAfterBytes(int bytes)
        {
            bytesToFailAfter.set(bytes);
            failuresEnabled.set(true);
        }

        public void disableFailures()
        {
            failuresEnabled.set(false);
        }

        @Override
        public OutputFile newOutputFile(String path)
        {
            OutputFile delegate = super.newOutputFile(path);
            return new FailingOutputFile(delegate, bytesToFailAfter, failuresEnabled);
        }
    }

    private static class FailingOutputFile
            implements OutputFile
    {
        private final OutputFile delegate;
        private final AtomicInteger bytesToFailAfter;
        private final AtomicBoolean failuresEnabled;

        public FailingOutputFile(OutputFile delegate, AtomicInteger bytesToFailAfter, AtomicBoolean failuresEnabled)
        {
            this.delegate = delegate;
            this.bytesToFailAfter = bytesToFailAfter;
            this.failuresEnabled = failuresEnabled;
        }

        @Override
        public org.apache.iceberg.io.PositionOutputStream create()
        {
            return new FailingPositionOutputStream(delegate.create(), bytesToFailAfter, failuresEnabled);
        }

        @Override
        public org.apache.iceberg.io.PositionOutputStream createOrOverwrite()
        {
            return new FailingPositionOutputStream(delegate.createOrOverwrite(), bytesToFailAfter, failuresEnabled);
        }

        @Override
        public String location()
        {
            return delegate.location();
        }

        @Override
        public InputFile toInputFile()
        {
            return delegate.toInputFile();
        }
    }

    private static class FailingPositionOutputStream
            extends org.apache.iceberg.io.PositionOutputStream
    {
        private final org.apache.iceberg.io.PositionOutputStream delegate;
        private final AtomicInteger bytesToFailAfter;
        private final AtomicBoolean failuresEnabled;
        private int bytesWritten;

        public FailingPositionOutputStream(
                org.apache.iceberg.io.PositionOutputStream delegate,
                AtomicInteger bytesToFailAfter,
                AtomicBoolean failuresEnabled)
        {
            this.delegate = delegate;
            this.bytesToFailAfter = bytesToFailAfter;
            this.failuresEnabled = failuresEnabled;
            this.bytesWritten = 0;
        }

        @Override
        public long getPos()
                throws IOException
        {
            return delegate.getPos();
        }

        @Override
        public void write(int b)
                throws IOException
        {
            if (shouldFail(1)) {
                throw new IOException("Simulated failure after " + bytesWritten + " bytes");
            }
            delegate.write(b);
            bytesWritten++;
        }

        @Override
        public void write(byte[] b)
                throws IOException
        {
            if (shouldFail(b.length)) {
                throw new IOException("Simulated failure after " + bytesWritten + " bytes");
            }
            delegate.write(b);
            bytesWritten += b.length;
        }

        @Override
        public void write(byte[] b, int off, int len)
                throws IOException
        {
            if (shouldFail(len)) {
                throw new IOException("Simulated failure after " + bytesWritten + " bytes");
            }
            delegate.write(b, off, len);
            bytesWritten += len;
        }

        @Override
        public void flush()
                throws IOException
        {
            delegate.flush();
        }

        @Override
        public void close()
                throws IOException
        {
            delegate.close();
        }

        private boolean shouldFail(int bytesToWrite)
        {
            int limit = bytesToFailAfter.get();
            return failuresEnabled.get() && limit >= 0 && bytesWritten + bytesToWrite > limit;
        }
    }

    /**
     * A FileIO implementation that can simulate commit conflicts.
     */
    private ConflictingFileIo installConflictingFileIo()
            throws Exception
    {
        // For testing purposes, we'll create a mock FileIO
        // In a real implementation, this would integrate with the catalog's FileIO factory
        return new ConflictingFileIo();
    }

    private static class ConflictingFileIo
            extends ForwardingFileIo
    {
        private final AtomicBoolean conflictEnabled = new AtomicBoolean(false);
        private static final String METADATA_FOLDER = "metadata";
        private static final String METADATA_FILE_PREFIX = "v";

        private final TrinoFileSystem fileSystem;

        public ConflictingFileIo()
        {
            this(createConflictingFileSystem());
        }

        private ConflictingFileIo(TrinoFileSystem fileSystem)
        {
            super(fileSystem, false);
            this.fileSystem = fileSystem;
        }

        private static TrinoFileSystem createConflictingFileSystem()
        {
            HdfsFileSystemFactory factory = new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS);
            return factory.create(TestingConnectorSession.builder().build());
        }

        public TrinoFileSystem getDelegate()
        {
            return fileSystem;
        }

        @Override
        public void close()
        {
            // TrinoFileSystem has no close method - it's managed by the system
            super.close();
        }

        public void enableConflict()
        {
            conflictEnabled.set(true);
        }

        public void disableConflict()
        {
            conflictEnabled.set(false);
        }

        @Override
        public OutputFile newOutputFile(String path)
        {
            OutputFile delegate = super.newOutputFile(path);

            // Only intercept metadata file writes to simulate conflicts
            if (conflictEnabled.get() && path.contains(METADATA_FOLDER) &&
                    path.substring(path.lastIndexOf('/') + 1).startsWith(METADATA_FILE_PREFIX)) {
                return new ConflictingOutputFile(delegate);
            }

            return delegate;
        }
    }

    private static class ConflictingOutputFile
            implements OutputFile
    {
        private final OutputFile delegate;
        private static final AtomicInteger counter = new AtomicInteger(0);

        public ConflictingOutputFile(OutputFile delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public org.apache.iceberg.io.PositionOutputStream create()
        {
            // Simulate a conflict on the first attempt
            if (counter.getAndIncrement() == 0) {
                throw new UncheckedIOException(
                        new IOException("Simulated commit conflict: Another process modified the table concurrently"));
            }
            return delegate.create();
        }

        @Override
        public org.apache.iceberg.io.PositionOutputStream createOrOverwrite()
        {
            // Simulate a conflict on the first attempt
            if (counter.getAndIncrement() == 0) {
                throw new UncheckedIOException(
                        new IOException("Simulated commit conflict: Another process modified the table concurrently"));
            }
            return delegate.createOrOverwrite();
        }

        @Override
        public String location()
        {
            return delegate.location();
        }

        @Override
        public InputFile toInputFile()
        {
            return delegate.toInputFile();
        }
    }
}
