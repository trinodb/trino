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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.ClassPath;
import io.airlift.concurrent.MoreFutures;
import io.trino.Session;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.plugin.deltalake.metastore.TestingDeltaLakeMetastoreModule;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static io.trino.plugin.deltalake.DeltaLakeConnectorFactory.CONNECTOR_NAME;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.plugin.deltalake.TestingDeltaLakeUtils.getConnectorService;
import static io.trino.plugin.hive.metastore.file.TestingFileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.testing.QueryAssertions.getTrinoExceptionCause;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.regex.Matcher.quoteReplacement;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestDeltaLakeLocalConcurrentWritesTest
        extends AbstractTestQueryFramework
{
    protected static final String SCHEMA = "test_delta_concurrent_writes_" + randomNameSuffix();

    private Path dataDirectory;
    private HiveMetastore metastore;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(DELTA_CATALOG)
                .setSchema(SCHEMA)
                .build();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).build();

        this.dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("delta_lake_data");
        this.metastore = createTestingFileHiveMetastore(dataDirectory.toFile());

        queryRunner.installPlugin(new TestingDeltaLakePlugin(dataDirectory, Optional.of(new TestingDeltaLakeMetastoreModule(metastore))));

        Map<String, String> connectorProperties = ImmutableMap.<String, String>builder()
                .put("delta.unique-table-location", "true")
                .put("delta.register-table-procedure.enabled", "true")
                .buildOrThrow();

        queryRunner.createCatalog(DELTA_CATALOG, CONNECTOR_NAME, connectorProperties);
        queryRunner.execute("CREATE SCHEMA " + SCHEMA);

        return queryRunner;
    }

    @AfterAll
    public void tearDown()
            throws IOException
    {
        if (metastore != null) {
            metastore.dropDatabase(SCHEMA, false);
            deleteRecursively(dataDirectory, ALLOW_INSECURE);
        }
    }

    // Copied from BaseDeltaLakeConnectorSmokeTest
    @Test
    public void testConcurrentInsertsReconciliationForBlindInserts()
            throws Exception
    {
        testConcurrentInsertsReconciliationForBlindInserts(false);
        testConcurrentInsertsReconciliationForBlindInserts(true);
    }

    private void testConcurrentInsertsReconciliationForBlindInserts(boolean partitioned)
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_inserts_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a INT, part INT) " +
                (partitioned ? " WITH (partitioned_by = ARRAY['part'])" : ""));

        try {
            // insert data concurrently
            executor.invokeAll(ImmutableList.<Callable<Void>>builder()
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("INSERT INTO " + tableName + " VALUES (1, 10)");
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("INSERT INTO " + tableName + " VALUES (11, 20)");
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("INSERT INTO " + tableName + " VALUES (21, 30)");
                                return null;
                            })
                            .build())
                    .forEach(MoreFutures::getDone);

            assertThat(query("SELECT * FROM " + tableName)).matches("VALUES (1, 10), (11, 20), (21, 30)");
            assertQuery("SELECT version, operation, isolation_level, read_version FROM \"" + tableName + "$history\"",
                    """
                            VALUES
                                (0, 'CREATE TABLE', 'WriteSerializable', 0),
                                (1, 'WRITE', 'WriteSerializable', 0),
                                (2, 'WRITE', 'WriteSerializable', 1),
                                (3, 'WRITE', 'WriteSerializable', 2)
                            """);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, SECONDS));
        }
    }

    @Test
    public void testConcurrentSerializableBlindInsertsReconciliationFailure()
            throws Exception
    {
        int threads = 5;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_serializable_blind_inserts_table_reconciliation" + randomNameSuffix();

        // TODO create the table through Trino when `isolation_level` table property can be set
        registerTableFromResources(tableName, "deltalake/serializable_partitioned_table", getQueryRunner());

        assertThat(query("SELECT * FROM " + tableName)).matches("VALUES (0, 10), (33, 40)");

        try {
            List<Future<Boolean>> futures = IntStream.range(0, threads)
                    .mapToObj(threadNumber -> executor.submit(() -> {
                        barrier.await(10, SECONDS);
                        try {
                            // Writing concurrently on the same partition even when doing blind inserts is not permitted
                            // in Serializable isolation level
                            getQueryRunner().execute("INSERT INTO " + tableName + " VALUES (1, 10)");
                            return true;
                        }
                        catch (Exception e) {
                            RuntimeException trinoException = getTrinoExceptionCause(e);
                            try {
                                assertThat(trinoException).hasMessage("Failed to write Delta Lake transaction log entry");
                            }
                            catch (Throwable verifyFailure) {
                                if (verifyFailure != e) {
                                    verifyFailure.addSuppressed(e);
                                }
                                throw verifyFailure;
                            }
                            return false;
                        }
                    }))
                    .collect(toImmutableList());

            long successfulInsertsCount = futures.stream()
                    .map(future -> tryGetFutureValue(future, 20, SECONDS).orElseThrow(() -> new RuntimeException("Wait timed out")))
                    .filter(success -> success)
                    .count();
            assertThat(successfulInsertsCount).isGreaterThanOrEqualTo(1);
            assertThat(successfulInsertsCount).isLessThan(threads);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, SECONDS));
        }
    }

    @Test
    public void testConcurrentInsertsReconciliationFailure()
            throws Exception
    {
        testConcurrentInsertsReconciliationFailure(false);
        testConcurrentInsertsReconciliationFailure(true);
    }

    private void testConcurrentInsertsReconciliationFailure(boolean partitioned)
            throws Exception
    {
        int threads = 5;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_inserts_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " (a, part)" +
                (partitioned ? " WITH (partitioned_by = ARRAY['part'])" : "") + " AS VALUES (1, 10)", 1);

        try {
            List<Future<Boolean>> futures = IntStream.range(0, threads)
                    .mapToObj(threadNumber -> executor.submit(() -> {
                        barrier.await(10, SECONDS);
                        try {
                            // Writing concurrently on the same partition of the table as from which we are reading from should fail
                            getQueryRunner().execute("INSERT INTO " + tableName + " SELECT * FROM " + tableName + " WHERE part = 10");
                            return true;
                        }
                        catch (Exception e) {
                            RuntimeException trinoException = getTrinoExceptionCause(e);
                            try {
                                assertThat(trinoException).hasMessage("Failed to write Delta Lake transaction log entry");
                            }
                            catch (Throwable verifyFailure) {
                                if (verifyFailure != e) {
                                    verifyFailure.addSuppressed(e);
                                }
                                throw verifyFailure;
                            }
                            return false;
                        }
                    }))
                    .collect(toImmutableList());

            long successfulInsertsCount = futures.stream()
                    .map(future -> tryGetFutureValue(future, 20, SECONDS).orElseThrow(() -> new RuntimeException("Wait timed out")))
                    .filter(success -> success)
                    .count();
            assertThat(successfulInsertsCount).isGreaterThanOrEqualTo(1);
            assertThat(successfulInsertsCount).isLessThan(threads);
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, SECONDS));
        }
    }

    protected void registerTableFromResources(String table, String resourcePath, QueryRunner queryRunner)
            throws IOException
    {
        TrinoFileSystem fileSystem = getConnectorService((DistributedQueryRunner) queryRunner, TrinoFileSystemFactory.class)
                .create(ConnectorIdentity.ofUser("test"));

        String tableLocation = "local:///" + SCHEMA + "/" + table;
        fileSystem.createDirectory(Location.of(tableLocation));

        try {
            List<ClassPath.ResourceInfo> resources = ClassPath.from(getClass().getClassLoader())
                    .getResources()
                    .stream()
                    .filter(resourceInfo -> resourceInfo.getResourceName().startsWith(resourcePath + "/"))
                    .collect(toImmutableList());
            for (ClassPath.ResourceInfo resourceInfo : resources) {
                String fileName = resourceInfo.getResourceName().replaceFirst("^" + Pattern.quote(resourcePath), quoteReplacement(tableLocation));
                byte[] bytes = resourceInfo.asByteSource().read();
                TrinoOutputFile trinoOutputFile = fileSystem.newOutputFile(Location.of(fileName));
                trinoOutputFile.createOrOverwrite(bytes);
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        queryRunner.execute(format("CALL system.register_table('%s', '%s', '%s')", SCHEMA, table, tableLocation));
    }
}
