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
import io.airlift.concurrent.MoreFutures;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.testing.QueryAssertions.getTrinoExceptionCause;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestDeltaLakeLocalConcurrentCreateTableTest
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Path catalogDir = Files.createTempDirectory("catalog-dir");
        closeAfterClass(() -> deleteRecursively(catalogDir, ALLOW_INSECURE));

        return DeltaLakeQueryRunner.builder()
                .addDeltaProperty("delta.unique-table-location", "false")
                .addDeltaProperty("fs.hadoop.enabled", "true")
                .addDeltaProperty("hive.metastore", "file")
                .addDeltaProperty("hive.metastore.catalog.dir", catalogDir.toUri().toString())
                .addDeltaProperty("hive.metastore.disable-location-checks", "true")
                .build();
    }

    @Test
    public void testConcurrentCreateTableAsSelect()
            throws Exception
    {
        testConcurrentCreateTableAsSelect(false);
        testConcurrentCreateTableAsSelect(true);
    }

    private void testConcurrentCreateTableAsSelect(boolean partitioned)
            throws InterruptedException
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);

        try {
            // Execute concurrent CTAS operations
            executor.invokeAll(ImmutableList.<Callable<Void>>builder()
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("CREATE TABLE  test_ctas_1"
                                        + (partitioned ? " WITH (partitioned_by = ARRAY['part'])" : "") + " AS SELECT 1 as a, 10 as part");
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("CREATE TABLE  test_ctas_2"
                                        + (partitioned ? " WITH (partitioned_by = ARRAY['part'])" : "") + " AS SELECT 11 as a, 20 as part");
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute("CREATE TABLE  test_ctas_3"
                                        + (partitioned ? " WITH (partitioned_by = ARRAY['part'])" : "") + " AS SELECT 21 as a, 30 as part");
                                return null;
                            })
                            .build())
                    .forEach(MoreFutures::getDone);

            // Verify each table was created with correct data
            assertThat(query("SELECT * FROM test_ctas_1")).matches("VALUES (1, 10)");
            assertThat(query("SELECT * FROM test_ctas_2")).matches("VALUES (11, 20)");
            assertThat(query("SELECT * FROM test_ctas_3")).matches("VALUES (21, 30)");

            // Verify table histories
            assertQuery(
                    "SELECT version, operation, isolation_level, is_blind_append FROM \"test_ctas_1$history\"",
                    "VALUES (0, 'CREATE TABLE AS SELECT', 'WriteSerializable', true)");
            assertQuery(
                    "SELECT version, operation, isolation_level, is_blind_append FROM \"test_ctas_2$history\"",
                    "VALUES (0, 'CREATE TABLE AS SELECT', 'WriteSerializable', true)");
            assertQuery(
                    "SELECT version, operation, isolation_level, is_blind_append FROM \"test_ctas_3$history\"",
                    "VALUES (0, 'CREATE TABLE AS SELECT', 'WriteSerializable', true)");
        }
        finally {
            // Clean up
            assertUpdate("DROP TABLE IF EXISTS test_ctas_1");
            assertUpdate("DROP TABLE IF EXISTS test_ctas_2");
            assertUpdate("DROP TABLE IF EXISTS test_ctas_3");
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @Test
    public void testConcurrentCreateTableAsSelectSameTable()
            throws Exception
    {
        testConcurrentCreateTableAsSelectSameTable(false);
        testConcurrentCreateTableAsSelectSameTable(true);
    }

    private void testConcurrentCreateTableAsSelectSameTable(boolean partitioned)
            throws Exception
    {
        int threads = 3;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        String tableName = "test_concurrent_ctas_" + randomNameSuffix();

        try {
            getQueryRunner().execute("create table stg_test as SELECT a, b, 20220101 as d FROM UNNEST(SEQUENCE(1, 9001), SEQUENCE(1, 9001)) AS t(a, b)");

            String selectString = " as ((select stg1.a as a, stg1.b as b, stg1.d as part from stg_test stg1, stg_test stg2 where stg1.d=stg2.d) " +
                    "union all (select stg1.a as a, stg1.b as b, stg1.d as part from stg_test stg1, stg_test stg2 where stg1.d=stg2.d))";

            // Execute concurrent CTAS operations
            executor.invokeAll(ImmutableList.<Callable<Void>>builder()
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                try {
                                    getQueryRunner().execute("CREATE TABLE " + tableName +
                                            (partitioned ? " WITH (partitioned_by = ARRAY['part'])" : "") + selectString);
                                }
                                catch (Exception e) {
                                    RuntimeException trinoException = getTrinoExceptionCause(e);
                                    assertThat(trinoException).hasMessageContaining("Table already exists");
                                }
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                try {
                                    getQueryRunner().execute("CREATE TABLE " + tableName +
                                            (partitioned ? " WITH (partitioned_by = ARRAY['part'])" : "") + selectString);
                                }
                                catch (Exception e) {
                                    RuntimeException trinoException = getTrinoExceptionCause(e);
                                    assertThat(trinoException).hasMessageContaining("Table already exists");
                                }
                                return null;
                            })
                            .add(() -> {
                                barrier.await(10, SECONDS);
                                try {
                                    getQueryRunner().execute("CREATE TABLE " + tableName +
                                            (partitioned ? " WITH (partitioned_by = ARRAY['part'])" : "") + selectString);
                                }
                                catch (Exception e) {
                                    RuntimeException trinoException = getTrinoExceptionCause(e);
                                    assertThat(trinoException).hasMessageContaining("Table already exists");
                                }
                                return null;
                            })
                            .build())
                    .forEach(MoreFutures::getDone);

            // Verify table exists and has one row
            assertThat((long) computeScalar("SELECT count(*) FROM " + tableName)).isEqualTo(162036002L);

            // Verify table history shows single creation
            assertQuery(
                    "SELECT version, operation, isolation_level, is_blind_append FROM \"" + tableName + "$history\"",
                    "VALUES (0, 'CREATE TABLE AS SELECT', 'WriteSerializable', true)");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
            assertUpdate("DROP TABLE IF EXISTS stg_test");
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }
}
