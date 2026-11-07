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

import io.airlift.concurrent.MoreFutures;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

// Commits to the same table from one coordinator wait their turn, so none needs to retry
@TestInstance(PER_CLASS)
final class TestIcebergCommitLock
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .addIcebergProperty("iceberg.commit-lock.enabled", "true")
                .build();
    }

    @RepeatedTest(3)
    void testConcurrentWritesCommitWithoutRetries()
            throws Exception
    {
        String tableName = "test_concurrent_writes_" + randomNameSuffix();
        // Without retries, any conflicting commit fails its query
        assertUpdate("CREATE TABLE " + tableName + " (a, part) WITH (partitioning = ARRAY['part'], max_commit_retry = 0) AS VALUES (0, 0), (1, 1)", 2);
        List<String> writes = List.of(
                "INSERT INTO " + tableName + " VALUES (0, 10)",
                "INSERT INTO " + tableName + " VALUES (1, 11)",
                // Deleting a whole partition commits without a transaction
                "DELETE FROM " + tableName + " WHERE part = 0",
                "DELETE FROM " + tableName + " WHERE part = 1");
        CyclicBarrier barrier = new CyclicBarrier(writes.size());
        ExecutorService executor = newFixedThreadPool(writes.size());
        try {
            executor.invokeAll(writes.stream()
                            .map(sql -> (Callable<Void>) () -> {
                                barrier.await(10, SECONDS);
                                getQueryRunner().execute(sql);
                                return null;
                            })
                            .toList())
                    .forEach(MoreFutures::getDone);

            assertThat(query("SELECT * FROM " + tableName))
                    .matches("VALUES (0, 10), (1, 11)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }
}
