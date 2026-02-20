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
package io.trino.tests.product.iceberg;

import io.airlift.concurrent.MoreFutures;
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.containers.environment.Row;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for concurrent insert operations on Iceberg tables.
 * <p>
 * Ported from the Tempto-based TestIcebergInsert.
 *
 * @see TestIcebergSparkCompatibility#testTrinoSparkConcurrentInsert()
 */
@ProductTest
@RequiresEnvironment(SparkIcebergEnvironment.class)
@TestGroup.Iceberg
class TestIcebergInsert
{
    @Test
    @Timeout(value = 60, unit = SECONDS)
    void testIcebergConcurrentInsert(SparkIcebergEnvironment env)
            throws Exception
    {
        int threads = 3;
        int insertsPerThread = 4;

        String tableName = "iceberg.default.test_insert_concurrent_" + randomNameSuffix();
        env.executeTrinoUpdate("CREATE TABLE " + tableName + "(a bigint)");

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            CyclicBarrier barrier = new CyclicBarrier(threads);
            List<Long> allInserted = executor.invokeAll(
                    IntStream.range(0, threads)
                            .mapToObj(thread -> (Callable<List<Long>>) () -> {
                                List<Long> inserted = new ArrayList<>();
                                for (int i = 0; i < insertsPerThread; i++) {
                                    barrier.await(20, SECONDS);
                                    long value = i + (long) insertsPerThread * thread;
                                    try (Connection conn = env.createTrinoConnection();
                                            Statement stmt = conn.createStatement()) {
                                        stmt.executeUpdate("INSERT INTO " + tableName + " VALUES " + value);
                                        inserted.add(value);
                                    }
                                    catch (SQLException e) {
                                        // failed to insert due to concurrent modification - this is expected behavior
                                    }
                                }
                                return inserted;
                            })
                            .collect(toImmutableList())).stream()
                    .map(MoreFutures::getDone)
                    .flatMap(List::stream)
                    .collect(toImmutableList());

            // At least one INSERT per round should succeed
            assertThat(allInserted)
                    .hasSizeBetween(insertsPerThread, threads * insertsPerThread);

            assertThat(env.executeTrino("SELECT * FROM " + tableName))
                    .containsOnly(allInserted.stream()
                            .map(v -> row(v))
                            .toArray(Row[]::new));

            env.executeTrinoUpdate("DROP TABLE " + tableName);
        }
        finally {
            executor.shutdownNow();
        }
    }
}
