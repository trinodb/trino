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
import io.trino.tempto.ProductTest;
import io.trino.tempto.assertions.QueryAssert;
import io.trino.tempto.query.QueryExecutionException;
import io.trino.tempto.query.QueryExecutor;
import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.HMS_ONLY;
import static io.trino.tests.product.TestGroups.ICEBERG;
import static io.trino.tests.product.TestGroups.STORAGE_FORMATS_DETAILED;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergInsert
        extends ProductTest
{
    /**
     * @see TestIcebergCreateTable#testCreateTable() See TestIcebergCreateTable for a non-concurrent INSERT test coverage.
     * @see TestIcebergSparkCompatibility#testTrinoSparkConcurrentInsert()
     */
    @Test(groups = {ICEBERG, STORAGE_FORMATS_DETAILED, HMS_ONLY}, timeOut = 60_000)
    public void testIcebergConcurrentInsert()
            throws Exception
    {
        int threads = 3;
        int insertsPerThread = 4;

        String tableName = "iceberg.default.test_insert_concurrent_" + randomNameSuffix();
        onTrino().executeQuery("CREATE TABLE " + tableName + "(a bigint)");

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            CyclicBarrier barrier = new CyclicBarrier(threads);
            QueryExecutor onTrino = onTrino();
            List<Long> allInserted = executor.invokeAll(
                    IntStream.range(0, threads)
                            .mapToObj(thread -> (Callable<List<Long>>) () -> {
                                List<Long> inserted = new ArrayList<>();
                                for (int i = 0; i < insertsPerThread; i++) {
                                    barrier.await(20, SECONDS);
                                    long value = i + (long) insertsPerThread * thread;
                                    try {
                                        onTrino.executeQuery("INSERT INTO " + tableName + " VALUES " + value);
                                    }
                                    catch (QueryExecutionException queryExecutionException) {
                                        // failed to insert
                                        continue;
                                    }
                                    inserted.add(value);
                                }
                                return inserted;
                            })
                            .collect(toImmutableList())).stream()
                    .map(MoreFutures::getDone)
                    .flatMap(List::stream)
                    .collect(toImmutableList());

            // At least one INSERT per round should succeed
            Assertions.assertThat(allInserted).hasSizeBetween(insertsPerThread, threads * insertsPerThread);

            assertThat(onTrino().executeQuery("SELECT * FROM " + tableName))
                    .containsOnly(allInserted.stream()
                            .map(QueryAssert.Row::row)
                            .toArray(QueryAssert.Row[]::new));

            onTrino().executeQuery("DROP TABLE " + tableName);
        }
        finally {
            executor.shutdownNow();
        }
    }
}
