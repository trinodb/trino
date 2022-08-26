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
package io.trino.plugin.hive;

import io.airlift.concurrent.MoreFutures;
import io.airlift.units.Duration;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.plugin.hive.s3.S3HiveQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.join;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHiveAnalyzeCorruptStatistics
        extends AbstractTestQueryFramework
{
    private HiveMinioDataLake hiveMinioDataLake;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        hiveMinioDataLake = closeAfterClass(new HiveMinioDataLake("test-analyze"));
        hiveMinioDataLake.start();

        return S3HiveQueryRunner.builder(hiveMinioDataLake)
                // Increase timeout to allow operations on a table having many columns
                .setThriftMetastoreTimeout(new Duration(5, MINUTES))
                .build();
    }

    // Repeat test with invocationCount for better test coverage, since the tested aspect is inherently non-deterministic.
    @Test(invocationCount = 3)
    public void testAnalyzeCorruptColumnStatisticsOnEmptyTable()
            throws Exception
    {
        String tableName = "test_analyze_corrupt_column_statistics_" + randomTableSuffix();

        // Concurrent ANALYZE statements generate duplicated rows in Thrift metastore's TAB_COL_STATS table when column statistics is empty
        prepareBrokenColumnStatisticsTable(tableName);

        // SHOW STATS should succeed even when the column statistics is broken
        assertQuerySucceeds("SHOW STATS FOR " + tableName);

        // ANALYZE and drop_stats are unsupported for tables having broken column statistics
        assertThatThrownBy(() -> query("ANALYZE " + tableName))
                .hasMessage(hiveMinioDataLake.getHiveHadoop().getHiveMetastoreEndpoint().toString()) // Thrift metastore doesn't throw helpful message
                .hasStackTraceContaining("ThriftHiveMetastore.setTableColumnStatistics");

        assertThatThrownBy(() -> query("CALL system.drop_stats('tpch', '" + tableName + "')"))
                .hasMessageContaining("The query returned more than one instance BUT either unique is set to true or only aggregates are to be returned, so should have returned one result maximum");

        assertUpdate("DROP TABLE " + tableName);
    }

    private void prepareBrokenColumnStatisticsTable(String tableName)
            throws InterruptedException
    {
        int columnNumber = 1000;
        List<String> columnNames = IntStream.rangeClosed(1, columnNumber).mapToObj(x -> "col_" + x + " int").collect(toImmutableList());
        List<String> columnValues = IntStream.rangeClosed(1, columnNumber).mapToObj(String::valueOf).collect(toImmutableList());

        assertUpdate("CREATE TABLE " + tableName + "(" + join(",", columnNames) + ")");
        assertUpdate("INSERT INTO " + tableName + " VALUES (" + join(",", columnValues) + ")", 1);
        assertUpdate("CALL system.drop_stats('tpch', '" + tableName + "')");

        int threads = 10;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        try {
            List<Future<Boolean>> futures = IntStream.range(0, threads)
                    .mapToObj(threadNumber -> executor.submit(() -> {
                        barrier.await(10, SECONDS);
                        try {
                            getQueryRunner().execute("ANALYZE " + tableName);
                            return true;
                        }
                        catch (Exception e) {
                            return false;
                        }
                    }))
                    .collect(toImmutableList());

            long succeeded = futures.stream()
                    .map(MoreFutures::getFutureValue)
                    .filter(success -> success)
                    .count();

            if (succeeded < 2) {
                throw new SkipException("Expect other invocations succeed");
            }
        }
        finally {
            executor.shutdownNow();
            executor.awaitTermination(10, SECONDS);
        }
    }
}
