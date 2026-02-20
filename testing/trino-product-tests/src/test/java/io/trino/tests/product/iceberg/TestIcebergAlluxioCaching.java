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

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.function.BooleanSupplier;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;

/**
 * Tests for Iceberg file system caching.
 * <p>
 * The test verifies that the first read populates the cache and subsequent reads are served from it.
 */
@ProductTest
@RequiresEnvironment(MultiNodeIcebergMinioCachingEnvironment.class)
@TestGroup.IcebergAlluxioCaching
class TestIcebergAlluxioCaching
{
    @Test
    void testReadFromCache(MultiNodeIcebergMinioCachingEnvironment env)
    {
        String bucketName = env.getBucketName();

        // Test with two different tables to verify caching across multiple tables
        testReadFromTable(env, bucketName, "table1");
        testReadFromTable(env, bucketName, "table2");
    }

    private void testReadFromTable(MultiNodeIcebergMinioCachingEnvironment env, String bucketName, String tableNameSuffix)
    {
        String schemaName = "test_caching";
        String tableName = "iceberg." + schemaName + ".test_cache_read" + tableNameSuffix;

        try {
            // Create test table with enough data to exercise caching
            createTestTable(env, bucketName, schemaName, tableName);

            CacheStats beforeCacheStats = getCacheStats(env);

            // First query - should fetch from external storage
            assertThat(env.executeTrino("SELECT * FROM " + tableName))
                    .satisfies(result -> {
                        if (result.getRowsCount() == 0) {
                            throw new AssertionError("Expected rows in table");
                        }
                    });

            assertEventually(
                    Duration.ofSeconds(20),
                    () -> {
                        CacheStats afterQueryCacheStats = getCacheStats(env);
                        return afterQueryCacheStats.cacheSpaceUsed() >= beforeCacheStats.cacheSpaceUsed() &&
                                afterQueryCacheStats.externalReads() > beforeCacheStats.externalReads() &&
                                afterQueryCacheStats.cacheReads() >= beforeCacheStats.cacheReads();
                    });

            // Second query - should read from cache
            assertEventually(
                    Duration.ofSeconds(10),
                    () -> {
                        CacheStats beforeSecondQuery = getCacheStats(env);
                        env.executeTrino("SELECT * FROM " + tableName);
                        CacheStats afterSecondQuery = getCacheStats(env);

                        // Subsequent queries should read from cache
                        // Cache reads should increase while external reads stay the same
                        return afterSecondQuery.cacheReads() > beforeSecondQuery.cacheReads() &&
                                afterSecondQuery.externalReads() == beforeSecondQuery.externalReads() &&
                                afterSecondQuery.cacheSpaceUsed() == beforeSecondQuery.cacheSpaceUsed();
                    });
        }
        finally {
            // Cleanup
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + tableName);
            env.executeTrinoUpdate("DROP SCHEMA IF EXISTS iceberg." + schemaName);
        }
    }

    /**
     * Creates a table with enough data to exercise caching (~6 files of ~2MB each).
     */
    private void createTestTable(MultiNodeIcebergMinioCachingEnvironment env, String bucketName, String schemaName, String tableName)
    {
        env.executeTrinoUpdate("DROP TABLE IF EXISTS " + tableName);
        env.executeTrinoUpdate("DROP SCHEMA IF EXISTS iceberg." + schemaName);
        env.executeTrinoUpdate("CREATE SCHEMA iceberg." + schemaName + " WITH (location = 's3://" + bucketName + "/test_iceberg_caching')");
        env.executeTrinoUpdate("CREATE TABLE " + tableName + " WITH (target_max_file_size = '2MB') AS SELECT * FROM tpch.sf1.customer");
    }

    private CacheStats getCacheStats(MultiNodeIcebergMinioCachingEnvironment env)
    {
        var result = env.executeTrino(
                "SELECT " +
                        "  sum(\"cachereads.alltime.count\") as cacheReads, " +
                        "  sum(\"externalreads.alltime.count\") as externalReads " +
                        "FROM jmx.current.\"io.trino.filesystem.alluxio:catalog=iceberg,name=iceberg,type=alluxiocachestats\"");
        var row = result.getRows().getFirst();
        return new CacheStats(
                ((Number) row.getValues().get(0)).doubleValue(),
                ((Number) row.getValues().get(1)).doubleValue(),
                ((Number) env.executeTrino(
                "SELECT sum(count) FROM jmx.current.\"org.alluxio:name=client.cachespaceusedcount,type=counters\"")
                        .getOnlyValue()).longValue());
    }

    /**
     * Retries an assertion until it passes or times out.
     */
    private void assertEventually(Duration timeout, BooleanSupplier assertion)
    {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (assertion.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(100);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting", e);
            }
        }
        throw new AssertionError("Assertion did not pass within " + timeout);
    }

    private record CacheStats(double cacheReads, double externalReads, long cacheSpaceUsed) {}
}
