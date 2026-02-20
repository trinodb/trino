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
package io.trino.tests.product.hive;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for Hive file system caching using Alluxio.
 * <p>
 * Ported from the Tempto-based TestHiveAlluxioCaching.
 * <p>
 * This test verifies that Trino's filesystem caching works correctly with the Hive connector:
 * <ul>
 *   <li>First query fetches data from external storage (HDFS)</li>
 *   <li>Subsequent queries read from the cache</li>
 * </ul>
 */
@ProductTest
@RequiresEnvironment(MultinodeHiveCachingEnvironment.class)
@TestGroup.HiveAlluxioCaching
class TestHiveAlluxioCaching
{
    @Test
    void testReadFromCache(MultinodeHiveCachingEnvironment env)
    {
        testReadFromTable(env, "table1");
        testReadFromTable(env, "table2");
    }

    private void testReadFromTable(MultinodeHiveCachingEnvironment env, String tableNameSuffix)
    {
        String cachedTableName = "hive.default.test_cache_read" + tableNameSuffix;
        String nonCachedTableName = "hivenoncached.default.test_cache_read" + tableNameSuffix;

        try {
            createTestTable(env, nonCachedTableName);

            CacheStats beforeCacheStats = getCacheStats(env);
            long tableSize = getTableSize(env, nonCachedTableName);

            // First query via caching catalog
            assertThat(env.executeTrino("SELECT * FROM " + cachedTableName))
                    .hasRowsCount(150000);

            // First query via caching catalog should fetch remote data
            assertEventually(
                    Duration.ofSeconds(20),
                    () -> {
                        CacheStats afterQueryCacheStats = getCacheStats(env);
                        assertThat(afterQueryCacheStats.cacheSpaceUsed())
                                .isGreaterThanOrEqualTo(beforeCacheStats.cacheSpaceUsed() + tableSize);
                        assertThat(afterQueryCacheStats.externalReads())
                                .isGreaterThan(beforeCacheStats.externalReads());
                        assertThat(afterQueryCacheStats.cacheReads())
                                .isGreaterThanOrEqualTo(beforeCacheStats.cacheReads());
                    });

            // Second query - should read exclusively from cache
            assertEventually(
                    Duration.ofSeconds(10),
                    () -> {
                        CacheStats beforeQueryCacheStats = getCacheStats(env);

                        // Execute query
                        assertThat(env.executeTrino("SELECT * FROM " + cachedTableName)).hasAnyRows();

                        // Query via caching catalog should read exclusively from cache
                        CacheStats afterQueryCacheStats = getCacheStats(env);
                        assertThat(afterQueryCacheStats.cacheReads())
                                .isGreaterThan(beforeQueryCacheStats.cacheReads());
                        assertThat(afterQueryCacheStats.externalReads())
                                .isEqualTo(beforeQueryCacheStats.externalReads());
                        assertThat(afterQueryCacheStats.cacheSpaceUsed())
                                .isEqualTo(beforeQueryCacheStats.cacheSpaceUsed());
                    });
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + nonCachedTableName);
        }
    }

    /**
     * Creates a table which should contain multiple small ORC files.
     */
    private void createTestTable(MultinodeHiveCachingEnvironment env, String tableName)
    {
        env.executeTrinoUpdate("DROP TABLE IF EXISTS " + tableName);
        env.executeTrinoInSession(session -> {
            session.executeUpdate("SET SESSION hive.target_max_file_size = '4MB'");
            session.executeUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.sf1.customer");
        });
    }

    /**
     * Gets the total size of the table's data files.
     */
    private long getTableSize(MultinodeHiveCachingEnvironment env, String tableName)
    {
        var result = env.executeTrino(
                "SELECT SUM(size) as size FROM (SELECT \"$path\", \"$file_size\" AS size FROM " + tableName + " GROUP BY 1, 2)");
        return ((Number) result.getOnlyValue()).longValue();
    }

    /**
     * Gets cache statistics from JMX.
     */
    private CacheStats getCacheStats(MultinodeHiveCachingEnvironment env)
    {
        // Try Alluxio JMX path first (used by MultinodeHiveCachingEnvironment)
        try {
            var result = env.executeTrino(
                    "SELECT " +
                            "  sum(\"cachereads.alltime.count\") as cacheReads, " +
                            "  sum(\"externalreads.alltime.count\") as externalReads " +
                            "FROM jmx.current.\"io.trino.filesystem.alluxio:catalog=hive,name=hive,type=alluxiocachestats\"");

            if (result.getRowsCount() > 0) {
                var row = result.getRows().get(0);
                double cacheReads = row.getValues().get(0) != null ? ((Number) row.getValues().get(0)).doubleValue() : 0;
                double externalReads = row.getValues().get(1) != null ? ((Number) row.getValues().get(1)).doubleValue() : 0;

                // Get cache space used from separate query
                long cacheSpaceUsed = getCacheSpaceUsed(env);
                return new CacheStats(cacheReads, externalReads, cacheSpaceUsed);
            }
        }
        catch (Exception e) {
            // JMX table might not exist
        }

        // Fallback: try native fs cache stats
        try {
            var result = env.executeTrino(
                    "SELECT " +
                            "  COALESCE(sum(\"CacheHits.TotalCount\"), 0) as cacheReads, " +
                            "  COALESCE(sum(\"CacheMisses.TotalCount\"), 0) as externalReads " +
                            "FROM jmx.current.\"io.trino.filesystem.cache:name=hive,type=cachestats\"");

            if (result.getRowsCount() > 0) {
                var row = result.getRows().get(0);
                double cacheReads = ((Number) row.getValues().get(0)).doubleValue();
                double externalReads = ((Number) row.getValues().get(1)).doubleValue();
                return new CacheStats(cacheReads, externalReads, 0);
            }
        }
        catch (Exception e) {
            // JMX table might not exist
        }

        return new CacheStats(0, 0, 0);
    }

    /**
     * Gets the cache space used from JMX.
     */
    private long getCacheSpaceUsed(MultinodeHiveCachingEnvironment env)
    {
        try {
            var result = env.executeTrino(
                    "SELECT sum(count) FROM jmx.current.\"org.alluxio:name=client.cachespaceusedcount,type=counters\"");
            if (result.getRowsCount() > 0 && result.getOnlyValue() != null) {
                return ((Number) result.getOnlyValue()).longValue();
            }
        }
        catch (Exception e) {
            // JMX table might not exist
        }
        return 0;
    }

    /**
     * Retries an assertion until it passes or times out.
     */
    private static void assertEventually(Duration timeout, Runnable assertion)
    {
        long start = System.nanoTime();
        while (!Thread.currentThread().isInterrupted()) {
            try {
                assertion.run();
                return;
            }
            catch (Exception | AssertionError e) {
                if (Duration.ofNanos(System.nanoTime() - start).compareTo(timeout) > 0) {
                    throw e;
                }
            }
            try {
                Thread.sleep(50);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting", e);
            }
        }
    }

    private record CacheStats(double cacheReads, double externalReads, long cacheSpaceUsed) {}
}
