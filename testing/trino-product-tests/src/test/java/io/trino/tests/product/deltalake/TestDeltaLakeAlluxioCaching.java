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
package io.trino.tests.product.deltalake;

import io.trino.testing.TestingNames;
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.function.BooleanSupplier;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;

@ProductTest
@RequiresEnvironment(DeltaLakeMinioCachingEnvironment.class)
@TestGroup.ConfiguredFeatures
@TestGroup.DeltaLakeAlluxioCaching
@TestGroup.ProfileSpecificTests
class TestDeltaLakeAlluxioCaching
{
    @Test
    void testReadFromCache(DeltaLakeMinioCachingEnvironment env)
    {
        testReadFromTable(env, "table1");
        testReadFromTable(env, "table2");
    }

    private void testReadFromTable(DeltaLakeMinioCachingEnvironment env, String tableNameSuffix)
    {
        String schemaSuffix = TestingNames.randomNameSuffix();
        String cachedSchema = "delta.test_caching_cached_" + schemaSuffix;
        String nonCachedSchema = "delta_non_cached.test_caching_non_cached_" + schemaSuffix;
        String cachedLocation = "s3://" + env.getBucketName() + "/delta_cached_" + schemaSuffix;
        String nonCachedLocation = "s3://" + env.getBucketName() + "/delta_non_cached_" + schemaSuffix;
        String tableName = "test_cache_read_" + tableNameSuffix;
        String cachedTable = cachedSchema + "." + tableName;
        String nonCachedTable = nonCachedSchema + "." + tableName;

        try {
            createTestTable(env, cachedSchema, nonCachedSchema, cachedLocation, nonCachedLocation, cachedTable, nonCachedTable);

            CacheStats beforeCacheStats = getCacheStats(env);

            assertThat(env.executeTrino("SELECT * FROM " + cachedTable))
                    .satisfies(result -> {
                        if (result.getRowsCount() == 0) {
                            throw new AssertionError("Expected rows in cached table");
                        }
                    });

            assertEventually(
                    Duration.ofSeconds(20),
                    () -> getCacheStats(env).externalReads() > beforeCacheStats.externalReads());

            assertEventually(
                    Duration.ofSeconds(10),
                    () -> {
                        CacheStats beforeSecondQuery = getCacheStats(env);
                        env.executeTrino("SELECT * FROM " + cachedTable);
                        CacheStats afterSecondQuery = getCacheStats(env);
                        return afterSecondQuery.cacheReads() > beforeSecondQuery.cacheReads() &&
                                afterSecondQuery.externalReads() == beforeSecondQuery.externalReads();
                    });
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + cachedTable);
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + nonCachedTable);
            env.executeTrinoUpdate("DROP SCHEMA IF EXISTS " + cachedSchema);
            env.executeTrinoUpdate("DROP SCHEMA IF EXISTS " + nonCachedSchema);
        }
    }

    private void createTestTable(
            DeltaLakeMinioCachingEnvironment env,
            String cachedSchema,
            String nonCachedSchema,
            String cachedLocation,
            String nonCachedLocation,
            String cachedTable,
            String nonCachedTable)
    {
        env.executeTrinoUpdate("DROP TABLE IF EXISTS " + cachedTable);
        env.executeTrinoUpdate("DROP TABLE IF EXISTS " + nonCachedTable);
        env.executeTrinoUpdate("DROP SCHEMA IF EXISTS " + cachedSchema);
        env.executeTrinoUpdate("DROP SCHEMA IF EXISTS " + nonCachedSchema);

        env.executeTrinoUpdate("CREATE SCHEMA " + cachedSchema + " WITH (location = '" + cachedLocation + "')");
        env.executeTrinoUpdate("CREATE SCHEMA " + nonCachedSchema + " WITH (location = '" + nonCachedLocation + "')");
        env.executeTrinoUpdate("SET SESSION delta.target_max_file_size = '2MB'");
        env.executeTrinoUpdate("CREATE TABLE " + nonCachedTable + " AS SELECT * FROM tpch.sf1.customer");
        env.executeTrinoUpdate("CREATE TABLE " + cachedTable + " AS SELECT * FROM " + nonCachedTable);
    }

    private CacheStats getCacheStats(DeltaLakeMinioCachingEnvironment env)
    {
        try {
            var result = env.executeTrino(
                    "SELECT " +
                            "  COALESCE(sum(\"CacheHits.TotalCount\"), 0) as cacheReads, " +
                            "  COALESCE(sum(\"CacheMisses.TotalCount\"), 0) as externalReads " +
                            "FROM jmx.current.\"io.trino.filesystem.cache:name=delta,type=cachestats\"");

            if (result.getRowsCount() > 0) {
                var row = result.getRows().getFirst();
                double cacheReads = ((Number) row.getValues().get(0)).doubleValue();
                double externalReads = ((Number) row.getValues().get(1)).doubleValue();
                return new CacheStats(cacheReads, externalReads);
            }
        }
        catch (Exception ignored) {
        }

        try {
            var result = env.executeTrino(
                    "SELECT " +
                            "  sum(\"cachereads.alltime.count\") as cacheReads, " +
                            "  sum(\"externalreads.alltime.count\") as externalReads " +
                            "FROM jmx.current.\"io.trino.filesystem.alluxio:catalog=delta,name=delta,type=alluxiocachestats\"");

            if (result.getRowsCount() > 0) {
                var row = result.getRows().getFirst();
                double cacheReads = row.getValues().get(0) != null ? ((Number) row.getValues().get(0)).doubleValue() : 0;
                double externalReads = row.getValues().get(1) != null ? ((Number) row.getValues().get(1)).doubleValue() : 0;
                return new CacheStats(cacheReads, externalReads);
            }
        }
        catch (Exception ignored) {
        }

        return new CacheStats(0, 0);
    }

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
                throw new RuntimeException("Interrupted while waiting for cache assertion", e);
            }
        }
        throw new AssertionError("Assertion did not pass within " + timeout);
    }

    private record CacheStats(double cacheReads, double externalReads) {}
}
