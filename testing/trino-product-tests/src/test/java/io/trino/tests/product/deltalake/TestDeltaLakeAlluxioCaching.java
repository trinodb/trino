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
        String schemaName = "test_caching_" + schemaSuffix;
        String cachedSchema = "delta." + schemaName;
        String cachedLocation = "s3://" + env.getBucketName() + "/delta_cached_" + schemaSuffix;
        String tableName = "test_cache_read_" + tableNameSuffix;
        String cachedTable = cachedSchema + "." + tableName;
        String nonCachedTable = "delta_non_cached." + schemaName + "." + tableName;

        try {
            createTestTable(env, cachedSchema, cachedLocation, cachedTable);

            CacheStats beforeCacheStats = getCacheStats(env);
            long tableSize = getTableSize(env, nonCachedTable);

            assertThat(env.executeTrino("SELECT * FROM " + cachedTable))
                    .satisfies(result -> {
                        if (result.getRowsCount() == 0) {
                            throw new AssertionError("Expected rows in cached table");
                        }
                    });

            assertEventually(
                    Duration.ofSeconds(20),
                    () -> {
                        CacheStats afterQueryCacheStats = getCacheStats(env);
                        return afterQueryCacheStats.cacheSpaceUsed() >= beforeCacheStats.cacheSpaceUsed() + tableSize &&
                                afterQueryCacheStats.externalReads() > beforeCacheStats.externalReads() &&
                                afterQueryCacheStats.cacheReads() >= beforeCacheStats.cacheReads();
                    });

            assertEventually(
                    Duration.ofSeconds(10),
                    () -> {
                        CacheStats beforeSecondQuery = getCacheStats(env);
                        env.executeTrino("SELECT * FROM " + cachedTable);
                        CacheStats afterSecondQuery = getCacheStats(env);
                        return afterSecondQuery.cacheReads() > beforeSecondQuery.cacheReads() &&
                                afterSecondQuery.externalReads() == beforeSecondQuery.externalReads() &&
                                afterSecondQuery.cacheSpaceUsed() == beforeSecondQuery.cacheSpaceUsed();
                    });
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + cachedTable);
            env.executeTrinoUpdate("DROP SCHEMA IF EXISTS " + cachedSchema);
        }
    }

    private void createTestTable(
            DeltaLakeMinioCachingEnvironment env,
            String cachedSchema,
            String cachedLocation,
            String cachedTable)
    {
        env.executeTrinoUpdate("DROP TABLE IF EXISTS " + cachedTable);
        env.executeTrinoUpdate("DROP SCHEMA IF EXISTS " + cachedSchema);

        env.executeTrinoUpdate("CREATE SCHEMA " + cachedSchema + " WITH (location = '" + cachedLocation + "')");
        env.executeTrinoInSession(session -> {
            session.executeUpdate("SET SESSION delta.target_max_file_size = '2MB'");
            session.executeUpdate("CREATE TABLE " + cachedTable + " AS SELECT * FROM tpch.sf1.customer");
        });
    }

    private long getTableSize(DeltaLakeMinioCachingEnvironment env, String tableName)
    {
        return ((Number) env.executeTrino(
        "SELECT SUM(size) FROM (SELECT \"$path\", \"$file_size\" AS size FROM " + tableName + " GROUP BY 1, 2)")
                .getOnlyValue()).longValue();
    }

    private CacheStats getCacheStats(DeltaLakeMinioCachingEnvironment env)
    {
        var result = env.executeTrino(
                "SELECT " +
                        "  sum(\"cachereads.alltime.count\") as cacheReads, " +
                        "  sum(\"externalreads.alltime.count\") as externalReads " +
                        "FROM jmx.current.\"io.trino.filesystem.alluxio:catalog=delta,name=delta,type=alluxiocachestats\"");
        var row = result.getRows().getFirst();
        return new CacheStats(
                ((Number) row.getValues().get(0)).doubleValue(),
                ((Number) row.getValues().get(1)).doubleValue(),
                ((Number) env.executeTrino(
                "SELECT sum(count) FROM jmx.current.\"org.alluxio:name=client.cachespaceusedcount,type=counters\"")
                        .getOnlyValue()).longValue());
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

    private record CacheStats(double cacheReads, double externalReads, long cacheSpaceUsed) {}
}
