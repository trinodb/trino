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

import io.airlift.units.Duration;
import io.trino.tempto.ProductTest;
import io.trino.tests.product.utils.CachingTestUtils.CacheStats;
import org.testng.annotations.Test;

import static io.airlift.testing.Assertions.assertGreaterThan;
import static io.airlift.testing.Assertions.assertGreaterThanOrEqual;
import static io.trino.tests.product.TestGroups.ICEBERG_ALLUXIO_CACHING;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.CachingTestUtils.getCacheStats;
import static io.trino.tests.product.utils.QueryAssertions.assertEventually;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class TestIcebergAlluxioCaching
        extends ProductTest
{
    @Test(groups = {ICEBERG_ALLUXIO_CACHING, PROFILE_SPECIFIC_TESTS})
    public void testReadFromCache()
    {
        testReadFromTable("table1");
        testReadFromTable("table2");
    }

    private void testReadFromTable(String tableNameSuffix)
    {
        String cachedTableName = "iceberg.default.test_cache_read" + tableNameSuffix;
        String nonCachedTableName = "iceberg.default.test_cache_read" + tableNameSuffix;

        createTestTable(cachedTableName);

        CacheStats beforeCacheStats = getCacheStats("iceberg");

        assertThat(onTrino().executeQuery("SELECT * FROM " + cachedTableName)).hasAnyRows();

        assertEventually(
                new Duration(20, SECONDS),
                () -> {
                    // first query via caching catalog should fetch external data
                    CacheStats afterQueryCacheStats = getCacheStats("iceberg");
                    assertGreaterThanOrEqual(afterQueryCacheStats.cacheSpaceUsed(), beforeCacheStats.cacheSpaceUsed());
                    assertGreaterThan(afterQueryCacheStats.externalReads(), beforeCacheStats.externalReads());
                    assertGreaterThanOrEqual(afterQueryCacheStats.cacheReads(), beforeCacheStats.cacheReads());
                });

        assertEventually(
                new Duration(10, SECONDS),
                () -> {
                    CacheStats beforeQueryCacheStats = getCacheStats("iceberg");

                    assertThat(onTrino().executeQuery("SELECT * FROM " + cachedTableName)).hasAnyRows();

                    // query via caching catalog should read exclusively from cache
                    CacheStats afterQueryCacheStats = getCacheStats("iceberg");
                    assertGreaterThan(afterQueryCacheStats.cacheReads(), beforeQueryCacheStats.cacheReads());
                    assertEquals(afterQueryCacheStats.externalReads(), beforeQueryCacheStats.externalReads());
                    assertEquals(afterQueryCacheStats.cacheSpaceUsed(), beforeQueryCacheStats.cacheSpaceUsed());
                });

        onTrino().executeQuery("DROP TABLE " + nonCachedTableName);
    }

    /**
     * Creates a table which should contain around 6 2 MB parquet files
     */
    private void createTestTable(String tableName)
    {
        onTrino().executeQuery("DROP TABLE IF EXISTS " + tableName);
        onTrino().executeQuery("SET SESSION iceberg.target_max_file_size = '2MB'");
        onTrino().executeQuery("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.sf1.customer");
    }
}
